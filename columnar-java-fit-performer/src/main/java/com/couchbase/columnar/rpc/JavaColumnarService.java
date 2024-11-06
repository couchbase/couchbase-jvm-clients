/*
 * Copyright (c) 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.rpc;

import com.couchbase.client.protocol.shared.EchoRequest;
import com.couchbase.client.protocol.shared.EchoResponse;
import com.couchbase.columnar.cluster.ColumnarClusterConnection;
import com.couchbase.columnar.fit.core.exceptions.ExceptionGrpcMappingUtil;
import com.couchbase.columnar.fit.core.util.VersionUtil;
import com.couchbase.columnar.modes.Mode;
import com.couchbase.columnar.util.ResultUtil;
import fit.columnar.CloseAllColumnarClustersRequest;
import fit.columnar.ClusterCloseRequest;
import fit.columnar.ClusterNewInstanceRequest;
import fit.columnar.ColumnarServiceGrpc;
import fit.columnar.EmptyResultOrFailureResponse;
import fit.columnar.SdkConnectionError;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.couchbase.columnar.fit.core.util.StartTimes.startTiming;

public class JavaColumnarService extends ColumnarServiceGrpc.ColumnarServiceImplBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(JavaColumnarService.class);
  private final IntraServiceContext context;

  public JavaColumnarService(IntraServiceContext context) {
    this.context = context;
  }

  @Override
  public void fetchPerformerCaps(fit.columnar.FetchPerformerCapsRequest request, StreamObserver<fit.columnar.FetchPerformerCapsResponse> responseObserver) {
    try {
      var builder = fit.columnar.FetchPerformerCapsResponse.newBuilder();
      var sdkVersion = VersionUtil.introspectSDKVersionJava();
      if (sdkVersion != null) {
        builder.setSdkVersion(sdkVersion);
      }
      builder.setSdk(fit.columnar.SDK.SDK_JAVA);
      builder.putClusterNewInstance(0, fit.columnar.PerApiElementClusterNewInstance.getDefaultInstance());
      builder.putClusterClose(0, fit.columnar.PerApiElementClusterClose.getDefaultInstance());
      // The SDK has two main modes: buffered and push-based streaming.
      var executeQueryBuffered = fit.columnar.PerApiElementExecuteQuery.newBuilder()
        .setExecuteQueryReturns(fit.columnar.PerApiElementExecuteQuery.ExecuteQueryReturns.EXECUTE_QUERY_RETURNS_QUERY_RESULT)
        .setRowIteration(fit.columnar.PerApiElementExecuteQuery.RowIteration.ROW_ITERATION_BUFFERED)
        .setRowDeserialization(fit.columnar.PerApiElementExecuteQuery.RowDeserialization.ROW_DESERIALIZATION_STATIC_ROW_TYPING_INDIVIDUAL)
        .setSupportsCustomDeserializer(true)
        .build();
      var executeQueryPushBased = fit.columnar.PerApiElementExecuteQuery.newBuilder()
        .setExecuteQueryReturns(fit.columnar.PerApiElementExecuteQuery.ExecuteQueryReturns.EXECUTE_QUERY_RETURNS_QUERY_METADATA)
        .setRowIteration(fit.columnar.PerApiElementExecuteQuery.RowIteration.ROW_ITERATION_STREAMING_PUSH_BASED)
        .setRowDeserialization(fit.columnar.PerApiElementExecuteQuery.RowDeserialization.ROW_DESERIALIZATION_STATIC_ROW_TYPING_INDIVIDUAL)
        .setSupportsCustomDeserializer(true)
        .build();
      builder.putClusterExecuteQuery(Mode.PUSH_BASED_STREAMING.ordinal(), executeQueryPushBased);
      builder.putClusterExecuteQuery(Mode.BUFFERED.ordinal(), executeQueryBuffered);
      builder.putScopeExecuteQuery(Mode.PUSH_BASED_STREAMING.ordinal(), executeQueryPushBased);
      builder.putScopeExecuteQuery(Mode.BUFFERED.ordinal(), executeQueryBuffered);
      for (Mode mode : new Mode[]{Mode.PUSH_BASED_STREAMING, Mode.BUFFERED}) {
        builder.putSdkConnectionError(mode.ordinal(), SdkConnectionError.newBuilder()
          .setInvalidCredErrorType(SdkConnectionError.InvalidCredentialErrorType.AS_INVALID_CREDENTIAL_EXCEPTION)
          .setBootstrapErrorType(SdkConnectionError.BootstrapErrorType.ERROR_AS_TIMEOUT_EXCEPTION)
          .build()
        );
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    } catch (RuntimeException err) {
      LOGGER.warn("Failure during fetchPerformerCaps: {}", err.toString());
      responseObserver.onError(ExceptionGrpcMappingUtil.map(err));
    }
  }

  @Override
  public void echo(EchoRequest request, StreamObserver<EchoResponse> responseObserver) {
    LOGGER.info("================ {} : {} ================ ", request.getTestName(), request.getMessage());
    responseObserver.onNext(EchoResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void clusterNewInstance(ClusterNewInstanceRequest request, StreamObserver<EmptyResultOrFailureResponse> responseObserver) {
    var startTime = startTiming();
    try {
      var onClusterConnectionClose = new ArrayList<Runnable>();

      request.getTunablesMap().forEach((k, v) -> {
        LOGGER.info("Setting cluster-level tunable {}={}", k, v);
        if (v != null) {
          System.setProperty(k, v);
        }
      });

      // This isn't at all safe when multiple cluster connections are used.  FIT is aware of this and will in
      // practice only use this mechanism when we care about a single cluster connection, such as FIT/PERF.
      onClusterConnectionClose.add(() -> {
        request.getTunablesMap().forEach((k, v) -> {
          LOGGER.info("Clearing cluster-level tunable {}", k);
          if (v != null) {
            System.clearProperty(k);
          }
        });
      });

      LOGGER.info("Created new cluster connection {}", request);

      var cc = new ColumnarClusterConnection(request, onClusterConnectionClose);
      context.addCluster(request.getClusterConnectionId(), cc);

      var clusters = context.clusters();
      LOGGER.info("Dumping {} cluster connections for resource leak troubleshooting:", clusters.size());
      clusters.forEach((key, value) -> LOGGER.info("Cluster connection {} {}", key, value.request().getCredential().getUsernameAndPassword().getUsername()));

      responseObserver.onNext(ResultUtil.success(startTime));
    } catch (RuntimeException err) {
      responseObserver.onNext(ResultUtil.failure(err, startTime));
    }
    responseObserver.onCompleted();
  }

  @Override
  public void clusterClose(ClusterCloseRequest request, StreamObserver<EmptyResultOrFailureResponse> responseObserver) {
    var startTime = startTiming();
    try {
      context.closeAndRemoveCluster(request.getExecutionContext().getClusterId());
      responseObserver.onNext(ResultUtil.success(startTime));
    } catch (RuntimeException err) {
      responseObserver.onNext(ResultUtil.failure(err, startTime));
    }
    responseObserver.onCompleted();
  }

  @Override
  public void closeAllClusters(CloseAllColumnarClustersRequest request, StreamObserver<EmptyResultOrFailureResponse> responseObserver) {
    var startTime = startTiming();
    try {
      context.closeAndRemoveAllClusters();
      responseObserver.onNext(ResultUtil.success(startTime));
    } catch (RuntimeException err) {
      responseObserver.onNext(ResultUtil.failure(err, startTime));
    }
    responseObserver.onCompleted();
  }
}
