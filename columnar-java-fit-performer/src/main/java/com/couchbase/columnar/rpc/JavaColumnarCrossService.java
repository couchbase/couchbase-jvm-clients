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

import com.couchbase.columnar.client.java.Queryable;
import com.couchbase.columnar.modes.Mode;
import com.couchbase.columnar.query.ExecuteQueryStreamer;
import com.couchbase.columnar.query.QueryResultBufferedStreamer;
import com.couchbase.columnar.query.QueryResultPushBasedStreamer;
import com.couchbase.columnar.util.ErrorUtil;
import com.couchbase.columnar.util.ResultUtil;
import fit.columnar.CloseAllQueryResultsRequest;
import fit.columnar.CloseQueryResultRequest;
import fit.columnar.ColumnarCrossServiceGrpc;
import fit.columnar.EmptyResultOrFailureResponse;
import fit.columnar.ExecuteQueryRequest;
import fit.columnar.ExecuteQueryResponse;
import fit.columnar.QueryCancelRequest;
import fit.columnar.QueryMetadataRequest;
import fit.columnar.QueryResultMetadataResponse;
import fit.columnar.QueryResultRequest;
import fit.columnar.QueryRowRequest;
import fit.columnar.QueryRowResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.columnar.fit.core.util.StartTimes.startTiming;

public class JavaColumnarCrossService extends ColumnarCrossServiceGrpc.ColumnarCrossServiceImplBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(JavaColumnarCrossService.class);
  private final IntraServiceContext context;
  private final Map<String, ExecuteQueryStreamer> queryResultStreamers = new HashMap<>();

  public JavaColumnarCrossService(IntraServiceContext context) {
    this.context = context;
  }

  @Override
  public void executeQuery(ExecuteQueryRequest request, StreamObserver<ExecuteQueryResponse> responseObserver) {
    var startTime = startTiming();
    try {
      Queryable clusterOrScope;
      int modeIdx;

      if (request.hasClusterLevel()) {
        clusterOrScope = context.cluster(request.getClusterLevel()).cluster();
        modeIdx = request.getClusterLevel().getShared().getModeIndex();
      } else if (request.hasScopeLevel()) {
        clusterOrScope = context.scope(request.getScopeLevel());
        modeIdx = request.getScopeLevel().getShared().getModeIndex();
      } else {
        throw new UnsupportedOperationException();
      }

      ExecuteQueryStreamer streamer;
      if (modeIdx == Mode.PUSH_BASED_STREAMING.ordinal()) {
        streamer = new QueryResultPushBasedStreamer(clusterOrScope, request);
      } else {
        streamer = new QueryResultBufferedStreamer(clusterOrScope, request);
      }

      queryResultStreamers.put(streamer.queryHandle(), streamer);
      responseObserver.onNext(fit.columnar.ExecuteQueryResponse.newBuilder()
        .setMetadata(ResultUtil.responseMetadata(startTime))
        .setQueryHandle(streamer.queryHandle())
        .build());

    } catch (Throwable err) {
      LOGGER.warn("Failure during executeQuery: {}", err.toString());
    }
    responseObserver.onCompleted();
  }

  @Override
  public void queryResult(QueryResultRequest request, StreamObserver<EmptyResultOrFailureResponse> responseObserver) {
    var startTime = startTiming();
    try {
      var streamer = queryResultStreamers.get(request.getQueryHandle());
      responseObserver.onNext(streamer.blockForQueryResult());
    } catch (Throwable err) {
      responseObserver.onNext(ResultUtil.failure(err, startTime));
    }
    responseObserver.onCompleted();
  }

  @Override
  public void queryRow(QueryRowRequest request, StreamObserver<QueryRowResponse> responseObserver) {
    var startTime = startTiming();
    try {
      var streamer = queryResultStreamers.get(request.getQueryHandle());
      responseObserver.onNext(streamer.blockForRow());
    } catch (Throwable err) {
      responseObserver.onNext(fit.columnar.QueryRowResponse.newBuilder()
        .setMetadata(ResultUtil.responseMetadata(startTime))
        .setRowLevelFailure(ErrorUtil.convertError(err))
        .build());
    }
    responseObserver.onCompleted();
  }

  @Override
  public void queryCancel(QueryCancelRequest request, StreamObserver<EmptyResultOrFailureResponse> responseObserver) {
    var startTime = startTiming();
    try {
      var streamer = queryResultStreamers.get(request.getQueryHandle());
      streamer.cancel();
      responseObserver.onNext(ResultUtil.success(startTime));
    } catch (Throwable err) {
      responseObserver.onNext(ResultUtil.failure(err, startTime));
    }
    responseObserver.onCompleted();

  }

  @Override
  public void queryMetadata(QueryMetadataRequest request, StreamObserver<QueryResultMetadataResponse> responseObserver) {
    var startTime = startTiming();
    try {
      var streamer = queryResultStreamers.get(request.getQueryHandle());
      responseObserver.onNext(streamer.blockForMetadata());
    } catch (Throwable err) {
      responseObserver.onNext(fit.columnar.QueryResultMetadataResponse.newBuilder()
        .setMetadata(ResultUtil.responseMetadata(startTime))
        .setFailure(ErrorUtil.convertError(err))
        .build());
    }
    responseObserver.onCompleted();
  }

  @Override
  public void closeQueryResult(CloseQueryResultRequest request, StreamObserver<EmptyResultOrFailureResponse> responseObserver) {
    super.closeQueryResult(request, responseObserver);
  }

  @Override
  public void closeAllQueryResults(CloseAllQueryResultsRequest request, StreamObserver<EmptyResultOrFailureResponse> responseObserver) {
    super.closeAllQueryResults(request, responseObserver);
  }
}
