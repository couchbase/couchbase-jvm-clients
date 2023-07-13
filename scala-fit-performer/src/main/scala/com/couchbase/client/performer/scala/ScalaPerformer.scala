/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.performer.scala

import com.couchbase.client.performer.core.CorePerformer
import com.couchbase.client.performer.core.commands.{SdkCommandExecutor, TransactionCommandExecutor}
import com.couchbase.client.performer.core.perf.Counters
import com.couchbase.client.protocol.shared._
import com.couchbase.client.performer.scala.util.{Capabilities, ClusterConnection}
import com.couchbase.client.protocol.performer.{Caps, PerformerCapsFetchResponse}
import com.couchbase.client.protocol.run.Workloads
import com.couchbase.client.protocol.shared.{
  ClusterConnectionCreateRequest,
  ClusterConnectionCreateResponse
}
import io.grpc.stub.StreamObserver
import io.grpc.{ServerBuilder, Status}
import org.slf4j.LoggerFactory

import java.lang

object ScalaPerformer {
  private val logger = LoggerFactory.getLogger(classOf[ScalaPerformer])

  def main(args: Array[String]): Unit = {
    var port = 8060
    // ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    for (parameter <- args) {
      parameter.split("=")(0) match {
        case "port" =>
          port = parameter.split("=")(1).toInt

        case _ =>
          logger.warn("Undefined input: {}. Ignoring it", parameter)
      }
    }
    val builder = ServerBuilder.forPort(port)
    builder.addService(new ScalaPerformer)
    val server = builder.build()
    server.start
    logger.info("Server Started at {}", server.getPort)
    server.awaitTermination()
  }
}

class ScalaPerformer extends CorePerformer {
  private val logger             = LoggerFactory.getLogger(classOf[ScalaPerformer])
  private val clusterConnections = collection.mutable.Map.empty[String, ClusterConnection]

  override protected def customisePerformerCaps(
      response: PerformerCapsFetchResponse.Builder
  ): Unit = {
    response
      .setPerformerUserAgent("scala")
      .addPerformerCaps(Caps.CLUSTER_CONFIG_CERT)
      .addAllSdkImplementationCaps(Capabilities.sdkImplementationCaps)
  }

  override def clusterConnectionCreate(
      request: ClusterConnectionCreateRequest,
      responseObserver: StreamObserver[ClusterConnectionCreateResponse]
  ): Unit = {
    try {
      val connection = new ClusterConnection(request)
      clusterConnections.put(request.getClusterConnectionId, connection)
      ClusterConnectionCreateRequest.getDefaultInstance.newBuilderForType
      logger.info(
        "Established connection to cluster at IP: {} with user {} and id {}",
        request.getClusterHostname,
        request.getClusterUsername,
        request.getClusterConnectionId
      )
      responseObserver.onNext(
        ClusterConnectionCreateResponse.newBuilder
          .setClusterConnectionCount(clusterConnections.size)
          .build
      )
      responseObserver.onCompleted()
    } catch {
      case err: Throwable =>
        logger.error("Operation failed during clusterConnectionCreate due to {}", err.getMessage)
        responseObserver.onError(Status.ABORTED.withDescription(err.toString).asException)
    }
  }

  override def clusterConnectionClose(
      request: ClusterConnectionCloseRequest,
      responseObserver: StreamObserver[ClusterConnectionCloseResponse]
  ): Unit = {
    try {
      clusterConnections(request.getClusterConnectionId).close()
      clusterConnections.remove(request.getClusterConnectionId)
      ClusterConnectionCreateRequest.getDefaultInstance.newBuilderForType
      responseObserver.onNext(
        ClusterConnectionCloseResponse.newBuilder
          .setClusterConnectionCount(clusterConnections.size)
          .build
      )
      responseObserver.onCompleted()
    } catch {
      case err: Throwable =>
        logger.error("Operation failed during clusterConnectionCreate due to {}", err.getMessage)
        responseObserver.onError(Status.ABORTED.withDescription(err.toString).asException)
    }
  }

  override def disconnectConnections(
      request: DisconnectConnectionsRequest,
      responseObserver: StreamObserver[DisconnectConnectionsResponse]
  ): Unit = {
    clusterConnections.foreach(cc => cc._2.cluster.disconnect())
    clusterConnections.clear()
    responseObserver.onNext(DisconnectConnectionsResponse.newBuilder.build)
    responseObserver.onCompleted()
  }

  override protected def executor(
      workloads: com.couchbase.client.protocol.run.Workloads,
      counters: Counters,
      api: API
  ): SdkCommandExecutor = {
    if (api == API.DEFAULT) {
      new ScalaSdkCommandExecutor(clusterConnections(workloads.getClusterConnectionId), counters)
    } else if (api == API.ASYNC) {
      new ReactiveScalaSdkCommandExecutor(
        clusterConnections(workloads.getClusterConnectionId),
        counters
      )
    } else throw new UnsupportedOperationException("Unknown API")
  }

  override protected def transactionsExecutor(
      workloads: Workloads,
      counters: Counters
  ): TransactionCommandExecutor = null
}
