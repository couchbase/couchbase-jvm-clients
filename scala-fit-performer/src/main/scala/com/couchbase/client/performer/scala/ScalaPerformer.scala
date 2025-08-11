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

// [if:1.5.0]
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent
import com.couchbase.client.performer.scala.transaction.{
  ScalaTransactionCommandExecutor,
  TransactionBlocking,
  TransactionMarshaller
}
import com.couchbase.client.core.transaction.cleanup.{ClientRecord, TransactionsCleaner}
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord
import com.couchbase.client.core.transaction.config.{
  CoreMergedTransactionConfig,
  CoreTransactionsConfig
}
import com.couchbase.client.core.transaction.log.CoreTransactionLogger
import com.couchbase.client.scala.transactions.config.TransactionsConfig
import com.couchbase.client.performer.scala.util.HooksUtil
import com.couchbase.utils.ResultsUtil
// [if:1.7.2]
import com.couchbase.client.core.transaction.forwards.CoreTransactionsExtension
import com.couchbase.client.scala.transactions.internal.TransactionsSupportedExtensionsUtil
import com.couchbase.client.scala.transactions.internal.TransactionsSupportedExtensionsUtil.Supported
// [end]
// [if:<1.7.2]
// format: off
//? import com.couchbase.client.core.transaction.forwards.{Extension, Supported}
// format: on
// [end]
// [end]

import com.couchbase.client.performer.core.commands.TransactionCommandExecutor
import com.couchbase.client.performer.core.util.VersionUtil
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.logging.{LogRedaction, RedactionLevel}
import com.couchbase.client.performer.core.CorePerformer
import com.couchbase.client.performer.core.commands.SdkCommandExecutor
import com.couchbase.client.performer.core.perf.Counters
import com.couchbase.client.performer.scala.util.{Capabilities, ClusterConnection}
import com.couchbase.client.protocol.performer.{Caps, PerformerCapsFetchResponse}
import com.couchbase.client.protocol.run.Workloads
import com.couchbase.client.protocol.shared._
import com.couchbase.client.protocol.transactions._
import io.grpc.stub.StreamObserver
import io.grpc.{ServerBuilder, Status}
import org.slf4j.LoggerFactory

import java.util.Optional
import scala.collection.JavaConverters._
import scala.jdk.OptionConverters._

object ScalaPerformer {
  private val logger = LoggerFactory.getLogger(classOf[ScalaPerformer])

  def main(args: Array[String]): Unit = {
    var port = 8060

    // Force that log redaction has been enabled
    LogRedaction.setRedactionLevel(RedactionLevel.PARTIAL)

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
      .addPerformerCaps(Caps.CLUSTER_CONFIG_INSECURE)
      .addPerformerCaps(Caps.CONTENT_AS_PERFORMER_VALIDATION)
      .addAllSdkImplementationCaps(Capabilities.sdkImplementationCaps)

    // [if:1.7.2]
    response.addPerformerCaps(Caps.TRANSACTIONS_SUPPORT_1)
    val supported       = Supported
    val protocolVersion = supported.protocolMajor() + "." + supported.protocolMinor()
    response.setTransactionsProtocolVersion(protocolVersion)
    val sdkVersionRaw = VersionUtil.introspectSDKVersionScala
    val sdkVersion    = if (sdkVersionRaw == null) {
      // Not entirely clear why this fails sometimes on CI, return something sort of sensible as a default.
      logger.warn("Unable to introspect the sdk version, forcing it to 1.5.0")
      "1.5.0"
    } else {
      sdkVersionRaw
    }
    response.setLibraryVersion(sdkVersion)

    Supported.extensions.asScala
      .filterNot(v =>
        // core-io does support this, but the Scala performer does not support the custom serializer required for
        // testing.  It's not very necessary since it gets tested by Java performer.
        v == CoreTransactionsExtension.EXT_SERIALIZATION
      )
      .foreach(ext => {
        try {
          val pc = com.couchbase.client.protocol.transactions.Caps.valueOf(ext.name)
          response.addTransactionImplementationsCaps(pc)
        } catch {
          case _: IllegalArgumentException =>

            // FIT and Java have used slightly different names for this
            if (ext.name == "EXT_CUSTOM_METADATA")
              response.addTransactionImplementationsCaps(
                com.couchbase.client.protocol.transactions.Caps.EXT_CUSTOM_METADATA_COLLECTION
              )
            else logger.warn("Could not find FIT extension for " + ext.name)
        }
      })
    // [end]

    // [if:1.5.0]
    // [if:<1.7.2]
    // format: off
//?    response.addPerformerCaps(Caps.TRANSACTIONS_SUPPORT_1)
//?    val supported = new Supported
//?    val protocolVersion = supported.protocolMajor + "." + supported.protocolMinor
//?    response.setTransactionsProtocolVersion("2.0")
//?    val sdkVersionRaw = VersionUtil.introspectSDKVersionScala
//?    val sdkVersion = if (sdkVersionRaw == null) {
//?      //? Not entirely clear why this fails sometimes on CI, return something sort of sensible as a default.
//?      logger.warn("Unable to introspect the sdk version, forcing it to 1.5.0")
//?      "1.5.0"
//?    }
//?    else {
//?      sdkVersionRaw
//?    }
//?    response.setLibraryVersion(sdkVersion)
//?    Extension.SUPPORTED.asScala
//?      .filterNot(v =>
//?        //? Scala does not yet support single query transactions.
//?        v == Extension.EXT_SINGLE_QUERY
//?          //? core-io does support this, but the Scala performer does not support the custom serializer required for
//?          //? testing.  It's not very necessary since it gets tested by Java performer.
//?          || v == Extension.EXT_SERIALIZATION)
//?      .foreach(ext => {
//?        try {
//?          val pc = com.couchbase.client.protocol.transactions.Caps.valueOf(ext.name)
//?          response.addTransactionImplementationsCaps(pc)
//?        } catch {
//?          case _: IllegalArgumentException =>
//?            //? FIT and Java have used slightly different names for this
//?            if (ext.name == "EXT_CUSTOM_METADATA") response.addTransactionImplementationsCaps(com.couchbase.client.protocol.transactions.Caps.EXT_CUSTOM_METADATA_COLLECTION)
//?            else logger.warn("Could not find FIT extension for " + ext.name)
//?        }
//?      })
    // format: on
    // [end]
    // [end]
  }

  override def clusterConnectionCreate(
      request: ClusterConnectionCreateRequest,
      responseObserver: StreamObserver[ClusterConnectionCreateResponse]
  ): Unit = {
    try {
      val connection =
        new ClusterConnection(request, () => clusterConnections(request.getClusterConnectionId))
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
    try {
      clusterConnections.foreach(cc => cc._2.cluster.disconnect())
      clusterConnections.clear()
      responseObserver.onNext(DisconnectConnectionsResponse.newBuilder.build)
      responseObserver.onCompleted()
    } catch {
      case err: Throwable =>
        logger.error("Operation failed during disconnectConnections due to {}", err.getMessage)
        responseObserver.onError(Status.ABORTED.withDescription(err.toString).asException)
    }
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
  ): TransactionCommandExecutor = {
    // [start:1.5.0]
    val connection: ClusterConnection = getClusterConnection(workloads.getClusterConnectionId)
    new ScalaTransactionCommandExecutor(connection, counters, Map.empty)
    // [end:1.5.0]
    // [if:<1.5.0]
    // format: off
    //? null
    // format: on
    // [end]
  }

  def getClusterConnection(clusterConnectionId: String): ClusterConnection = clusterConnections(
    clusterConnectionId
  )

  private def collectionIdentifierFor(doc: DocId) = new CollectionIdentifier(
    doc.getBucketName,
    Optional.of(doc.getScopeName),
    Optional.of(doc.getCollectionName)
  )

  override def echo(request: EchoRequest, responseObserver: StreamObserver[EchoResponse]): Unit = {
    logger.info(
      "================ %s : %s ================ ".format(request.getTestName, request.getMessage)
    )
    responseObserver.onNext(EchoResponse.newBuilder.build)
    responseObserver.onCompleted()
  }

  // [start:1.5.0]
  override def transactionCreate(
      request: TransactionCreateRequest,
      responseObserver: StreamObserver[com.couchbase.client.protocol.transactions.TransactionResult]
  ): Unit = {
    try {
      val connection: ClusterConnection = getClusterConnection(request.getClusterConnectionId)
      logger.info("Starting transaction on cluster connection {}", request.getClusterConnectionId)
      val counters = new Counters()
      val e        = new ScalaTransactionCommandExecutor(connection, counters, Map.empty)
      val response = TransactionBlocking.run(connection, request, Some(e), false, Map.empty)
      responseObserver.onNext(response)
      responseObserver.onCompleted()
    } catch {
      case err: RuntimeException =>
        logger.error("Operation failed during transactionCreate due to :  " + err)
        err.printStackTrace()
        responseObserver.onError(Status.ABORTED.withDescription(err.toString).asException)
    }
  }

  override def transactionStream(
      toTest: StreamObserver[TransactionStreamPerformerToDriver]
  ): StreamObserver[TransactionStreamDriverToPerformer] = {
    val marshaller = new TransactionMarshaller(clusterConnections, Map.empty)
    marshaller.run(toTest)
  }

  override def transactionCleanup(
      request: TransactionCleanupRequest,
      responseObserver: StreamObserver[TransactionCleanupAttempt]
  ): Unit = {
    try {
      logger.info("Starting transaction cleanup attempt")
      // Only the KV timeout is used from this
      val config       = TransactionsConfig()
      val connection   = getClusterConnection(request.getClusterConnectionId)
      val collection   = collectionIdentifierFor(request.getAtr)
      val cleanupHooks =
        HooksUtil.configureCleanupHooks(request.getHookList.asScala, () => connection)
      // [if:1.7.2]
      val cleaner = new TransactionsCleaner(
        connection.cluster.async.core,
        cleanupHooks,
        TransactionsSupportedExtensionsUtil.Supported
      )
      // [end]
      // [if:<1.7.2]
      // format: off
      //? val cleaner = new TransactionsCleaner(connection.cluster.async.core, cleanupHooks)
      // format: on
      // [end]
      val l        = new CoreTransactionLogger(null, "")
      val merged   = new CoreMergedTransactionConfig(config.toCore)
      val atrEntry = ActiveTransactionRecord
        .findEntryForTransaction(
          connection.cluster.async.core,
          collection,
          request.getAtr.getDocId,
          request.getAttemptId,
          merged,
          null,
          l
        )
        .block
        .asScala
      val response: TransactionCleanupAttempt = atrEntry match {
        case Some(value) =>
          val result = cleaner
            .cleanupATREntry(collection, request.getAtrId, request.getAttemptId, value, false)
            .block
          ResultsUtil.mapCleanupAttempt(result, atrEntry)
        case None =>
          // Can happen if 2+ cleanups are being executed concurrently
          TransactionCleanupAttempt.newBuilder
            .setSuccess(false)
            .setAtr(request.getAtr)
            .setAttemptId(request.getAttemptId)
            .addLogs("Failed at performer to get ATR entry before running cleanupATREntry")
            .build

      }
      logger.info("Finished transaction cleanup attempt, success={}", response.getSuccess)
      responseObserver.onNext(response)
      responseObserver.onCompleted()
    } catch {
      case err: RuntimeException =>
        logger.error("Operation failed during transactionCleanup due to : " + err)
        responseObserver.onError(Status.ABORTED.withDescription(err.toString).asException)
    }
  }

  override def clientRecordProcess(
      request: ClientRecordProcessRequest,
      responseObserver: StreamObserver[ClientRecordProcessResponse]
  ): Unit = {
    try {
      logger.info("Starting client record process attempt")
      val config: CoreTransactionsConfig   = TransactionsConfig().toCore
      val connection: ClusterConnection    = getClusterConnection(request.getClusterConnectionId)
      val collection: CollectionIdentifier = new CollectionIdentifier(
        request.getBucketName,
        Optional.of(request.getScopeName),
        Optional.of(request.getCollectionName)
      )
      val cr: ClientRecord =
        HooksUtil.configureClientRecordHooks(request.getHookList.asScala, connection)
      val response: ClientRecordProcessResponse.Builder = ClientRecordProcessResponse.newBuilder
      try {
        val result = cr.processClient(request.getClientUuid, collection, config, null).block
        response
          .setSuccess(true)
          .setNumActiveClients(result.numActiveClients)
          .setIndexOfThisClient(result.indexOfThisClient)
          .addAllExpiredClientIds(result.expiredClientIds)
          .setNumExistingClients(result.numExistingClients)
          .setNumExpiredClients(result.numExpiredClients)
          .setOverrideActive(result.overrideActive)
          .setOverrideEnabled(result.overrideEnabled)
          .setOverrideExpires(result.overrideExpires)
          .setCasNowNanos(result.casNow)
          .setClientUuid(request.getClientUuid)
          .build
      } catch {
        case err: RuntimeException =>
          logger.info(s"processClient failed with ${err}")
          response.setSuccess(false)
      }
      responseObserver.onNext(response.build)
      responseObserver.onCompleted()
    } catch {
      case err: RuntimeException =>
        logger.error("Operation failed during clientRecordProcess due to : " + err)
        responseObserver.onError(Status.ABORTED.withDescription(err.toString).asException)
    }
  }

  override def cleanupSetFetch(
      request: CleanupSetFetchRequest,
      responseObserver: StreamObserver[CleanupSetFetchResponse]
  ): Unit = {
    try {
      val connection: ClusterConnection = getClusterConnection(request.getClusterConnectionId)
      val cleanupSet = connection.cluster.async.core.transactionsCleanup.cleanupSet.asScala
        .map(cs =>
          Collection.newBuilder
            .setBucketName(cs.bucket)
            .setScopeName(cs.scope.orElse(CollectionIdentifier.DEFAULT_SCOPE))
            .setCollectionName(cs.collection.orElse(CollectionIdentifier.DEFAULT_COLLECTION))
            .build
        )
        .asJava
      responseObserver.onNext(
        CleanupSetFetchResponse.newBuilder
          .setCleanupSet(
            CleanupSet.newBuilder
              .addAllCleanupSet(cleanupSet)
          )
          .build
      )
      responseObserver.onCompleted()
    } catch {
      case err: Throwable =>
        logger.error("Operation failed during cleanupSetFetch due to {}", err.toString)
        responseObserver.onError(Status.ABORTED.withDescription(err.toString).asException)
    }
  }
  // [end:1.5.0]
}
