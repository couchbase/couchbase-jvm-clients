/*
 * Copyright (c) 2023 Couchbase, Inc.
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
// [skip:<1.5.0]

package com.couchbase.client.performer.scala.kv

import com.couchbase.client.performer.core.perf.PerRun
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.{convertException, convertTranscoder, setSuccess}
import com.couchbase.client.performer.scala.util.{ClusterConnection, ContentAsUtil, ScalaFluxStreamer, ScalaIteratorStreamer}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.{CollectionLevelCommand, Command}
import com.couchbase.client.protocol.shared.{ContentAs, DocLocation}
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.kv._
import reactor.core.scala.publisher.{SFlux, SMono}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

object GetReplicaHelper {
  def handle(
      perRun: PerRun,
      connection: ClusterConnection,
      command: com.couchbase.client.protocol.sdk.Command,
      docId: (DocLocation) => String
  ): com.couchbase.client.protocol.run.Result.Builder = {
    val clc = command.getCollectionCommand
    val out = com.couchbase.client.protocol.run.Result.newBuilder()
    out.setInitiated(getTimeNow)

    if (clc.hasGetAllReplicas) {
      handleGetAllReplicas(perRun, connection, command, docId, clc, out)
    } else if (clc.hasGetAnyReplica) {
      handleGetAnyReplica(connection, command, docId, clc, out)
    }

    out
  }

  def handleReactive(
      perRun: PerRun,
      connection: ClusterConnection,
      command: com.couchbase.client.protocol.sdk.Command,
      docId: (DocLocation) => String
  ): SMono[com.couchbase.client.protocol.run.Result.Builder] = {
    val clc = command.getCollectionCommand
    val out = com.couchbase.client.protocol.run.Result.newBuilder()
    out.setInitiated(getTimeNow)

    val result: SMono[Unit] = if (clc.hasGetAllReplicas) {
      handleGetAllReplicasReactive(perRun, connection, command, docId, clc, out)
    } else {
      if (!clc.hasGetAnyReplica) {
        throw new UnsupportedOperationException()
      }
      handleGetAnyReplicaReactive(connection, command, docId, clc, out)
    }

    result.map(_ => out)
  }

  private def handleGetAnyReplica(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): Unit = {
    val req        = clc.getGetAnyReplica
    val collection = connection.collection(req.getLocation)
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val start = System.nanoTime

    val result = options match {
      case Some(opts) => collection.getAnyReplica(docId, opts).get
      case None       => collection.getAnyReplica(docId).get
    }

    out.setElapsedNanos(System.nanoTime - start)

    if (command.getReturnResult) {
      out.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setGetReplicaResult(populateResult(Some(req.getContentAs), result))
      )
    } else setSuccess(out)
  }

  private def handleGetAllReplicas(
      perRun: PerRun,
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): Unit = {
    val req        = clc.getGetAllReplicas
    val collection = connection.collection(req.getLocation)
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val start = System.nanoTime

    val iterator = options match {
      case Some(opts) => collection.getAllReplicas(docId, opts)
      case None       => collection.getAllReplicas(docId)
    }

    out.setElapsedNanos(System.nanoTime - start)

    val streamer = new ScalaIteratorStreamer[GetReplicaResult](
      iterator.toIterator,
      perRun,
      req.getStreamConfig.getStreamId,
      req.getStreamConfig,
      (r: GetReplicaResult) => {
        com.couchbase.client.protocol.run.Result.newBuilder
          .setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder
              .setGetReplicaResult(
                populateResult(Some(req.getContentAs), r, Some(req.getStreamConfig.getStreamId))
              )
          )
          .build
      },
      (err: Throwable) => convertException(err)
    )
    perRun.streamerOwner.addAndStart(streamer)
    out.setStream(
      com.couchbase.client.protocol.streams.Signal.newBuilder
        .setCreated(
          com.couchbase.client.protocol.streams.Created.newBuilder
            .setType(com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN)
            .setStreamId(streamer.streamId)
        )
    )
  }

  private def handleGetAnyReplicaReactive(
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): SMono[Unit] = {
    val req        = clc.getGetAnyReplica
    val collection = connection.collection(req.getLocation).reactive
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val start = System.nanoTime

    (options match {
      case Some(opts) => collection.getAnyReplica(docId, opts)
      case None       => collection.getAnyReplica(docId)
    }).map(result => {
      out.setElapsedNanos(System.nanoTime - start)

      if (command.getReturnResult) {
        out.setSdk(
          com.couchbase.client.protocol.sdk.Result.newBuilder
            .setGetReplicaResult(populateResult(Some(req.getContentAs), result))
        )
      } else setSuccess(out)
    })
  }

  private def handleGetAllReplicasReactive(
      perRun: PerRun,
      connection: ClusterConnection,
      command: Command,
      getId: (DocLocation) => String,
      clc: CollectionLevelCommand,
      out: Result.Builder
  ): SMono[Unit] = {
    // [start:<1.4.10]
    /*
        throw new UnsupportedOperationException("This version of the Scala SDK does not support getAllReplicas")
        // [end:<1.4.10]
     */

    // [start:1.4.10]
    val req        = clc.getGetAllReplicas
    val collection = connection.collection(req.getLocation).reactive
    val options    = createOptions(req)
    val docId      = getId(req.getLocation)

    val start = System.nanoTime

    val results: SFlux[GetReplicaResult] = options match {
      case Some(opts) => collection.getAllReplicas(docId, opts)
      case None       => collection.getAllReplicas(docId)
    }

    out.setElapsedNanos(System.nanoTime - start)

    val streamer = new ScalaFluxStreamer[GetReplicaResult](
      results,
      perRun,
      req.getStreamConfig.getStreamId,
      req.getStreamConfig,
      (r: GetReplicaResult) => {
        com.couchbase.client.protocol.run.Result.newBuilder
          .setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder
              .setGetReplicaResult(
                populateResult(Some(req.getContentAs), r, Some(req.getStreamConfig.getStreamId))
              )
          )
          .build
      },
      (err: Throwable) => convertException(err)
    )
    perRun.streamerOwner.addAndStart(streamer)
    out.setStream(
      com.couchbase.client.protocol.streams.Signal.newBuilder
        .setCreated(
          com.couchbase.client.protocol.streams.Created.newBuilder
            .setType(com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN)
            .setStreamId(streamer.streamId)
        )
    )

    SMono.empty
    // [end:1.4.10]
  }

  private def createOptions(
      request: com.couchbase.client.protocol.sdk.kv.GetAnyReplica
  ) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = GetAnyReplicaOptions()
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      Some(out)
    } else None
  }

  private def createOptions(
      request: com.couchbase.client.protocol.sdk.kv.GetAllReplicas
  ) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = GetAllReplicasOptions()
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      Some(out)
    } else None
  }

  def populateResult(
      contentAs: Option[ContentAs],
      value: GetReplicaResult,
      streamId: Option[String] = None
  ): com.couchbase.client.protocol.sdk.kv.GetReplicaResult = {
    val builder = com.couchbase.client.protocol.sdk.kv.GetReplicaResult.newBuilder
      .setCas(value.cas)

    streamId.foreach(v => builder.setStreamId(v))

    contentAs match {
      case Some(ca) =>
        builder.setContent(
          ContentAsUtil
            .contentType(
              ca,
              () => value.contentAs[Array[Byte]],
              () => value.contentAs[String],
              () => value.contentAs[JsonObject],
              () => value.contentAs[JsonArray],
              () => value.contentAs[Boolean],
              () => value.contentAs[Int],
              () => value.contentAs[Double]
            )
            .get
        )

      case _ =>
    }

    // [start:1.0.9]
    value.expiryTime.foreach(et => builder.setExpiryTime(et.getEpochSecond))
    // [end:1.0.9]

    builder.build
  }
}
