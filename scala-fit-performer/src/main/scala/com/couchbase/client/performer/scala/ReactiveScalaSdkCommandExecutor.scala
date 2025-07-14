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

import com.couchbase.client.performer.core.commands.SdkCommandExecutor
import com.couchbase.client.performer.core.perf.{Counters, PerRun}
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.Content.{ContentJson, ContentString}
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor._
import com.couchbase.client.performer.scala.kv.LookupInHelper
import com.couchbase.client.performer.scala.util.{ClusterConnection, ScalaFluxStreamer}
import com.couchbase.client.protocol.run.Result
import reactor.core.scala.publisher.SMono

import scala.concurrent.duration.DurationInt
import scala.runtime.Nothing$
import scala.util.Try
// [start:1.5.0]
import com.couchbase.client.scala.kv.ScanType.{RangeScan, SamplingScan}
// [end:1.5.0]
import com.couchbase.client.scala.kv._
// [start:1.4.11]
import com.couchbase.client.performer.scala.manager.BucketManagerHelper
import com.couchbase.client.performer.scala.manager.CollectionManagerHelper
// [end:1.4.11]

class ReactiveScalaSdkCommandExecutor(val connection: ClusterConnection, val counters: Counters)
    extends SdkCommandExecutor(counters) {
  override protected def convertException(
      raw: Throwable
  ): com.couchbase.client.protocol.shared.Exception = {
    ScalaSdkCommandExecutor.convertException(raw)
  }

  override protected def performOperation(
      op: com.couchbase.client.protocol.sdk.Command,
      perRun: PerRun
  ) = {
    val result = performOperationInternal(op, perRun)
    perRun.resultsStream.enqueue(result)
  }

  protected def performOperationInternal(
      op: com.couchbase.client.protocol.sdk.Command,
      perRun: PerRun
  ): com.couchbase.client.protocol.run.Result = {
    var result = com.couchbase.client.protocol.run.Result.newBuilder()

    if (op.hasInsert) {
      val request    = op.getInsert
      val collection = connection.collection(request.getLocation).reactive
      val content    = convertContent(request.getContent)
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r     = if (options == null) content match {
        case ContentString(value) => collection.insert(docId, value).block()
        case ContentJson(value)   => collection.insert(docId, value).block()
      }
      else
        content match {
          case ContentString(value) => collection.insert(docId, value, options).block()
          case ContentJson(value)   => collection.insert(docId, value, options).block()
        }
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    } else if (op.hasGet) {
      val request    = op.getGet
      val collection = connection.collection(request.getLocation).reactive
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r     =
        if (options == null) collection.get(docId).block()
        else collection.get(docId, options).block()
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(Some(request.getContentAs), result, r)
      else setSuccess(result)
    } else if (op.hasRemove) {
      val request    = op.getRemove
      val collection = connection.collection(request.getLocation).reactive
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r     =
        if (options == null) collection.remove(docId).block()
        else collection.remove(docId, options).block()
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    } else if (op.hasReplace) {
      val request    = op.getReplace
      val collection = connection.collection(request.getLocation).reactive
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      val content    = convertContent(request.getContent)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r     = if (options == null) content match {
        case ContentString(value) => collection.replace(docId, value).block()
        case ContentJson(value)   => collection.replace(docId, value).block()
      }
      else
        content match {
          case ContentString(value) => collection.replace(docId, value, options).block()
          case ContentJson(value)   => collection.replace(docId, value, options).block()
        }
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    } else if (op.hasUpsert) {
      val request    = op.getUpsert
      val collection = connection.collection(request.getLocation).reactive
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      val content    = convertContent(request.getContent)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r     = if (options == null) content match {
        case ContentString(value) => collection.upsert(docId, value).block()
        case ContentJson(value)   => collection.upsert(docId, value).block()
      }
      else
        content match {
          case ContentString(value) => collection.upsert(docId, value, options).block()
          case ContentJson(value)   => collection.upsert(docId, value, options).block()
        }
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
      // [start:1.5.0]
    } else if (op.hasRangeScan) {
      val request    = op.getRangeScan
      val collection = connection.collection(request.getCollection).reactive
      val options    = createOptions(request)
      val scanType   = convertScanType(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val flux  =
        if (options == null) collection.scan(scanType)
        else collection.scan(scanType, options)
      result.setElapsedNanos(System.nanoTime - start)
      val streamer = new ScalaFluxStreamer[ScanResult](
        flux,
        perRun,
        request.getStreamConfig.getStreamId,
        request.getStreamConfig,
        (r: ScanResult) => processScanResult(request, r.asInstanceOf[ScanResult]),
        (err: Throwable) => convertException(err)
      )
      perRun.streamerOwner.addAndStart(streamer)
      result.setStream(
        com.couchbase.client.protocol.streams.Signal.newBuilder
          .setCreated(
            com.couchbase.client.protocol.streams.Created.newBuilder
              .setType(com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN)
              .setStreamId(streamer.streamId)
          )
      )
      // [end:1.5.0]
    } else if (op.hasClusterCommand) {
      val clc     = op.getClusterCommand
      val cluster = connection.cluster.reactive

      if (clc.hasWaitUntilReady) {
        val request = clc.getWaitUntilReady
        logger.info(
          "Calling waitUntilReady with timeout " + request.getTimeoutMillis + " milliseconds."
        )
        val timeout = request.getTimeoutMillis.milliseconds

        var response: SMono[Unit] = null

        if (request.hasOptions) {
          val options = waitUntilReadyOptions(request)
          response = cluster.waitUntilReady(timeout, options)
        } else {
          response = cluster.waitUntilReady(timeout)
        }

        response
          .doOnSuccess(_ => {
            setSuccess(result)
            result.build()
          })
          .block()
      }
      // [start:1.4.11]
      else if (clc.hasBucketManager) {
        return BucketManagerHelper.handleBucketManagerReactive(cluster, op).block()
      }
      // [end:1.4.11]
    } else if (op.hasBucketCommand) {
      val blc    = op.getBucketCommand
      val bucket = connection.cluster.reactive.bucket(blc.getBucketName)

      if (blc.hasWaitUntilReady) {
        val request = blc.getWaitUntilReady
        logger.info(
          "Calling waitUntilReady on bucket " + bucket + " with timeout " + request.getTimeoutMillis + " milliseconds."
        )
        val timeout = request.getTimeoutMillis.milliseconds

        var response: SMono[Unit] = null

        if (request.hasOptions) {
          val options = waitUntilReadyOptions(request)
          response = bucket.waitUntilReady(timeout, options)
        } else {
          response = bucket.waitUntilReady(timeout)
        }

        response
          .`then`[Result](SMono.fromCallable[Result](() => {
            setSuccess(result)
            result.build()
          }))
          .block()
      }
    } else if (op.hasCollectionCommand) {
      val clc        = op.getCollectionCommand
      val collection = if (clc.hasCollection) {
        val coll = clc.getCollection
        Some(
          connection.cluster
            .bucket(coll.getBucketName)
            .scope(coll.getScopeName)
            .collection(coll.getCollectionName)
        )
      } else None

      if (clc.hasLookupIn || clc.hasLookupInAllReplicas || clc.hasLookupInAnyReplica) {
        result = LookupInHelper
          .handleLookupInReactive(perRun, connection, op, (loc) => getDocId(loc))
          .block()
      } else throw new UnsupportedOperationException()
    } else
      throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))

    result.build
  }
}
