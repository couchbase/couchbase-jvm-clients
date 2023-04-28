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
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor._
import com.couchbase.client.performer.scala.util.{ClusterConnection, ScalaIteratorStreamer}
import com.couchbase.client.protocol.run.Result
import reactor.core.scala.publisher.SMono

import scala.concurrent.duration.DurationInt
import scala.runtime.Nothing$
import scala.util.Try
// [start:1.4.1]
import com.couchbase.client.scala.kv.ScanType.{RangeScan, SamplingScan}
// [end:1.4.1]
import com.couchbase.client.scala.kv._

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
    val result = com.couchbase.client.protocol.run.Result.newBuilder()

    if (op.hasInsert) {
      val request    = op.getInsert
      val collection = connection.collection(request.getLocation)
      val content    = convertContent(request.getContent)
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) content match {
        case ContentString(value) => collection.insert(docId, value).get
        case ContentJson(value)   => collection.insert(docId, value).get
      }
      else
        content match {
          case ContentString(value) => collection.insert(docId, value, options).get
          case ContentJson(value)   => collection.insert(docId, value, options).get
        }
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    } else if (op.hasGet) {
      val request    = op.getGet
      val collection = connection.collection(request.getLocation)
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r =
        if (options == null) collection.get(docId).get
        else collection.get(docId, options).get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    } else if (op.hasRemove) {
      val request    = op.getRemove
      val collection = connection.collection(request.getLocation)
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r =
        if (options == null) collection.remove(docId).get
        else collection.remove(docId, options).get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    } else if (op.hasReplace) {
      val request    = op.getReplace
      val collection = connection.collection(request.getLocation)
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      val content    = convertContent(request.getContent)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) content match {
        case ContentString(value) => collection.replace(docId, value).get
        case ContentJson(value)   => collection.replace(docId, value).get
      }
      else
        content match {
          case ContentString(value) => collection.replace(docId, value, options).get
          case ContentJson(value)   => collection.replace(docId, value, options).get
        }
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    } else if (op.hasUpsert) {
      val request    = op.getUpsert
      val collection = connection.collection(request.getLocation)
      val docId      = getDocId(request.getLocation)
      val options    = createOptions(request)
      val content    = convertContent(request.getContent)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) content match {
        case ContentString(value) => collection.upsert(docId, value).get
        case ContentJson(value)   => collection.upsert(docId, value).get
      }
      else
        content match {
          case ContentString(value) => collection.upsert(docId, value, options).get
          case ContentJson(value)   => collection.upsert(docId, value, options).get
        }
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    }
    // [start:1.4.1]
    else if (op.hasRangeScan) {
      val request    = op.getRangeScan
      val collection = connection.collection(request.getCollection)
      val options    = createOptions(request)
      val scanType   = convertScanType(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val iterator =
        if (options == null) collection.scan(scanType)
        else collection.scan(scanType, options)
      result.setElapsedNanos(System.nanoTime - start)
      val streamer = new ScalaIteratorStreamer[ScanResult](
        iterator,
        perRun,
        request.getStreamConfig.getStreamId,
        request.getStreamConfig,
        (r: AnyRef) => processScanResult(request, r.asInstanceOf[ScanResult]),
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
    } else if (op.hasClusterCommand) {
        val clc = op.getClusterCommand
        val cluster = connection.cluster.reactive

        if (clc.hasWaitUntilReady) {
            val request = clc.getWaitUntilReady
            logger.info("Calling waitUntilReady with timeout " + request.getTimeoutMillis + " milliseconds.")
            val timeout = request.getTimeoutMillis.milliseconds

            var response: SMono[Unit] = null

            if (request.hasOptions) {
                val options = waitUntilReadyOptions(request)
                response = cluster.waitUntilReady(timeout, options)
            } else {
                response = cluster.waitUntilReady(timeout)
            }

            response.doOnSuccess(_ => {
                setSuccess(result)
                result.build()
            }).block()

        }

    } else if (op.hasBucketCommand) {
        val blc = op.getBucketCommand
        val bucket = connection.cluster.reactive.bucket(blc.getBucketName)

        if (blc.hasWaitUntilReady) {
            val request = blc.getWaitUntilReady
            logger.info("Calling waitUntilReady on bucket " + bucket + " with timeout " + request.getTimeoutMillis + " milliseconds.")
            val timeout = request.getTimeoutMillis.milliseconds

            var response: SMono[Unit] = null

            if (request.hasOptions) {
                val options = waitUntilReadyOptions(request)
                response = bucket.waitUntilReady(timeout, options)
            } else {
                response = bucket.waitUntilReady(timeout)
            }
            response.`then`[Result](SMono.fromCallable[Result](() => {
                setSuccess(result)
                result.build()
            })).block()
        }

    }
    // [end:1.4.1]
    else throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))

    result.build
  }
}
