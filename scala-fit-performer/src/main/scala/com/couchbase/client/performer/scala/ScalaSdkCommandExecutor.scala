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

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.performer.core.commands.SdkCommandExecutor
import com.couchbase.client.performer.core.perf.Counters
import com.couchbase.client.performer.core.util.ErrorUtil
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.protocol
import com.couchbase.client.protocol.shared
import com.couchbase.client.protocol._
import com.couchbase.client.performer.scala.util.ClusterConnection
import com.couchbase.client.scala.codec.{JsonTranscoder, LegacyTranscoder, RawBinaryTranscoder, RawJsonTranscoder, RawStringTranscoder, Transcoder}
import com.couchbase.client.scala.durability.{Durability, PersistTo, ReplicateTo}
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.kv.{GetOptions, GetResult, InsertOptions, MutationResult, RemoveOptions, ReplaceOptions, UpsertOptions}
import com.couchbase.client.protocol
import com.couchbase.client.protocol.shared.{CouchbaseExceptionEx, CouchbaseExceptionType, ExceptionOther}
import com.google.protobuf.ByteString

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

class ScalaSdkCommandExecutor(val connection: ClusterConnection, val counters: Counters) extends SdkCommandExecutor(counters) {
  override protected def convertException(raw: Throwable): com.couchbase.client.protocol.shared.Exception = {
    val ret = com.couchbase.client.protocol.shared.Exception.newBuilder

    if (raw.isInstanceOf[CouchbaseException] || raw.isInstanceOf[UnsupportedOperationException]) {
      val typ = if (raw.isInstanceOf[UnsupportedOperationException]) CouchbaseExceptionType.SDK_UNSUPPORTED_OPERATION_EXCEPTION
      else ErrorUtil.convertException(raw.asInstanceOf[CouchbaseException])

      val out = CouchbaseExceptionEx.newBuilder
        .setName(raw.getClass.getSimpleName)
        .setType(typ)
        .setSerialized(raw.toString)
      if (raw.getCause != null) {
        out.setCause(convertException(raw.getCause))
      }
      ret.setCouchbase(out)
    }
    else ret.setOther(ExceptionOther.newBuilder.setName(raw.getClass.getSimpleName).setSerialized(raw.toString))

    ret.build
  }

  def convertDurability(durability: protocol.shared.DurabilityType): Durability = {
    if (durability.hasDurabilityLevel()) {
      durability.getDurabilityLevel() match {
        case shared.Durability.NONE => Durability.Disabled
        case shared.Durability.MAJORITY => Durability.Majority
        case shared.Durability.MAJORITY_AND_PERSIST_TO_ACTIVE => Durability.MajorityAndPersistToActive
        case shared.Durability.PERSIST_TO_MAJORITY => Durability.PersistToMajority
        case _ => throw new UnsupportedOperationException("Unknown durability level")
      }
    }
    else if (durability.hasObserve) {
      Durability.ClientVerified(durability.getObserve.getReplicateTo match {
        case shared.ReplicateTo.REPLICATE_TO_NONE => ReplicateTo.None
        case shared.ReplicateTo.REPLICATE_TO_ONE => ReplicateTo.One
        case shared.ReplicateTo.REPLICATE_TO_TWO => ReplicateTo.Two
        case shared.ReplicateTo.REPLICATE_TO_THREE => ReplicateTo.Three
        case _ => throw new UnsupportedOperationException("Unknown replicateTo level")
      }, durability.getObserve.getPersistTo match {
        case shared.PersistTo.PERSIST_TO_NONE => PersistTo.None
        case shared.PersistTo.PERSIST_TO_ACTIVE => PersistTo.Active
        case shared.PersistTo.PERSIST_TO_ONE => PersistTo.One
        case shared.PersistTo.PERSIST_TO_TWO => PersistTo.Two
        case shared.PersistTo.PERSIST_TO_THREE => PersistTo.Three
        case shared.PersistTo.PERSIST_TO_FOUR => PersistTo.Four
        case _ => throw new UnsupportedOperationException("Unknown persistTo level")
      })
    }
    else {
      throw new UnsupportedOperationException("Unknown durability")
    }
  }

  def convertExpiry(expiry: shared.Expiry): Either[Instant, Duration] = {
    if (expiry.hasAbsoluteEpochSecs) {
      Left(Instant.ofEpochSecond(expiry.getAbsoluteEpochSecs))
    }
    else if (expiry.hasRelativeSecs) {
      Right(Duration.create(expiry.getRelativeSecs, TimeUnit.SECONDS))
    }
    else {
      throw new UnsupportedOperationException("Unknown expiry")
    }
  }

  override protected def performOperation(op: com.couchbase.client.protocol.sdk.Command): com.couchbase.client.protocol.run.Result = {
    val result = com.couchbase.client.protocol.run.Result.newBuilder()

    if (op.hasInsert) {
      val request = op.getInsert
      val collection = connection.collection(request.getLocation)
      val content = convertContent(request.getContent)
      val docId = getDocId(request.getLocation)
      val options = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) collection.insert(docId, content).get
      else collection.insert(docId, content, options).get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    }
    else if (op.hasGet) {
      val request = op.getGet
      val collection = connection.collection(request.getLocation)
      val docId = getDocId(request.getLocation)
      val options = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) collection.get(docId).get
      else collection.get(docId, options).get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    }
    else if (op.hasRemove) {
      val request = op.getRemove
      val collection = connection.collection(request.getLocation)
      val docId = getDocId(request.getLocation)
      val options = createOptions(request)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) collection.remove(docId).get
      else collection.remove(docId, options).get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    }
    else if (op.hasReplace) {
      val request = op.getReplace
      val collection = connection.collection(request.getLocation)
      val docId = getDocId(request.getLocation)
      val options = createOptions(request)
      val content = convertContent(request.getContent)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) collection.replace(docId, content).get
      else collection.replace(docId, content, options).get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    }
    else if (op.hasUpsert) {
      val request = op.getUpsert
      val collection = connection.collection(request.getLocation)
      val docId = getDocId(request.getLocation)
      val options = createOptions(request)
      val content = convertContent(request.getContent)
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val r = if (options == null) collection.upsert(docId, content).get
      else collection.upsert(docId, content, options).get
      result.setElapsedNanos(System.nanoTime - start)
      if (op.getReturnResult) populateResult(result, r)
      else setSuccess(result)
    }
    else throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))

    result.build
  }

  private def convertContent(content: shared.Content): String = {
    if (content.hasPassthroughString) content.getPassthroughString
    else throw new UnsupportedOperationException("Unknown content")
  }

  private def createOptions(request: com.couchbase.client.protocol.sdk.kv.Insert) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out = InsertOptions()
      if (opts.hasTimeoutMsecs) out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasExpiry) out = convertExpiry(opts.getExpiry) match {
        case Left(expiry) =>
          // [start:1.1.0]
          out.expiry(expiry)
          // [end:1.1.0]
          // [start:<1.1.0]
          throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
        // [end:<1.1.0]
        case Right(expiry) => out.expiry(expiry)
      }
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    }
    else null
  }

  private def createOptions(request: com.couchbase.client.protocol.sdk.kv.Remove) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out = RemoveOptions()
      if (opts.hasTimeoutMsecs) out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasCas) out = out.cas(opts.getCas)
      out
    }
    else null
  }

  private def createOptions(request: com.couchbase.client.protocol.sdk.kv.Get) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out = GetOptions()
      if (opts.hasTimeoutMsecs) out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasWithExpiry) out = out.withExpiry(opts.getWithExpiry)
      if (opts.getProjectionCount > 0) out = out.project(opts.getProjectionList.asByteStringList().toSeq.map(v => v.toString))
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    }
    else null
  }

  private def createOptions(request: com.couchbase.client.protocol.sdk.kv.Replace) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out = ReplaceOptions()
      if (opts.hasTimeoutMsecs) out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasExpiry) out = convertExpiry(opts.getExpiry) match {
        case Left(expiry) =>
          // [start:1.1.0]
          out.expiry(expiry)
          // [end:1.1.0]
          // [start:<1.1.0]
          throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
          // [end:<1.1.0]
        case Right(expiry) => out.expiry(expiry)
      }
      if (opts.hasPreserveExpiry) {
        // [start:1.1.5]
        out.preserveExpiry(opts.getPreserveExpiry)
        // [end:1.1.5]
        // [start:<1.1.5]
        throw new UnsupportedOperationException()
        // [end:<1.1.5]
      }
      if (opts.hasCas) out = out.cas(opts.getCas)
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    }
    else null
  }

  private def createOptions(request: com.couchbase.client.protocol.sdk.kv.Upsert) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out = UpsertOptions()
      if (opts.hasTimeoutMsecs) out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasExpiry) out = convertExpiry(opts.getExpiry) match {
        case Left(expiry) =>
          // [start:1.1.0]
          out.expiry(expiry)
          // [end:1.1.0]
          // [start:<1.1.0]
          throw new UnsupportedOperationException("This SDK version does not support this form of expiry");
          // [end:<1.1.0]
        case Right(expiry) => out.expiry(expiry)
      }
      if (opts.hasPreserveExpiry) {
        // [start:1.1.5]
        out.preserveExpiry(opts.getPreserveExpiry)
        // [end:1.1.5]
        // [start:<1.1.5]
        throw new UnsupportedOperationException()
        // [end:<1.1.5]
      }
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    }
    else null
  }

  private def convertTranscoder(transcoder: shared.Transcoder): Transcoder = {
    if (transcoder.hasRawJson) return RawJsonTranscoder.Instance
    else if (transcoder.hasJson) return JsonTranscoder.Instance
    else if (transcoder.hasLegacy) return LegacyTranscoder.Instance
    else if (transcoder.hasRawString) return RawStringTranscoder.Instance
    else if (transcoder.hasRawBinary) return RawBinaryTranscoder.Instance
    else throw new UnsupportedOperationException("Unknown transcoder")
  }

  private def setSuccess(result: com.couchbase.client.protocol.run.Result.Builder): Unit = {
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder.setSuccess(true))
  }

  private def populateResult(result: com.couchbase.client.protocol.run.Result.Builder, value: MutationResult): Unit = {
    val builder = com.couchbase.client.protocol.sdk.kv.MutationResult.newBuilder.setCas(value.cas)
    value.mutationToken.foreach(mt => builder.setMutationToken(com.couchbase.client.protocol.shared.MutationToken.newBuilder
      .setPartitionId(mt.partitionID)
      .setPartitionUuid(mt.partitionUUID)
      .setSequenceNumber(mt.sequenceNumber)
      .setBucketName(mt.bucketName)))
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder.setMutationResult(builder))
  }

  private def populateResult(result: com.couchbase.client.protocol.run.Result.Builder, value: GetResult): Unit = {
    val builder = com.couchbase.client.protocol.sdk.kv.GetResult.newBuilder
      .setCas(value.cas)
      .setContent(ByteString.copyFrom(value.contentAs[JsonObject].toString.getBytes))
    // [start:1.1.0]
    value.expiryTime.foreach(et => builder.setExpiryTime(et.getEpochSecond))
    // [end:1.1.0]
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder.setGetResult(builder))
  }

}