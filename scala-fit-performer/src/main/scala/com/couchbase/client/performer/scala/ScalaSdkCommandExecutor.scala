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

import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.performer.core.commands.SdkCommandExecutor
import com.couchbase.client.performer.core.perf.{Counters, PerRun}
import com.couchbase.client.performer.core.util.ErrorUtil
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor._
import com.couchbase.client.performer.scala.kv.LookupInHelper
import com.couchbase.client.performer.scala.query.{QueryHelper, QueryIndexManagerHelper}
import com.couchbase.client.performer.scala.util.{ClusterConnection, ContentAsUtil, ScalaIteratorStreamer}
import com.couchbase.client.protocol
import com.couchbase.client.protocol.sdk.cluster.waituntilready.WaitUntilReadyRequest
import com.couchbase.client.protocol.sdk.kv.rangescan.{Scan, ScanTermChoice}
import com.couchbase.client.protocol.shared
import com.couchbase.client.protocol.shared.{CouchbaseExceptionEx, CouchbaseExceptionType, ExceptionOther}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.diagnostics.WaitUntilReadyOptions
import com.couchbase.client.scala.durability.{Durability, PersistTo, ReplicateTo}
import com.couchbase.client.scala.json.{JsonArray, JsonObject}

import scala.concurrent.duration.DurationInt
// [start:1.2.4]
import com.couchbase.client.performer.scala.eventing.EventingHelper
import com.couchbase.client.performer.scala.search.SearchHelper
// [end:1.2.4]
// [start:1.5.0]
import com.couchbase.client.scala.kv.ScanType.{RangeScan, SamplingScan}
// [end:1.5.0]
import com.couchbase.client.scala.kv._
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.google.protobuf.ByteString
// [start:1.4.11]
import com.couchbase.client.performer.scala.manager.BucketManagerHelper
import com.couchbase.client.performer.scala.manager.CollectionManagerHelper
// [end:1.4.11]

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.convert.ImplicitConversions._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

sealed trait Content
case class ContentString(value: String)   extends Content
case class ContentJson(value: JsonObject) extends Content

class ScalaSdkCommandExecutor(val connection: ClusterConnection, val counters: Counters)
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
      if (op.getReturnResult) populateResult(request, result, r)
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
    // [start:1.5.0]
    } else if (op.hasRangeScan) {
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
        iterator.get,
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
    // [end:1.5.0]
    } else if (op.hasClusterCommand) {
        val clc = op.getClusterCommand

        if (clc.hasQueryIndexManager) {
            result = QueryIndexManagerHelper.handleClusterQueryIndexManager(connection.cluster, op)
        }
        // [start:1.2.4]
        else if (clc.hasSearch) {
            result = SearchHelper.handleSearchBlocking(connection.cluster, clc.getSearch)
        } else if (clc.hasSearchIndexManager) {
            result = SearchHelper.handleClusterSearchIndexManager(connection.cluster, op)
        }
        // [end:1.2.4]
        else if (clc.hasWaitUntilReady) {
            val request = clc.getWaitUntilReady
            logger.info("Calling waitUntilReady with timeout " + request.getTimeoutMillis + " milliseconds.")
            val timeout = request.getTimeoutMillis.milliseconds

            if (request.hasOptions) {
                val options = waitUntilReadyOptions(request)
                connection.cluster.waitUntilReady(timeout, options)
            } else {
                connection.cluster.waitUntilReady(timeout)
            }

            setSuccess(result)

        }
        // [start:1.4.11]
        else if (clc.hasBucketManager) {
            result = BucketManagerHelper.handleBucketManager(connection.cluster, op)
        }
        // [end:1.4.11]
        // [start:1.2.4]
        else if (clc.hasEventingFunctionManager) {
            result = EventingHelper.handleEventingFunctionManager(connection.cluster, op)
        }
        // [end:1.2.4]
        else if (clc.hasQuery) {
          result = QueryHelper.handleClusterQuery(connection, op, clc)
        }
        else throw new UnsupportedOperationException("Unknown cluster command")
    } else if (op.hasBucketCommand) {
        val blc = op.getBucketCommand
        val bucket = connection.cluster.bucket(blc.getBucketName)

        if (blc.hasWaitUntilReady) {
            val request = blc.getWaitUntilReady
            logger.info("Calling waitUntilReady on bucket " + bucket + " with timeout " + request.getTimeoutMillis + " milliseconds.")
            val timeout = request.getTimeoutMillis.milliseconds

            if (request.hasOptions) {
                val options = waitUntilReadyOptions(request)
                bucket.waitUntilReady(timeout, options)
            } else {
                bucket.waitUntilReady(timeout)
            }

            setSuccess(result)
        }
      // [start:1.4.11]
      if (blc.hasCollectionManager) {
        result = CollectionManagerHelper.handleCollectionManager(connection.cluster, op)
      }
      // [end:1.4.11]
    } else if (op.hasScopeCommand) {
      val slc = op.getScopeCommand

      val scope = if (slc.hasScope) {
        Some(connection.cluster.bucket(slc.getScope.getBucketName).scope(slc.getScope.getScopeName))
      }
      else None

      if (slc.hasQuery) {
        result = QueryHelper.handleScopeQuery(scope.get, op, slc)
      }
    } else if (op.hasCollectionCommand) {
      val clc  = op.getCollectionCommand
      val collection = if (clc.hasCollection) {
          val coll = clc.getCollection
          Some(connection.cluster
                  .bucket(coll.getBucketName)
                  .scope(coll.getScopeName)
                  .collection(coll.getCollectionName))
      }
      else None

      if (clc.hasQueryIndexManager) {
        result = QueryIndexManagerHelper.handleCollectionQueryIndexManager(collection.get, op)
      }
      else if (clc.hasLookupIn || clc.hasLookupInAllReplicas || clc.hasLookupInAnyReplica) {
        result = LookupInHelper.handleLookupIn(perRun, connection, op, (loc) => getDocId(loc))
      }
      else throw new UnsupportedOperationException("Unknown collection command")


    } else
      throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))

    result.build
  }
}

object ScalaSdkCommandExecutor {
  def convertDurability(durability: protocol.shared.DurabilityType): Durability = {
    if (durability.hasDurabilityLevel()) {
      convertDurability(durability.getDurabilityLevel)
    } else if (durability.hasObserve) {
      Durability.ClientVerified(
        durability.getObserve.getReplicateTo match {
          case shared.ReplicateTo.REPLICATE_TO_NONE  => ReplicateTo.None
          case shared.ReplicateTo.REPLICATE_TO_ONE   => ReplicateTo.One
          case shared.ReplicateTo.REPLICATE_TO_TWO   => ReplicateTo.Two
          case shared.ReplicateTo.REPLICATE_TO_THREE => ReplicateTo.Three
          case _                                     => throw new UnsupportedOperationException("Unknown replicateTo level")
        },
        durability.getObserve.getPersistTo match {
          case shared.PersistTo.PERSIST_TO_NONE   => PersistTo.None
          case shared.PersistTo.PERSIST_TO_ACTIVE => PersistTo.Active
          case shared.PersistTo.PERSIST_TO_ONE    => PersistTo.One
          case shared.PersistTo.PERSIST_TO_TWO    => PersistTo.Two
          case shared.PersistTo.PERSIST_TO_THREE  => PersistTo.Three
          case shared.PersistTo.PERSIST_TO_FOUR   => PersistTo.Four
          case _                                  => throw new UnsupportedOperationException("Unknown persistTo level")
        }
      )
    } else {
      throw new UnsupportedOperationException("Unknown durability")
    }
  }
  def convertDurability(durability: protocol.shared.Durability): Durability = {
    durability match {
      case shared.Durability.NONE => Durability.Disabled
      case shared.Durability.MAJORITY => Durability.Majority
      case shared.Durability.MAJORITY_AND_PERSIST_TO_ACTIVE => Durability.MajorityAndPersistToActive
      case shared.Durability.PERSIST_TO_MAJORITY => Durability.PersistToMajority
      case _ => throw new UnsupportedOperationException("Unknown durability level")
    }
  }

  def convertExpiry(expiry: shared.Expiry): Either[Instant, Duration] = {
    if (expiry.hasAbsoluteEpochSecs) {
      Left(Instant.ofEpochSecond(expiry.getAbsoluteEpochSecs))
    } else if (expiry.hasRelativeSecs) {
      Right(Duration.create(expiry.getRelativeSecs, TimeUnit.SECONDS))
    } else {
      throw new UnsupportedOperationException("Unknown expiry")
    }
  }

  // [start:1.5.0]
  def processScanResult(request: Scan, r: ScanResult): com.couchbase.client.protocol.run.Result = {
    val builder = com.couchbase.client.protocol.sdk.kv.rangescan.ScanResult.newBuilder
      .setId(r.id)
      .setIdOnly(r.idOnly)
      .setStreamId(request.getStreamConfig.getStreamId)

    r.cas.foreach(v => builder.setCas(v))
    r.expiryTime.foreach(v => builder.setExpiryTime(v.getEpochSecond))

    if (request.hasContentAs) {
        val content = ContentAsUtil.contentType(request.getContentAs,
            () => r.contentAs[Array[Byte]],
            () => r.contentAs[String],
            () => r.contentAs[JsonObject],
            () => r.contentAs[JsonArray],
            () => r.contentAs[Boolean],
            () => r.contentAs[Int],
            () => r.contentAs[Double])

      content match {
        case Success(b) =>
          builder.setContent(b)

          com.couchbase.client.protocol.run.Result.newBuilder
            .setSdk(
              com.couchbase.client.protocol.sdk.Result.newBuilder
                .setRangeScanResult(builder.build)
            )
            .build

        case Failure(err) =>
          com.couchbase.client.protocol.run.Result.newBuilder
            .setStream(
              com.couchbase.client.protocol.streams.Signal.newBuilder
                .setError(
                  com.couchbase.client.protocol.streams.Error.newBuilder
                    .setStreamId(request.getStreamConfig.getStreamId)
                    .setException(convertException(err))
                )
            )
            .build
      }
    } else {
      com.couchbase.client.protocol.run.Result.newBuilder
        .setSdk(
          com.couchbase.client.protocol.sdk.Result.newBuilder
            .setRangeScanResult(builder.build)
        )
        .build
    }
  }

  def convertScanTerm(st: ScanTermChoice): Option[ScanTerm] = {
    if (st.hasDefault) {
      None
    } else if (st.hasMaximum) {
      None
    } else if (st.hasMinimum) {
      None
    } else if (st.hasTerm) {
      val stt = st.getTerm
      if (stt.hasExclusive && stt.getExclusive) {
        if (stt.hasAsString) {
          Some(com.couchbase.client.scala.kv.ScanTerm.exclusive(stt.getAsString))
        } else throw new UnsupportedOperationException("Unknown scan term");
      }
      else if (stt.hasAsString) {
        Some(com.couchbase.client.scala.kv.ScanTerm.inclusive(stt.getAsString))
      } else throw new UnsupportedOperationException("Unknown scan term");
    } else throw new UnsupportedOperationException("Unknown scan term")
  }

  def convertScanType(request: Scan): ScanType = {
    if (request.getScanType.hasRange) {
      val scan = request.getScanType.getRange
      if (scan.hasFromTo) {
        val from = convertScanTerm(scan.getFromTo.getFrom)
        val to   = convertScanTerm(scan.getFromTo.getTo)
        RangeScan(from, to)
      } else if (scan.hasDocIdPrefix) {
        com.couchbase.client.scala.kv.ScanType.PrefixScan(scan.getDocIdPrefix)
      } else throw new UnsupportedOperationException("Unknown scan type")
    } else if (request.getScanType.hasSampling) {
      val scan = request.getScanType.getSampling
      if (scan.hasSeed) {
        SamplingScan(scan.getLimit, scan.getSeed)
      } else {
        SamplingScan(scan.getLimit)
      }
    } else throw new UnsupportedOperationException("Unknown scan type")
  }
  // [end:1.5.0]

  def convertContent(content: shared.Content): Content = {
    if (content.hasPassthroughString) ContentString(content.getPassthroughString)
    else if (content.hasConvertToJson)
      ContentJson(
        JsonObject
          .fromJson(new String(content.getConvertToJson.toByteArray, StandardCharsets.UTF_8))
      )
    else throw new UnsupportedOperationException("Unknown content")
  }

  def createOptions(request: com.couchbase.client.protocol.sdk.kv.Insert) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = InsertOptions()
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasExpiry) out = convertExpiry(opts.getExpiry) match {
        case Left(expiry) =>
          // [start:1.1.0]
          out.expiry(expiry)
          // [end:1.1.0]
          // [start:<1.1.0]
/*
          throw new UnsupportedOperationException(
            "This SDK version does not support this form of expiry"
          );
        // [end:<1.1.0]
*/
        case Right(expiry) => out.expiry(expiry)
      }
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    } else null
  }

  def createOptions(request: com.couchbase.client.protocol.sdk.kv.Remove) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = RemoveOptions()
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasCas) out = out.cas(opts.getCas)
      out
    } else null
  }

  def createOptions(request: com.couchbase.client.protocol.sdk.kv.Get) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = GetOptions()
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasWithExpiry) out = out.withExpiry(opts.getWithExpiry)
      if (opts.getProjectionCount > 0)
        out = out.project(opts.getProjectionList.asByteStringList().toSeq.map(v => v.toString))
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    } else null
  }

  def createOptions(request: com.couchbase.client.protocol.sdk.kv.Replace) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = ReplaceOptions()
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasExpiry) out = convertExpiry(opts.getExpiry) match {
        case Left(expiry) =>
          // [start:1.1.0]
          out.expiry(expiry)
          // [end:1.1.0]
          // [start:<1.1.0]
/*
          throw new UnsupportedOperationException(
            "This SDK version does not support this form of expiry"
          );
        // [end:<1.1.0]
*/
        case Right(expiry) => out.expiry(expiry)
      }
      if (opts.hasPreserveExpiry) {
        // [start:1.1.5]
        out.preserveExpiry(opts.getPreserveExpiry)
        // [end:1.1.5]
        // [start:<1.1.5]
/*
        throw new UnsupportedOperationException("This SDK version does not support expiry")
        // [end:<1.1.5]
*/
      }
      if (opts.hasCas) out = out.cas(opts.getCas)
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    } else null
  }

  def createOptions(request: com.couchbase.client.protocol.sdk.kv.Upsert) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = UpsertOptions()
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasDurability) out = out.durability(convertDurability(opts.getDurability))
      if (opts.hasExpiry) out = convertExpiry(opts.getExpiry) match {
        case Left(expiry) =>
          // [start:1.1.0]
          out.expiry(expiry)
          // [end:1.1.0]
          // [start:<1.1.0]
/*
          throw new UnsupportedOperationException(
            "This SDK version does not support this form of expiry"
          );
        // [end:<1.1.0]
*/
        case Right(expiry) => out.expiry(expiry)
      }
      if (opts.hasPreserveExpiry) {
        // [start:1.1.5]
        out.preserveExpiry(opts.getPreserveExpiry)
        // [end:1.1.5]
        // [start:<1.1.5]
/*
        throw new UnsupportedOperationException("This SDK version does not support preserve expiry")
        // [end:<1.1.5]
*/
      }
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      out
    } else null
  }

  // [start:1.5.0]
  def createOptions(request: com.couchbase.client.protocol.sdk.kv.rangescan.Scan) = {
    if (request.hasOptions) {
      val opts = request.getOptions
      var out  = ScanOptions()

      if (opts.hasIdsOnly) out = out.idsOnly(opts.getIdsOnly)
      if (opts.hasConsistentWith)
        out = out.consistentWith(convertMutationState(opts.getConsistentWith))
      if (opts.hasTranscoder) out = out.transcoder(convertTranscoder(opts.getTranscoder))
      if (opts.hasTimeoutMsecs)
        out = out.timeout(Duration.create(opts.getTimeoutMsecs, TimeUnit.MILLISECONDS))
      if (opts.hasBatchByteLimit) out = out.batchByteLimit(opts.getBatchByteLimit)
      if (opts.hasBatchItemLimit) out = out.batchItemLimit(opts.getBatchItemLimit)
      if (opts.hasBatchTimeLimit) throw new UnsupportedOperationException("Cannot support batch time limit");
      // Will add when adding support for Caps.OBSERVABILITY_1.
      // if (opts.hasParentSpanId) out = out.parentSpan(spans.get(opts.getParentSpanId))
      out
    } else null
  }
  // [end:1.5.0]

  def convertTranscoder(transcoder: shared.Transcoder): Transcoder = {
    if (transcoder.hasRawJson) RawJsonTranscoder.Instance
    else if (transcoder.hasJson) JsonTranscoder.Instance
    else if (transcoder.hasLegacy) LegacyTranscoder.Instance
    else if (transcoder.hasRawString) RawStringTranscoder.Instance
    else if (transcoder.hasRawBinary) RawBinaryTranscoder.Instance
    else throw new UnsupportedOperationException("Unknown transcoder")
  }

  def setSuccess(result: com.couchbase.client.protocol.run.Result.Builder): Unit = {
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder.setSuccess(true))
  }

  def populateResult(
      result: com.couchbase.client.protocol.run.Result.Builder,
      value: MutationResult
  ): Unit = {
    val builder = com.couchbase.client.protocol.sdk.kv.MutationResult.newBuilder.setCas(value.cas)
    value.mutationToken.foreach(
      mt =>
        builder.setMutationToken(
          com.couchbase.client.protocol.shared.MutationToken.newBuilder
            .setPartitionId(mt.partitionID)
            .setPartitionUuid(mt.partitionUUID)
            .setSequenceNumber(mt.sequenceNumber)
            .setBucketName(mt.bucketName)
        )
    )
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder.setMutationResult(builder))
  }

  def populateResult(
      request: com.couchbase.client.protocol.sdk.kv.Get,
      result: com.couchbase.client.protocol.run.Result.Builder,
      value: GetResult
  ): Unit = {
    val builder = com.couchbase.client.protocol.sdk.kv.GetResult.newBuilder
      .setCas(value.cas)
      .setContent(ContentAsUtil.contentType(request.getContentAs,
          () => value.contentAs[Array[Byte]],
          () => value.contentAs[String],
          () => value.contentAs[JsonObject],
          () => value.contentAs[JsonArray],
          () => value.contentAs[Boolean],
          () => value.contentAs[Int],
          () => value.contentAs[Double]).get)
    // [start:1.1.0]
    value.expiryTime.foreach(et => builder.setExpiryTime(et.getEpochSecond))
    // [end:1.1.0]
    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder.setGetResult(builder))
  }

  def convertMutationState(
      consistentWith: com.couchbase.client.protocol.shared.MutationState
  ): MutationState = {
    val tokens = consistentWith.getTokensList.toSeq
      .map(
        mt =>
          new MutationToken(
            mt.getPartitionId.asInstanceOf[Short],
            mt.getPartitionUuid,
            mt.getSequenceNumber,
            mt.getBucketName
          )
      )

    MutationState(tokens)
  }

  def convertException(raw: Throwable): com.couchbase.client.protocol.shared.Exception = {
    val ret = com.couchbase.client.protocol.shared.Exception.newBuilder

    if (raw.isInstanceOf[CouchbaseException] || raw.isInstanceOf[UnsupportedOperationException]) {
      val typ =
        if (raw.isInstanceOf[UnsupportedOperationException])
          CouchbaseExceptionType.SDK_UNSUPPORTED_OPERATION_EXCEPTION
        else ErrorUtil.convertException(raw.asInstanceOf[CouchbaseException])

      val out = CouchbaseExceptionEx.newBuilder
        .setName(raw.getClass.getSimpleName)
        .setType(typ)
        .setSerialized(raw.toString)
      if (raw.getCause != null) {
        out.setCause(convertException(raw.getCause))
      }
      ret.setCouchbase(out)
    } else
      ret.setOther(
        ExceptionOther.newBuilder.setName(raw.getClass.getSimpleName).setSerialized(raw.toString)
      )

    ret.build
  }

  def convertDuration(duration: scala.concurrent.duration.Duration): com.google.protobuf.Duration = {
      com.google.protobuf.Duration.newBuilder()
              .setSeconds(duration.toSeconds)
              .setNanos((duration.toNanos - TimeUnit.SECONDS.toNanos(duration.toSeconds)).toInt)
              .build
  }

    def waitUntilReadyOptions(request: WaitUntilReadyRequest): WaitUntilReadyOptions = {
        var options = WaitUntilReadyOptions()

        if (request.getOptions.hasDesiredState) {
            options = options.desiredState(ClusterState.valueOf(request.getOptions.getDesiredState.toString))
        }

        if (request.getOptions.getServiceTypesList.size() > 0) {
            val serviceTypes = request.getOptions.getServiceTypesList

            var services: Set[ServiceType] = Set()
            for (service <- serviceTypes) {
                services = services.++(Set(ServiceType.valueOf(service.toString)))
            }
            options = options.serviceTypes(services)
        }

        options
    }
}
