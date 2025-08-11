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
package com.couchbase.client.performer.scala.transaction

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.error.transaction.internal.TestFailOtherException
import com.couchbase.client.core.transaction.threadlocal.TransactionMarkerOwner
import com.couchbase.client.performer.core.commands.{BatchExecutor, TransactionCommandExecutor}
import com.couchbase.client.performer.scala.Content.{
  ContentByteArray,
  ContentJson,
  ContentNull,
  ContentString
}
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.{
  convertContent,
  convertTranscoder
}
import com.couchbase.client.performer.scala.error.InternalPerformerFailure
import com.couchbase.client.performer.scala.transaction.TransactionShared.ExpectSuccess
import com.couchbase.client.performer.scala.util.{ClusterConnection, OptionsUtil, ResultValidation}
import com.couchbase.client.protocol.shared.API
import com.couchbase.client.protocol.transactions.{
  ExpectedResult,
  TransactionCommand,
  TransactionCreateRequest,
  TransactionStreamPerformerToDriver
}
import com.couchbase.client.scala.codec._
import com.couchbase.client.scala.codec.Conversions._
import com.couchbase.client.scala.json._
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.transactions.TransactionAttemptContext
// [if:3.9.0]
import com.couchbase.client.scala.transactions.getmulti
// [end]
import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.util.Success

object TransactionBlocking {

  /** Starts a transaction that will run until completion.
    */
  def run(
      connection: ClusterConnection,
      req: TransactionCreateRequest,
      executor: Option[TransactionCommandExecutor],
      performanceMode: Boolean,
      spans: collection.Map[String, RequestSpan]
  ): com.couchbase.client.protocol.transactions.TransactionResult = {
    val txn = new TransactionBlocking(executor)
    txn.run(connection, req, None, performanceMode, spans)
  }

  private def getLogger(ctx: TransactionAttemptContext) = {
    ctx.internal.internal.logger()
  }
}
class TransactionBlocking(executor: Option[TransactionCommandExecutor])
    extends TransactionShared(executor) {

  /** Starts a two-way transaction.
    */
  override def runInternal(
      connection: ClusterConnection,
      req: TransactionCreateRequest,
      toTest: Option[StreamObserver[TransactionStreamPerformerToDriver]],
      performanceMode: Boolean,
      spans: collection.Map[String, RequestSpan]
  ): com.couchbase.client.scala.transactions.TransactionResult = {
    val attemptCount = new AtomicInteger(-1)
    if (req.getApi != API.DEFAULT)
      throw new InternalPerformerFailure(new IllegalStateException("Unexpected API"))
    val ptcb = OptionsUtil.makeTransactionOptions(connection, req, spans)
    val out  = connection.cluster.transactions.run(
      (ctx: TransactionAttemptContext) => {
        if (testFailure.get != null) {
          logger.info("Test failure is set at start of new attempt, fast failing transaction")
          throw testFailure.get
        }
        val count = attemptCount.incrementAndGet
        // logger.info("Starting attempt {} {}", count, ctx.attemptId());
        val attemptToUse = Math.min(count, req.getAttemptsCount - 1)
        val attempt      = req.getAttempts(attemptToUse)
        attempt.getCommandsList.asScala.foreach(command => {
          performOperation(connection, ctx, command, toTest, performanceMode, "")
        })
        if (!performanceMode) logger.info("Reached end of all operations and lambda")
        Success()
      },
      // [if:3.9.0]
      ptcb.orNull
      // [end]
      // [if:<3.9.0]
      // format: off
      //? Option(ptcb.orNull)
      // format: on
      // [end]
    )
    if (TransactionMarkerOwner.get.block.isPresent)
      throw new InternalPerformerFailure(
        new IllegalStateException("Still in blocking transaction context after completion")
      )
    out.get
  }

  private def performOperation(
      connection: ClusterConnection,
      ctx: TransactionAttemptContext,
      op: TransactionCommand,
      toTest: Option[StreamObserver[TransactionStreamPerformerToDriver]],
      performanceMode: Boolean,
      dbg: String
  ): Unit = {
    if (op.getWaitMsecs != 0) try {
      logger.info("{} Sleeping for Msecs: {}", dbg, op.getWaitMsecs)
      Thread.sleep(op.getWaitMsecs)
    } catch {
      case e: InterruptedException =>
        throw new InternalPerformerFailure(new RuntimeException(e))
    }

    if (op.hasInsert) {
      val request = op.getInsert
      val content = readContent(
        if (request.hasContentJson) Some(request.getContentJson) else None,
        if (request.hasContent) Some(request.getContent) else None
      )
      val collection = connection.collection(request.getDocId)
      performOperation(
        dbg + "insert " + request.getDocId.getDocId,
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          logger.info(
            "{} Performing insert operation on {} on bucket {} on collection {}",
            dbg,
            request.getDocId.getDocId,
            request.getDocId.getBucketName,
            request.getDocId.getCollectionName
          )
          // [if:1.9.0]
          val options = TransactionOptionsUtil.transactionInsertOptions(request)
          content match {
            case ContentString(value) =>
              options match {
                case Some(opts) =>
                  ctx.insert(collection, request.getDocId.getDocId, value, opts).get
                case None => ctx.insert(collection, request.getDocId.getDocId, value).get
              }
            case ContentJson(value) =>
              options match {
                case Some(opts) =>
                  ctx.insert(collection, request.getDocId.getDocId, value, opts).get
                case None => ctx.insert(collection, request.getDocId.getDocId, value).get
              }
            case ContentByteArray(value) =>
              options match {
                case Some(opts) =>
                  ctx.insert(collection, request.getDocId.getDocId, value, opts).get
                case None => ctx.insert(collection, request.getDocId.getDocId, value).get
              }
            case ContentNull(value) =>
              options match {
                case Some(opts) =>
                  ctx.insert(collection, request.getDocId.getDocId, value, opts).get
                case None => ctx.insert(collection, request.getDocId.getDocId, value).get
              }
          }
          // [end]
          // [if:<1.9.0]
          // format: off
          //? content match {
          //?   case ContentString(value) => ctx.insert(collection, request.getDocId.getDocId, value).get
          //?   case ContentJson(value) => ctx.insert(collection, request.getDocId.getDocId, value).get
          //?   case ContentByteArray(value) => ctx.insert(collection, request.getDocId.getDocId, value).get
          //?   case ContentNull(value) => ctx.insert(collection, request.getDocId.getDocId, value).get
          //? }
          // format: on
          // [end]
        }
      )
    } else if (op.hasInsertV2) {
      val request = op.getInsertV2
      val content = convertContent(request.getContent)
      performOperation(
        dbg + "insert-v2",
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          val collection = connection.collection(request.getLocation)
          val docId      = executor.get.getDocId(request.getLocation)
          content match {
            case ContentString(value)    => ctx.insert(collection, docId, value).get
            case ContentJson(value)      => ctx.insert(collection, docId, value).get
            case ContentByteArray(value) => ctx.insert(collection, docId, value).get
            case ContentNull(value)      => ctx.insert(collection, docId, value).get
          }
        }
      )
    } else if (op.hasReplace) {
      val request = op.getReplace
      val content = readContent(
        if (request.hasContentJson) Some(request.getContentJson) else None,
        if (request.hasContent) Some(request.getContent) else None
      )
      performOperation(
        dbg + "replace " + request.getDocId.getDocId,
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          // [if:1.9.0]
          val options = TransactionOptionsUtil.transactionReplaceOptions(request)
          if (request.getUseStashedResult) {
            content match {
              case ContentString(value) =>
                options match {
                  case Some(opts) => ctx.replace(stashedGet.get, value, opts).get
                  case None       => ctx.replace(stashedGet.get, value).get
                }
              case ContentJson(value) =>
                options match {
                  case Some(opts) => ctx.replace(stashedGet.get, value, opts).get
                  case None       => ctx.replace(stashedGet.get, value).get
                }
              case ContentByteArray(value) =>
                options match {
                  case Some(opts) => ctx.replace(stashedGet.get, value, opts).get
                  case None       => ctx.replace(stashedGet.get, value).get
                }
              case ContentNull(value) =>
                options match {
                  case Some(opts) => ctx.replace(stashedGet.get, value, opts).get
                  case None       => ctx.replace(stashedGet.get, value).get
                }
            }
          } else if (request.hasUseStashedSlot) {
            if (!stashedGetMap.contains(request.getUseStashedSlot))
              throw new IllegalStateException(
                "Do not have a stashed get in slot " + request.getUseStashedSlot
              )
            content match {
              case ContentString(value) =>
                options match {
                  case Some(opts) =>
                    ctx.replace(stashedGetMap(request.getUseStashedSlot), value, opts).get
                  case None => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
                }
              case ContentJson(value) =>
                options match {
                  case Some(opts) =>
                    ctx.replace(stashedGetMap(request.getUseStashedSlot), value, opts).get
                  case None => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
                }
              case ContentByteArray(value) =>
                options match {
                  case Some(opts) =>
                    ctx.replace(stashedGetMap(request.getUseStashedSlot), value, opts).get
                  case None => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
                }
              case ContentNull(value) =>
                options match {
                  case Some(opts) =>
                    ctx.replace(stashedGetMap(request.getUseStashedSlot), value, opts).get
                  case None => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
                }
            }
          } else {
            val collection = connection.collection(request.getDocId)
            logger.info(
              "{} Performing replace operation on docId {} to new content on collection {}",
              dbg,
              request.getDocId.getDocId,
              request.getDocId.getCollectionName
            )
            val r = ctx.get(collection, request.getDocId.getDocId).get
            content match {
              case ContentString(value) =>
                options match {
                  case Some(opts) => ctx.replace(r, value, opts).get
                  case None       => ctx.replace(r, value).get
                }
              case ContentJson(value) =>
                options match {
                  case Some(opts) => ctx.replace(r, value, opts).get
                  case None       => ctx.replace(r, value).get
                }
              case ContentByteArray(value) =>
                options match {
                  case Some(opts) => ctx.replace(r, value, opts).get
                  case None       => ctx.replace(r, value).get
                }
              case ContentNull(value) =>
                options match {
                  case Some(opts) => ctx.replace(r, value, opts).get
                  case None       => ctx.replace(r, value).get
                }
            }
          }
          // [end]
          // [if:<1.9.0]
          // format: off
          //? if (request.getUseStashedResult) {
          //?   content match {
          //?     case ContentString(value) => ctx.replace(stashedGet.get, value).get
          //?     case ContentJson(value) => ctx.replace(stashedGet.get, value).get
          //?     case ContentByteArray(value) => ctx.replace(stashedGet.get, value).get
          //?     case ContentNull(value) => ctx.replace(stashedGet.get, value).get
          //?   }
          //? } else if (request.hasUseStashedSlot) {
          //?   if (!stashedGetMap.contains(request.getUseStashedSlot))
          //?     throw new IllegalStateException(
          //?       "Do not have a stashed get in slot " + request.getUseStashedSlot
          //?     )
          //?   content match {
          //?     case ContentString(value) => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
          //?     case ContentJson(value) => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
          //?     case ContentByteArray(value) => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
          //?     case ContentNull(value) => ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
          //?   }
          //? } else {
          //?   val collection = connection.collection(request.getDocId)
          //?   logger.info(
          //?     "{} Performing replace operation on docId {} to new content on collection {}",
          //?     dbg,
          //?     request.getDocId.getDocId,
          //?     request.getDocId.getCollectionName
          //?   )
          //?   val r = ctx.get(collection, request.getDocId.getDocId).get
          //?   content match {
          //?     case ContentString(value) => ctx.replace(r, value).get
          //?     case ContentJson(value) => ctx.replace(r, value).get
          //?     case ContentByteArray(value) => ctx.replace(r, value).get
          //?     case ContentNull(value) => ctx.replace(r, value).get
          //?   }
          //? }
          // format: on
          // [end]

        }
      )
    } else if (op.hasReplaceV2) {
      val request = op.getReplaceV2
      val content = convertContent(request.getContent)
      performOperation(
        dbg + "replace-v2",
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          if (request.hasUseStashedSlot) {
            if (!stashedGetMap.contains(request.getUseStashedSlot))
              throw new IllegalStateException(
                "Do not have a stashed get in slot " + request.getUseStashedSlot
              )
            content match {
              case ContentString(value) =>
                ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
              case ContentJson(value) =>
                ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
              case ContentByteArray(value) =>
                ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
              case ContentNull(value) =>
                ctx.replace(stashedGetMap(request.getUseStashedSlot), value).get
            }
          } else {
            val collection = connection.collection(request.getLocation)
            val r          =
              ctx.get(collection, executor.get.getDocId(request.getLocation)).get
            content match {
              case ContentString(value)    => ctx.replace(r, value).get
              case ContentJson(value)      => ctx.replace(r, value).get
              case ContentByteArray(value) => ctx.replace(r, value).get
              case ContentNull(value)      => ctx.replace(r, value).get
            }
          }

        }
      )
    } else if (op.hasRemove) {
      val request = op.getRemove
      performOperation(
        dbg + "remove " + request.getDocId.getDocId,
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          if (request.getUseStashedResult) {
            ctx.remove(stashedGet.get).get
          } else if (request.hasUseStashedSlot) {
            if (!stashedGetMap.contains(request.getUseStashedSlot))
              throw new IllegalStateException(
                "Do not have a stashed get in slot " + request.getUseStashedSlot
              )
            val x = ctx.remove(stashedGetMap(request.getUseStashedSlot))
            x.get
          } else {
            val collection = connection.collection(request.getDocId)
            val r          = ctx.get(collection, request.getDocId.getDocId).get
            ctx.remove(r).get
          }

        }
      )
    } else if (op.hasRemoveV2) {
      val request = op.getRemoveV2
      performOperation(
        dbg + "remove-v2",
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          if (request.hasUseStashedSlot) {
            if (!stashedGetMap.contains(request.getUseStashedSlot))
              throw new IllegalStateException(
                "Do not have a stashed get in slot " + request.getUseStashedSlot
              )
            ctx.remove(stashedGetMap(request.getUseStashedSlot)).get
          } else {
            val collection = connection.collection(request.getLocation)
            val r          =
              ctx.get(collection, executor.get.getDocId(request.getLocation)).get
            ctx.remove(r).get
          }

        }
      )
    } else if (op.hasCommit) {}
    else if (op.hasRollback) {
      throw new RuntimeException("Driver has requested app-rollback")
    } else if (op.hasGet) {
      val request    = op.getGet
      val collection = connection.collection(request.getDocId)
      performOperation(
        dbg + "get " + request.getDocId.getDocId,
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          logger.info(
            "{} Performing get operation on {} on bucket {} on collection {}",
            dbg,
            request.getDocId.getDocId,
            request.getDocId.getBucketName,
            request.getDocId.getCollectionName
          )
          // [if:1.9.0]
          val options = TransactionOptionsUtil.transactionGetOptions(request)
          val out     = options match {
            case Some(opts) => ctx.get(collection, request.getDocId.getDocId, opts).get
            case None       => ctx.get(collection, request.getDocId.getDocId).get
          }
          // [end]
          // [if:<1.9.0]
          // format: off
          //? val out = ctx.get(collection, request.getDocId.getDocId).get
          // format: on
          // [end]
          val contentAsValidation =
            if (request.hasContentAsValidation) Some(request.getContentAsValidation) else None
          handleGetResult(request, out, connection, contentAsValidation)

        }
      )
    } else if (op.hasGetV2) {
      val request = op.getGetV2
      performOperation(
        dbg + "get-v2",
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          val collection = connection.collection(request.getLocation)
          // [if:1.9.0]
          val options = TransactionOptionsUtil.transactionGetOptions(request)
          options match {
            case Some(opts) =>
              ctx.get(collection, executor.get.getDocId(request.getLocation), opts).get
            case None => ctx.get(collection, executor.get.getDocId(request.getLocation)).get
          }
          // [end]
          // [if:<1.9.0]
          // format: off
          //? ctx.get(collection, executor.get.getDocId(request.getLocation)).get
          // format: on
          // [end]
        }
      )
    } else if (op.hasGetOptional) {
      val req        = op.getGetOptional
      val request    = req.getGet
      val collection = connection.collection(request.getDocId)
      performOperation(
        dbg + "get optional " + request.getDocId.getDocId,
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          logger.info(
            "{} Performing getOptional operation on {} on bucket {} on collection {} ",
            dbg,
            request.getDocId.getDocId,
            request.getDocId.getBucketName,
            request.getDocId.getCollectionName
          )
          // [if:1.9.0]
          val options = TransactionOptionsUtil.transactionGetOptions(request)
          val out     = options match {
            case Some(opts) => ctx.get(collection, request.getDocId.getDocId, opts)
            case None       => ctx.get(collection, request.getDocId.getDocId)
          }
          // [end]
          // [if:<1.9.0]
          // format: off
          //? val out = ctx.get(collection, request.getDocId.getDocId)
          // format: on
          // [end]
          val contentAsValidation =
            if (request.hasContentAsValidation) Some(request.getContentAsValidation) else None
          handleGetOptionalResult(request, req, out, connection, contentAsValidation)
        }
      )
      // [start:1.8.0]
    } else if (op.hasGetFromPreferredServerGroup) {
      val request = op.getGetFromPreferredServerGroup
      performOperation(
        dbg + "get-from-preferred-server-group",
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          val collection = connection.collection(request.getDocId)
          // [if:1.9.0]
          val options =
            TransactionOptionsUtil.transactionGetReplicaFromPreferredServerGroupOptions(request)
          val result = options match {
            case Some(opts) =>
              ctx
                .getReplicaFromPreferredServerGroup(collection, request.getDocId.getDocId, opts)
                .get
            case None =>
              ctx.getReplicaFromPreferredServerGroup(collection, request.getDocId.getDocId).get
          }
          // [end]
          // [if:<1.9.0]
          // format: off
          //? val result = ctx.getReplicaFromPreferredServerGroup(collection, request.getDocId.getDocId).get
          // format: on
          // [end]
          handleGetReplicaFromPreferredServerGroupResult(request, result, connection)
        }
      )
      // [end:1.8.0]
    } else if (op.hasWaitOnLatch) {
      val request   = op.getWaitOnLatch
      val latchName = request.getLatchName
      performOperation(
        dbg + "wait on latch " + latchName,
        ctx,
        Seq(ExpectSuccess),
        op.getDoNotPropagateError,
        performanceMode,
        () => handleWaitOnLatch(request, TransactionBlocking.getLogger(ctx))
      )
    } else if (op.hasSetLatch) {
      val request   = op.getSetLatch
      val latchName = request.getLatchName
      performOperation(
        dbg + "set latch " + latchName,
        ctx,
        Seq(ExpectSuccess),
        op.getDoNotPropagateError,
        performanceMode,
        () =>
          handleSetLatch(
            request,
            toTest.get,
            TransactionBlocking.getLogger(ctx)
          )
      )
    } else if (op.hasParallelize) {
      val request = op.getParallelize
      BatchExecutor.performCommandBatchBlocking(
        logger,
        request,
        (c) =>
          performOperation(
            connection,
            ctx,
            c.command,
            toTest,
            performanceMode,
            "parallel" + c.threadIdx + " "
          )
      )
    } else if (op.hasInsertRegularKv) {
      val request    = op.getInsertRegularKv
      val collection =
        connection.collection(request.getDocId)
      performOperation(
        dbg + "KV insert " + request.getDocId.getDocId,
        ctx,
        Seq(ExpectSuccess),
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          val content =
            JsonObject.fromJson(request.getContentJson)
          collection.insert(request.getDocId.getDocId, content)

        }
      )
    } else if (op.hasReplaceRegularKv) {
      val request    = op.getReplaceRegularKv
      val collection =
        connection.collection(request.getDocId)
      performOperation(
        dbg + "KV replace " + request.getDocId.getDocId,
        ctx,
        Seq(ExpectSuccess),
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          val content =
            JsonObject.fromJson(request.getContentJson)
          collection.replace(request.getDocId.getDocId, content)

        }
      )
    } else if (op.hasRemoveRegularKv) {
      val request    = op.getRemoveRegularKv
      val collection =
        connection.collection(request.getDocId)
      performOperation(
        dbg + "KV remove " + request.getDocId.getDocId,
        ctx,
        Seq(ExpectSuccess),
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          collection.remove(request.getDocId.getDocId)

        }
      )
    } else if (op.hasThrowException) {
      TransactionBlocking.getLogger(ctx).info("Test throwing a TestFailOther exception here")
      logger.info("Throwing exception")
      throw new TestFailOtherException
    } else if (op.hasQuery) {
      val request = op.getQuery
      performOperation(
        dbg + "Query " + request.getStatement,
        ctx,
        request.getExpectedResultList.asScala,
        op.getDoNotPropagateError,
        performanceMode,
        () => {
          val queryOptions =
            OptionsUtil.transactionQueryOptions(request)
          val qr = if (request.hasScope) {
            val bucketName = request.getScope.getBucketName
            val scopeName  = request.getScope.getScopeName
            val scope      = connection.cluster.bucket(bucketName).scope(scopeName)
            queryOptions match {
              case Some(opts) => ctx.query(scope, request.getStatement, opts)
              case None       => ctx.query(scope, request.getStatement)
            }
          } else {
            queryOptions match {
              case Some(opts) => ctx.query(request.getStatement, opts)
              case None       => ctx.query(request.getStatement)
            }
          }
          ResultValidation.validateQueryResult(request, qr.get)
        }
      )
      // [if:3.9.0]
    } else if (op.hasGetMulti) {
      val request = op.getGetMulti
      if (!request.getGetMultiReplicasFromPreferredServerGroup) {
        val specs = request.getSpecsList.asScala.map { spec =>
          val collection = connection.collection(spec.getLocation)
          val docId      = executor.get.getDocId(spec.getLocation)
          val created    = getmulti.TransactionGetMultiSpec.get(collection, docId)
          if (spec.hasTranscoder) {
            created.transcoder(ScalaSdkCommandExecutor.convertTranscoder(spec.getTranscoder))
          } else {
            created
          }
        }.toSeq

        performOperation(
          dbg + "getMulti",
          ctx,
          Seq.empty,
          op.getDoNotPropagateError,
          performanceMode,
          () => {
            val results = if (request.hasOptions) {
              val options = GetMultiHelper.convertToGetMulti(request.getOptions)
              ctx.getMulti(specs, options).get
            } else {
              ctx.getMulti(specs).get
            }
            GetMultiHelper.handleGetMultiResult(request, results)
          }
        )
      } else {
        val specs = request.getSpecsList.asScala.map { spec =>
          val collection = connection.collection(spec.getLocation)
          val docId      = executor.get.getDocId(spec.getLocation)
          val created    =
            getmulti.TransactionGetMultiReplicasFromPreferredServerGroupSpec.get(collection, docId)
          if (spec.hasTranscoder) {
            created.transcoder(ScalaSdkCommandExecutor.convertTranscoder(spec.getTranscoder))
          } else {
            created
          }
        }.toSeq

        performOperation(
          dbg + "getMultiReplicasFromPreferredServerGroup",
          ctx,
          Seq.empty,
          op.getDoNotPropagateError,
          performanceMode,
          () => {
            val results = if (request.hasOptions) {
              val options = GetMultiHelper.convertToGetMultiReplicas(request.getOptions)
              ctx.getMultiReplicasFromPreferredServerGroup(specs, options).get
            } else {
              ctx.getMultiReplicasFromPreferredServerGroup(specs).get
            }
            GetMultiHelper.handleGetMultiFromPreferredServerGroupResult(
              request,
              results,
              TransactionBlocking.getLogger(ctx)
            )
          }
        )
      }
      // [end]
    } else if (op.hasTestFail) {
      val msg   = "Should not reach here"
      val error = new InternalPerformerFailure(new IllegalStateException(msg))
      // Make absolutely certain the test fails
      testFailure.set(error)
      throw error
    } else
      throw new InternalPerformerFailure(
        new IllegalArgumentException("Unknown operation")
      )
  }
  private def performOperation(
      opDebug: String,
      ctx: TransactionAttemptContext,
      expectedResults: Seq[ExpectedResult],
      doNotPropagateError: Boolean,
      performanceMode: Boolean,
      op: Runnable
  ): Unit = {
    try {
      val now = System.currentTimeMillis
      if (!performanceMode) logger.info("Running command '{}'", opDebug)
      op.run()
      if (!performanceMode)
        logger.info("Took {} millis to run command '{}'", System.currentTimeMillis - now, opDebug)
      handleIfResultSucceededWhenShouldNot(
        opDebug,
        () => TransactionBlocking.getLogger(ctx),
        expectedResults
      )
    } catch {
      case err: RuntimeException =>
        handleOperationError(
          opDebug,
          () => dump(TransactionBlocking.getLogger(ctx)),
          expectedResults,
          doNotPropagateError,
          err
        )
    }
  }
}
