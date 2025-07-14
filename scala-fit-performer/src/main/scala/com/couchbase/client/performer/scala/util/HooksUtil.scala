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
package com.couchbase.client.performer.scala.util

import com.couchbase.client.core.error.subdoc.{PathExistsException, PathNotFoundException}
import com.couchbase.client.core.error.transaction.internal.{
  TestFailAmbiguousException,
  TestFailHardException,
  TestFailOtherException,
  TestFailTransientException
}
import com.couchbase.client.core.error.{
  CasMismatchException,
  DocumentExistsException,
  DocumentNotFoundException,
  ValueTooLargeException
}
import com.couchbase.client.core.transaction.cleanup.{
  CleanerHooks,
  ClientRecord,
  ClientRecordFactoryMock
}
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory
import com.couchbase.client.core.transaction.util.{
  CoreTransactionAttemptContextHooks,
  TestTransactionAttemptContextFactory
}
import com.couchbase.client.core.transaction.{CoreTransactionAttemptContext, ExpiryUtil}
import com.couchbase.client.performer.scala.error.InternalPerformerFailure
import com.couchbase.client.protocol.hooks.transactions.{Hook, HookAction, HookCondition, HookPoint}
import com.couchbase.client.scala.codec.RawJsonTranscoder
import com.couchbase.client.scala.kv.UpsertOptions
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

import java.time.Duration
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.{BiFunction, Function, Supplier}
import javax.annotation.Nullable

/** Utility routines related to configuring transaction hooks.
  */
object HooksUtil {
  private val Logger = LoggerFactory.getLogger(this.getClass)

  private def configureHook(
      @Nullable ctx: CoreTransactionAttemptContext,
      callCount: CallCounts,
      hook: Hook,
      getCluster: () => ClusterConnection,
      param: String
  ): Mono[Any] =
    configureHookRaw(ctx, callCount, hook, getCluster, param).map((v) => v.asInstanceOf[Integer])

  // The slightly awkward Mono<Object> is because we occasionally need the result as a String.
  private def configureHookRaw(
      @Nullable ctx: CoreTransactionAttemptContext,
      callCount: CallCounts,
      hook: Hook,
      clusterConn: () => ClusterConnection,
      param: String
  ): Mono[Any] =
    Mono.defer(() => {
      // This action may or may not be taken, depending on the hook conditionals
      val action: Mono[Any] = hook.getHookAction match {
        case HookAction.FAIL_HARD =>
          Mono.error(new TestFailHardException)

        case HookAction.FAIL_OTHER =>
          Mono.error(new TestFailOtherException)

        case HookAction.FAIL_TRANSIENT =>
          Mono.error(new TestFailTransientException)

        case HookAction.FAIL_AMBIGUOUS =>
          Mono.error(new TestFailAmbiguousException)

        case HookAction.FAIL_DOC_NOT_FOUND =>
          Mono.error(new DocumentNotFoundException(null))

        case HookAction.FAIL_DOC_ALREADY_EXISTS =>
          Mono.error(new DocumentExistsException(null))

        case HookAction.FAIL_PATH_ALREADY_EXISTS =>
          Mono.error(new PathExistsException(null))

        case HookAction.FAIL_PATH_NOT_FOUND =>
          Mono.error(new PathNotFoundException(null))

        case HookAction.FAIL_CAS_MISMATCH =>
          Mono.error(new CasMismatchException(null))

        case HookAction.FAIL_ATR_FULL =>
          Mono.error(new ValueTooLargeException(null))

        case HookAction.MUTATE_DOC | HookAction.REMOVE_DOC =>
          // In format "bucket-name/collection-name/doc-id"
          try {
            val docLocation    = hook.getHookActionParam1
            val splits         = docLocation.split("/")
            val bucketName     = splits(0)
            val collectionName = splits(1)
            val docId          = splits(2)
            val coll           =
              clusterConn().cluster.bucket(bucketName).collection(collectionName)
            val content = hook.getHookActionParam2
            if (hook.getHookAction eq HookAction.MUTATE_DOC) {
              if (content == null || content.isEmpty)
                throw new InternalPerformerFailure(
                  new IllegalStateException("No content provided for MUTATE_DOC!")
                )
              coll.reactive
                .upsert(
                  docId,
                  content,
                  UpsertOptions().transcoder(RawJsonTranscoder.Instance)
                )
                .asJava()
                .thenReturn(0)
            } else
              coll.reactive
                .remove(docId)
                .doOnSubscribe((v) => Logger.info("Executing hook to remove doc {}", docId))
                .asJava()
                .`then`(Mono.just(0))
          } catch {
            case err: RuntimeException =>
              throw new InternalPerformerFailure(err)
          }

        case HookAction.RETURN_STRING =>
          Mono.just(hook.getHookActionParam1)

        case HookAction.BLOCK =>
          val blockFor = Duration.ofMillis(hook.getHookActionParam1.toInt)
          Mono
            .fromRunnable(() => {
              ctx.logger.info(
                ctx.attemptId,
                "performer: starting blocking wait of " + hook.getHookActionValue + "millis"
              )

            })
            .`then`(Mono.delay(blockFor))
            .thenReturn(1)

        case _ =>
          throw new InternalPerformerFailure(
            new IllegalStateException("Cannot handle hook action " + hook.getHookAction)
          )
      }
      val out: Mono[Any] = hook.getHookCondition match {
        case HookCondition.ON_CALL =>
          Mono.defer(() => {
            val desiredCallNumber = hook.getHookConditionParam1
            val callNumber        = callCount.getCount(hook.getHookPoint)
            val r: Mono[Any]      = if (callNumber == desiredCallNumber) action else Mono.just(1)
            r
          })

        case HookCondition.ON_CALL_LE =>
          Mono.defer(() => {
            val desiredCallNumber = hook.getHookConditionParam1
            val callNumber        = callCount.getCount(hook.getHookPoint)
            println(s"ON_CALL_LE ${desiredCallNumber} ${callNumber}")
            val r: Mono[Any] = if (callNumber <= desiredCallNumber) {
              action
            } else Mono.just(1)
            r
          })

        case HookCondition.ON_CALL_GE =>
          Mono.defer(() => {
            val desiredCallNumber = hook.getHookConditionParam1
            val callNumber        = callCount.getCount(hook.getHookPoint)
            val r: Mono[Any]      = if (callNumber >= desiredCallNumber) {
              Logger.info(
                "Executing the hook since the condition for ON_CALL_GE  is met: call count={} desired={}",
                callNumber,
                desiredCallNumber
              )
              action
            } else Mono.just(1)
            r
          })

        case HookCondition.ALWAYS =>
          action.doOnSubscribe((v) => Logger.info("Executing hook ALWAYS"))

        case HookCondition.EQUALS =>
          Mono.defer(() => {
            val desiredParam = hook.getHookConditionParam2
            Logger.info(
              "Evaluating whether to execute EQUALS hook at {}: param={} desired={}",
              hook.getHookPoint,
              param,
              desiredParam
            )
            val r: Mono[Any] = if (param == desiredParam) action else Mono.just(1)
            r
          })

        case HookCondition.ON_CALL_AND_EQUALS =>
          Mono.defer(() => {
            callCount.add(hook.getHookPoint, param)
            val desiredCallNumber = hook.getHookConditionParam1
            val desiredParam      = hook.getHookConditionParam2
            val callNumber        = callCount.getCount(hook.getHookPoint, param)
            val r: Mono[Any]      =
              if (callNumber == desiredCallNumber && param == desiredParam) action else Mono.just(1)
            r
          })

        case HookCondition.WHILE_NOT_EXPIRED =>
          Mono.defer(() => {
            val hasExpired   = ExpiryUtil.hasExpired(ctx, "hook-check", Optional.empty[String])
            val r: Mono[Any] = if (hasExpired) Mono.just(1) else action
            r
          })

        case HookCondition.WHILE_EXPIRED =>
          Mono.defer(() => {
            val hasExpired   = ExpiryUtil.hasExpired(ctx, "hook-check", Optional.empty[String])
            val r: Mono[Any] = if (hasExpired) action else Mono.just(1)
            r
          })

        case _ =>
          throw new InternalPerformerFailure(
            new IllegalStateException("Cannot handle hook condition " + hook.getHookCondition)
          )
      }
      // Make sure callCount gets incremented each time this is called
      Mono.fromRunnable(() => callCount.add(hook.getHookPoint)).`then`(out)
    })

  private def setHookIfExists(
      hook: Hook,
      mock: CoreTransactionAttemptContextHooks,
      fieldName: String,
      toHook: (CoreTransactionAttemptContext, String) => Mono[Any]
  ): Unit = {
    try {
      val field = mock.getClass.getDeclaredField(fieldName)
      field.set(mock, scalaFunctoJava(toHook))
    } catch {
      case e @ (_: NoSuchFieldException | _: IllegalAccessException) =>
        throw new InternalPerformerFailure(
          new IllegalArgumentException(
            "Trying to perform a test that requires hook " + hook.getHookPoint + " on a transaction library that doesn't have the required"
          )
        )
    }
  }

  /** This is to support backwards compatibility with older transaction libraries that do not have newer hooks.
    */
  private def setHookIfExists(
      mock: CoreTransactionAttemptContextHooks,
      confHook: Function[CoreTransactionAttemptContext, Mono[Integer]],
      hook: Hook,
      name: String
  ): Unit = {
    try {
      val field = mock.getClass.getDeclaredField(name)
      field.set(mock, confHook)
    } catch {
      case e @ (_: NoSuchFieldException | _: IllegalAccessException) =>
        throw new InternalPerformerFailure(
          new IllegalArgumentException(
            "Trying to perform a test that requires hook " + hook.getHookPoint + " on a transaction library that doesn't have the required " + name
          )
        )
    }
  }

  def scalaFunctoJava[T](
      function: (CoreTransactionAttemptContext, T) => Mono[Any]
  ): BiFunction[CoreTransactionAttemptContext, T, Mono[Integer]] = {
    new BiFunction[CoreTransactionAttemptContext, T, Mono[Integer]] {
      override def apply(t: CoreTransactionAttemptContext, u: T): Mono[Integer] = {
        val out = function.apply(t, u)
        out.thenReturn(0)
      }
    }
  }

  def scalaFunctoJava(
      function: (CoreTransactionAttemptContext) => Mono[Any]
  ): Function[CoreTransactionAttemptContext, Mono[Integer]] = {
    new Function[CoreTransactionAttemptContext, Mono[Integer]] {
      override def apply(t: CoreTransactionAttemptContext): Mono[Integer] = {
        val out = function.apply(t)
        out.thenReturn(0)
      }
    }
  }

  def scalaFunctoJava2(function: (String) => Mono[Any]): Function[String, Mono[Integer]] = {
    new Function[String, Mono[Integer]] {
      override def apply(t: String): Mono[Integer] = {
        val out = function.apply(t)
        out.thenReturn(0)
      }
    }
  }

  def configureHooks(
      hooks: Seq[Hook],
      clusterConn: () => ClusterConnection
  ): TransactionAttemptContextFactory = {
    // Should get one callCount per ResumableTransaction, captured by the lambdas below
    val callCount  = new CallCounts
    val hasExpired = new AtomicBoolean(false)
    if (!hooks.isEmpty) {
      val mock = new CoreTransactionAttemptContextHooks
      for (i <- 0 until hooks.size) {
        val hook     = hooks(i)
        val confHook = scalaFunctoJava((ctx: CoreTransactionAttemptContext) =>
          configureHook(ctx, callCount, hook, clusterConn, null)
        )
        hook.getHookPoint match {
          case HookPoint.BEFORE_ATR_COMMIT =>
            mock.beforeAtrCommit = confHook

          case HookPoint.BEFORE_ATR_COMMIT_AMBIGUITY_RESOLUTION =>
            setHookIfExists(mock, confHook, hook, "beforeAtrCommitAmbiguityResolution")

          case HookPoint.BEFORE_ATR_COMPLETE =>
            Logger.info("Inside BEFORE_ATR_COMPLETE")
            mock.beforeAtrComplete = confHook

          case HookPoint.AFTER_ATR_COMMIT =>
            mock.afterAtrCommit = confHook

          case HookPoint.BEFORE_DOC_COMMITTED =>
            mock.beforeDocCommitted =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_DOC_ROLLED_BACK =>
            mock.beforeDocRolledBack =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_DOC_COMMITTED_BEFORE_SAVING_CAS =>
            mock.afterDocCommittedBeforeSavingCAS =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_DOC_COMMITTED =>
            mock.afterDocCommitted =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_DOC_REMOVED =>
            mock.beforeDocRemoved =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_DOC_REMOVED_PRE_RETRY =>
            mock.afterDocRemovedPreRetry =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_DOC_REMOVED_POST_RETRY =>
            mock.afterDocRemovedPostRetry =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_DOCS_REMOVED =>
            mock.afterDocsRemoved = confHook

          case HookPoint.BEFORE_ATR_PENDING =>
            mock.beforeAtrPending = confHook

          case HookPoint.AFTER_ATR_PENDING =>
            mock.afterAtrPending = confHook

          case HookPoint.AFTER_ATR_COMPLETE =>
            mock.afterAtrComplete = confHook

          case HookPoint.BEFORE_ATR_ROLLED_BACK =>
            mock.beforeAtrRolledBack = confHook

          case HookPoint.AFTER_GET_COMPLETE =>
            mock.afterGetComplete =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_ROLLBACK_DELETE_INSERTED =>
            mock.beforeRollbackDeleteInserted =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_STAGED_REPLACE_COMPLETE =>
            mock.afterStagedReplaceComplete =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_STAGED_REMOVE_COMPLETE =>
            mock.afterStagedRemoveComplete =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_STAGED_INSERT =>
            mock.beforeStagedInsert =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_STAGED_REMOVE =>
            mock.beforeStagedRemove =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_STAGED_REPLACE =>
            mock.beforeStagedReplace =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_STAGED_INSERT_COMPLETE =>
            mock.afterStagedInsertComplete =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_ATR_ABORTED =>
            mock.beforeAtrAborted = confHook

          case HookPoint.AFTER_ATR_ABORTED =>
            mock.afterAtrAborted = confHook

          case HookPoint.AFTER_ATR_ROLLED_BACK =>
            mock.afterAtrRolledBack = confHook

          case HookPoint.AFTER_ROLLBACK_REPLACE_OR_REMOVE =>
            mock.afterRollbackReplaceOrRemove =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_ROLLBACK_DELETE_INSERTED =>
            mock.afterRollbackDeleteInserted =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_CHECK_ATR_ENTRY_FOR_BLOCKING_DOC =>
            mock.beforeCheckATREntryForBlockingDoc =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_DOC_GET =>
            mock.beforeDocGet = scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
              configureHook(ctx, callCount, hook, clusterConn, id)
            )

          case HookPoint.BEFORE_GET_DOC_IN_EXISTS_DURING_STAGED_INSERT =>
            mock.beforeGetDocInExistsDuringStagedInsert =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.HAS_EXPIRED =>
            mock.hasExpiredClientSideHook =
              (ctx: CoreTransactionAttemptContext, stage: String, docId: Optional[String]) => {
                var out = false
                if (hasExpired.get) {
                  Logger.info("Has already expired on a hook, so returning true for expiry")
                  out = true
                } else {
                  hook.getHookCondition match {
                    case HookCondition.ALWAYS =>
                      out = true

                    case HookCondition.EQUALS_BOTH =>
                      if (!docId.isPresent) {
                        // Cannot perform EQUALS_BOTH if docId not present
                        out = false
                      } else {
                        out =
                          stage == hook.getHookConditionParam3 && docId.get == hook.getHookConditionParam2
                      }

                    case HookCondition.EQUALS =>
                      out = stage == hook.getHookConditionParam2
                      if (out) Logger.info("Injecting expiry at stage={}", stage)

                    case _ =>
                      throw new InternalPerformerFailure(
                        new IllegalStateException(
                          "Cannot handle hook condition " + hook.getHookCondition
                        )
                      )
                  }
                  if (out) hasExpired.set(true)
                }
                out

              }

          case HookPoint.ATR_ID_FOR_VBUCKET =>
            mock.randomAtrIdForVbucket = (ctx: CoreTransactionAttemptContext) => {
              val r = configureHookRaw(ctx, callCount, hook, clusterConn, null).block
              Optional.of(r.asInstanceOf[String])

            }

          case HookPoint.BEFORE_QUERY =>
            mock.beforeQuery =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, query: String) =>
                configureHook(ctx, callCount, hook, clusterConn, query)
              )

          case HookPoint.AFTER_QUERY =>
            mock.afterQuery = scalaFunctoJava((ctx: CoreTransactionAttemptContext, query: String) =>
              configureHook(ctx, callCount, hook, clusterConn, query)
            )

          case HookPoint.BEFORE_REMOVE_STAGED_INSERT =>
            mock.beforeRemoveStagedInsert =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.AFTER_REMOVE_STAGED_INSERT =>
            mock.afterRemoveStagedInsert =
              scalaFunctoJava((ctx: CoreTransactionAttemptContext, id: String) =>
                configureHook(ctx, callCount, hook, clusterConn, id)
              )

          case HookPoint.BEFORE_DOC_CHANGED_DURING_COMMIT =>
            setHookIfExists(
              hook,
              mock,
              "beforeDocChangedDuringCommit",
              (ctx: CoreTransactionAttemptContext, query: String) =>
                configureHook(ctx, callCount, hook, clusterConn, query)
            )

          case HookPoint.BEFORE_DOC_CHANGED_DURING_ROLLBACK =>
            setHookIfExists(
              hook,
              mock,
              "beforeDocChangedDuringRollback",
              (ctx: CoreTransactionAttemptContext, query: String) =>
                configureHook(ctx, callCount, hook, clusterConn, query)
            )

          case HookPoint.BEFORE_DOC_CHANGED_DURING_STAGING =>
            setHookIfExists(
              hook,
              mock,
              "beforeDocChangedDuringStaging",
              (ctx: CoreTransactionAttemptContext, query: String) =>
                configureHook(ctx, callCount, hook, clusterConn, query)
            )

          case HookPoint.CLEANUP_BEFORE_ATR_REMOVE                    =>
          case HookPoint.CLEANUP_BEFORE_COMMIT_DOC                    =>
          case HookPoint.CLEANUP_BEFORE_DOC_GET                       =>
          case HookPoint.CLEANUP_BEFORE_REMOVE_DOC                    =>
          case HookPoint.CLEANUP_BEFORE_REMOVE_DOC_LINKS              =>
          case HookPoint.CLEANUP_BEFORE_REMOVE_DOC_STAGED_FOR_REMOVAL =>
          // These will be configured by configureCleanupHooks

          case _ =>
            throw new InternalPerformerFailure(
              new IllegalStateException("Cannot handle hook point " + hook.getHookPoint)
            )
        }
      }
      return new TestTransactionAttemptContextFactory(mock)
    }
    new TransactionAttemptContextFactory
  }
  def configureCleanupHooks(
      hooks: collection.Seq[Hook],
      clusterConn: () => ClusterConnection
  ): CleanerHooks = {
    val callCount = new CallCounts
    val mock      = new CleanerHooks
    for (i <- hooks.indices) {
      val hook = hooks(i)
      hook.getHookPoint match {
        case HookPoint.CLEANUP_BEFORE_COMMIT_DOC =>
          mock.beforeCommitDoc =
            scalaFunctoJava2((id: String) => configureHook(null, callCount, hook, clusterConn, id))

        case HookPoint.CLEANUP_BEFORE_REMOVE_DOC_STAGED_FOR_REMOVAL =>
          mock.beforeRemoveDocStagedForRemoval =
            scalaFunctoJava2((id: String) => configureHook(null, callCount, hook, clusterConn, id))

        case HookPoint.CLEANUP_BEFORE_DOC_GET =>
          mock.beforeDocGet =
            scalaFunctoJava2((id: String) => configureHook(null, callCount, hook, clusterConn, id))

        case HookPoint.CLEANUP_BEFORE_REMOVE_DOC =>
          mock.beforeRemoveDoc =
            scalaFunctoJava2((id: String) => configureHook(null, callCount, hook, clusterConn, id))

        case HookPoint.CLEANUP_BEFORE_REMOVE_DOC_LINKS =>
          mock.beforeRemoveLinks =
            scalaFunctoJava2((id: String) => configureHook(null, callCount, hook, clusterConn, id))

        case HookPoint.CLEANUP_BEFORE_ATR_REMOVE =>
          mock.beforeAtrRemove = new Supplier[Mono[Integer]] {
            override def get(): Mono[Integer] = {
              val out = configureHook(null, callCount, hook, clusterConn, null)
              out.thenReturn(0)
            }
          }

        case _ =>
      }
    }
    mock
  }
  def configureClientRecordHooks(
      hooks: collection.Seq[Hook],
      clusterConn: ClusterConnection
  ): ClientRecord = {
    val callCount = new CallCounts
    val mock      = new ClientRecordFactoryMock
    for (hook <- hooks) {
      val basic = new Supplier[Mono[Integer]]() {
        override def get(): Mono[Integer] = {
          val out = configureHook(null, callCount, hook, () => clusterConn, null)
          out.thenReturn(0)
        }
      }
      hook.getHookPoint match {
        case HookPoint.CLIENT_RECORD_BEFORE_CREATE =>
          mock.beforeCreateRecord = basic

        case HookPoint.CLIENT_RECORD_BEFORE_GET =>
          mock.beforeGetRecord = basic

        case HookPoint.CLIENT_RECORD_BEFORE_UPDATE =>
          mock.beforeUpdateRecord = basic

        case HookPoint.CLIENT_RECORD_BEFORE_REMOVE_CLIENT =>
          mock.beforeRemoveClient = basic

        case _ =>
          throw new InternalPerformerFailure(
            new IllegalArgumentException("Unknown client record hook " + hook.getHookPoint)
          )
      }
    }
    mock.create(clusterConn.cluster.async.core)
  }
}
