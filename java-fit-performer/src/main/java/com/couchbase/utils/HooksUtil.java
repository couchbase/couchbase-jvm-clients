/*
 * Copyright (c) 2020 Couchbase, Inc.
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
// [skip:<3.3.0]
package com.couchbase.utils;

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ValueTooLargeException;
import com.couchbase.client.core.error.subdoc.PathExistsException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.core.error.transaction.internal.TestFailAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.TestFailHardException;
import com.couchbase.client.core.error.transaction.internal.TestFailOtherException;
import com.couchbase.client.core.error.transaction.internal.TestFailTransientException;
import com.couchbase.client.core.transaction.ExpiryUtil;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.cleanup.CleanerHooks;
import com.couchbase.client.core.transaction.cleanup.ClientRecord;
import com.couchbase.client.core.transaction.cleanup.ClientRecordFactoryMock;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;
import com.couchbase.client.core.transaction.util.TestTransactionAttemptContextFactory;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.protocol.hooks.transactions.Hook;
import com.couchbase.client.protocol.hooks.transactions.HookAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Utility routines related to configuring transaction hooks.
 */
public class HooksUtil {
    private HooksUtil() {
    }

    static Logger logger = LoggerFactory.getLogger(HooksUtil.class);

    private static Mono<Integer> configureHook(@Nullable final CoreTransactionAttemptContext ctx,
                                               final CallCounts callCount,
                                               final Hook hook,
                                               final Supplier<ClusterConnection> getCluster,
                                               final String param) {
        return configureHookRaw(ctx, callCount, hook, getCluster, param)
                .map(v -> (Integer) v);
    }

    // The slightly awkward Mono<Object> is because we occasionally need the result as a String.
    private static Mono<Object> configureHookRaw(@Nullable final CoreTransactionAttemptContext ctx,
                                                 final CallCounts callCount,
                                                 final Hook hook,
                                                 final Supplier<ClusterConnection> clusterConn,
                                                 final String param) {

        return Mono.defer(() -> {
            Mono<Object> out;
            // This action may or may not be taken, depending on the hook conditionals
            Mono<Object> action;
            switch (hook.getHookAction()) {
                case FAIL_HARD:
                    action = Mono.error(new TestFailHardException());
                    break;
                case FAIL_OTHER:
                    action = Mono.error(new TestFailOtherException());
                    break;
                case FAIL_TRANSIENT:
                    action = Mono.error(new TestFailTransientException());
                    break;
                case FAIL_AMBIGUOUS:
                    action = Mono.error(new TestFailAmbiguousException());
                    break;
                case FAIL_DOC_NOT_FOUND:
                    action = Mono.error(new DocumentNotFoundException(null));
                    break;
                case FAIL_DOC_ALREADY_EXISTS:
                    action = Mono.error(new DocumentExistsException(null));
                    break;
                case FAIL_PATH_ALREADY_EXISTS:
                    action = Mono.error(new PathExistsException(null));
                    break;
                case FAIL_PATH_NOT_FOUND:
                    action = Mono.error(new PathNotFoundException(null));
                    break;
                case FAIL_CAS_MISMATCH:
                    action = Mono.error(new CasMismatchException(null));
                    break;
                case FAIL_ATR_FULL:
                    action = Mono.error(new ValueTooLargeException(null));
                    break;
                case MUTATE_DOC:
                case REMOVE_DOC:
                    // In format "bucket-name/collection-name/doc-id"
                    try {
                        String docLocation = hook.getHookActionParam1();
                        String[] splits = docLocation.split("/");
                        String bucketName = splits[0];
                        String collectionName = splits[1];
                        String docId = splits[2];
                        Collection coll = clusterConn.get().cluster().bucket(bucketName).collection(collectionName);
                        String content = hook.getHookActionParam2();

                        if (hook.getHookAction() == HookAction.MUTATE_DOC) {
                            if (content == null || content.isEmpty()) {
                                throw new InternalPerformerFailure(
                                        new IllegalStateException("No content provided for MUTATE_DOC!"));
                            }

                            action = coll.reactive().upsert(docId, content,
                                    UpsertOptions.upsertOptions().transcoder(RawJsonTranscoder.INSTANCE))
                                    .doOnSubscribe(v -> logger.info("Executing hook to mutate doc {} with" +
                                            " content {}", docId, content))
                                    .thenReturn(0);
                        }
                        else {
                            action = coll.reactive().remove(docId)
                                    .doOnSubscribe(v -> logger.info("Executing hook to remove doc {}", docId))
                                    .thenReturn(0);
                        }
                    } catch (RuntimeException err) {
                        throw new InternalPerformerFailure(err);
                    }

                    break;
                case RETURN_STRING:
                    action = Mono.just(hook.getHookActionParam1());
                    break;
                case BLOCK:
                    Duration blockFor = Duration.ofMillis(Integer.parseInt(hook.getHookActionParam1()));
                    action = Mono.fromRunnable(() -> {
                        ctx.logger().info(ctx.attemptId(), "performer: starting blocking wait of " + hook.getHookActionValue() + "millis");
                    })
                            .then(Mono.delay(blockFor))
                            .thenReturn((Object) 1);
                            // .publishOn(SchedulerUtil.scheduler) // don't use default scheduler (parallel) as triggers a driver validation error
                            // But, not available in older versions, so simply don't publish the message
                            // .doOnNext(v -> ctx.logger().info(ctx.attemptId(), "performer: finished delay"));
                    break;
                default:
                    throw new InternalPerformerFailure(
                            new IllegalStateException("Cannot handle hook action " + hook.getHookAction()));
            }

            switch (hook.getHookCondition()) {
                case ON_CALL:
                    out = Mono.defer(() -> {
                        final int desiredCallNumber = hook.getHookConditionParam1();
                        int callNumber = callCount.getCount(hook.getHookPoint());
                        logger.info("Evaluating whether to execute ON_CALL hook at {}: call count={} desired={}",
                                hook.getHookPoint(), callNumber, desiredCallNumber);
                        if (callNumber == desiredCallNumber) {
                            return action;
                        } else {
                            return Mono.just(1);
                        }
                    });
                    break;

                case ON_CALL_LE:
                    out = Mono.defer(() -> {
                        final int desiredCallNumber = hook.getHookConditionParam1();
                        int callNumber =callCount.getCount(hook.getHookPoint());
                        if (callNumber <= desiredCallNumber) {
                            logger.info("Executing the hook since the condition for ON_CALL_LE  is met: call count={} desired={}",
                                    callNumber, desiredCallNumber);
                            return action;
                        } else {
                            return Mono.just(1);
                        }
                    });
                    break;

                case ON_CALL_GE:
                    out = Mono.defer(() -> {
                        final int desiredCallNumber = hook.getHookConditionParam1();
                        int callNumber =callCount.getCount(hook.getHookPoint());
                        if (callNumber >= desiredCallNumber) {
                            logger.info("Executing the hook since the condition for ON_CALL_GE  is met: call count={} desired={}",
                                    callNumber, desiredCallNumber);
                            return action;
                        } else {
                            return Mono.just(1);
                        }
                    });
                    break;

                case ALWAYS:
                    out = action.doOnSubscribe(v ->
                            logger.info("Executing hook ALWAYS"));
                    break;

                case EQUALS:
                    out = Mono.defer(() -> {
                        final String desiredParam = hook.getHookConditionParam2();
                        logger.info("Evaluating whether to execute EQUALS hook at {}: param={} desired={}",
                                hook.getHookPoint(), param, desiredParam);
                        if (param.equals(desiredParam)) {
                            return action;
                        } else {
                            return Mono.just(1);
                        }
                    });
                    break;

                case ON_CALL_AND_EQUALS:
                    out = Mono.defer(() -> {
                        callCount.add(hook.getHookPoint(), param);

                        final int desiredCallNumber = hook.getHookConditionParam1();
                        final String desiredParam = hook.getHookConditionParam2();
                        int callNumber = callCount.getCount(hook.getHookPoint(), param);
                        logger.info("Evaluating whether to execute ON_CALL_AND_EQUALS hook at {}: call count (for this hook-param pair)={} param={} desired={} {}",
                                hook.getHookPoint(), callNumber, param, desiredCallNumber, desiredParam);
                        if (callNumber == desiredCallNumber && param.equals(desiredParam)) {
                            return action;
                        } else {
                            return Mono.just(1);
                        }
                    });
                    break;

                case WHILE_NOT_EXPIRED: {
                    out = Mono.defer(() -> {
                        boolean hasExpired = ExpiryUtil.hasExpired(ctx, "hook-check", Optional.empty());
                        
                        logger.info("Evaluating whether to execute WHILE_NOT_EXPIRED hook at {}, hasExpired={}", 
                                hook.getHookPoint(), hasExpired);

                        return hasExpired ? Mono.just(1) : action;
                    });
                    break;
                }

                case WHILE_EXPIRED: {
                    out = Mono.defer(() -> {
                        boolean hasExpired = ExpiryUtil.hasExpired(ctx, "hook-check", Optional.empty());

                        logger.info("Evaluating whether to execute WHILE_EXPIRED hook at {}, hasExpired={}",
                                hook.getHookPoint(), hasExpired);

                        return hasExpired ? action : Mono.just(1);
                    });
                    break;
                }

                default:
                    throw new InternalPerformerFailure(
                            new IllegalStateException("Cannot handle hook condition " + hook.getHookCondition()));
            }

            // Make sure callCount gets incremented each time this is called
            return Mono.fromRunnable(() -> callCount.add(hook.getHookPoint()))
                    .then(out);
        });
    }

    private static void setHookIfExists(Hook hook, CoreTransactionAttemptContextHooks mock, String fieldName, BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> toHook) {
        try {
            Field field = mock.getClass().getDeclaredField(fieldName);
            field.set(mock, toHook);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new InternalPerformerFailure(
                    new IllegalArgumentException("Trying to perform a test that requires hook " + hook.getHookPoint() + " on a transaction library that doesn't have the required"));
        }
    }

    /**
     * This is to support backwards compatibility with older transaction libraries that do not have newer hooks.
     */
    private static void setHookIfExists(final CoreTransactionAttemptContextHooks mock,
                                        final Function<CoreTransactionAttemptContext, Mono<Integer>> confHook,
                                        final Hook hook,
                                        final String name) {
        try {
            Field field = mock.getClass().getDeclaredField(name);
            field.set(mock, confHook);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new InternalPerformerFailure(
                    new IllegalArgumentException("Trying to perform a test that requires hook " + hook.getHookPoint() + " on a transaction library that doesn't have the required " + name));
        }
    }

    public static TransactionAttemptContextFactory configureHooks(List<Hook> hooks,
                                                                  Supplier<ClusterConnection> clusterConn) {
        // Should get one callCount per ResumableTransaction, captured by the lambdas below
        final CallCounts callCount = new CallCounts();
        AtomicBoolean hasExpired = new AtomicBoolean(false);

        if (!hooks.isEmpty()) {
            CoreTransactionAttemptContextHooks mock = new CoreTransactionAttemptContextHooks();

            for (int i = 0; i < hooks.size(); i++) {
                Hook hook = hooks.get(i);

                Function<CoreTransactionAttemptContext, Mono<Integer>> confHook =
                        (ctx) -> configureHook(ctx, callCount, hook, clusterConn, null);

                switch (hook.getHookPoint()) {
                    case BEFORE_ATR_COMMIT:
                        mock.beforeAtrCommit = confHook;
                        break;

                    case BEFORE_ATR_COMMIT_AMBIGUITY_RESOLUTION:
                        setHookIfExists(mock, confHook, hook, "beforeAtrCommitAmbiguityResolution");
                        break;

                    case BEFORE_ATR_COMPLETE:
                        logger.info("Inside BEFORE_ATR_COMPLETE");
                        mock.beforeAtrComplete = confHook;
                        break;

                    case AFTER_ATR_COMMIT:
                        mock.afterAtrCommit = confHook;
                        break;

                    case BEFORE_DOC_COMMITTED:
                        mock.beforeDocCommitted = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_DOC_ROLLED_BACK:
                        mock.beforeDocRolledBack = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_DOC_COMMITTED_BEFORE_SAVING_CAS:
                        mock.afterDocCommittedBeforeSavingCAS = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_DOC_COMMITTED:
                        mock.afterDocCommitted = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_DOC_REMOVED:
                        mock.beforeDocRemoved = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_DOC_REMOVED_PRE_RETRY:
                        mock.afterDocRemovedPreRetry = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_DOC_REMOVED_POST_RETRY:
                        mock.afterDocRemovedPostRetry = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_DOCS_REMOVED:
                        mock.afterDocsRemoved = confHook;
                        break;

                    case BEFORE_ATR_PENDING:
                        mock.beforeAtrPending = confHook;
                        break;

                    case AFTER_ATR_PENDING:
                        mock.afterAtrPending = confHook;
                        break;

                    case AFTER_ATR_COMPLETE:
                        mock.afterAtrComplete = confHook;
                        break;

                    case BEFORE_ATR_ROLLED_BACK:
                        mock.beforeAtrRolledBack = confHook;
                        break;

                    case AFTER_GET_COMPLETE:
                        mock.afterGetComplete = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_ROLLBACK_DELETE_INSERTED:
                        mock.beforeRollbackDeleteInserted = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_STAGED_REPLACE_COMPLETE:
                        mock.afterStagedReplaceComplete = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_STAGED_REMOVE_COMPLETE:
                        mock.afterStagedRemoveComplete = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_STAGED_INSERT:
                        mock.beforeStagedInsert = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_STAGED_REMOVE:
                        mock.beforeStagedRemove = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_STAGED_REPLACE:
                        mock.beforeStagedReplace = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_STAGED_INSERT_COMPLETE:
                        mock.afterStagedInsertComplete = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_ATR_ABORTED:
                        mock.beforeAtrAborted = confHook;
                        break;

                    case AFTER_ATR_ABORTED:
                        mock.afterAtrAborted = confHook;
                        break;

                    case AFTER_ATR_ROLLED_BACK:
                        mock.afterAtrRolledBack = confHook;
                        break;

                    case AFTER_ROLLBACK_REPLACE_OR_REMOVE:
                        mock.afterRollbackReplaceOrRemove = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_ROLLBACK_DELETE_INSERTED:
                        mock.afterRollbackDeleteInserted = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_CHECK_ATR_ENTRY_FOR_BLOCKING_DOC:
                        mock.beforeCheckATREntryForBlockingDoc = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_DOC_GET:
                        mock.beforeDocGet = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_GET_DOC_IN_EXISTS_DURING_STAGED_INSERT:
                        mock.beforeGetDocInExistsDuringStagedInsert = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case HAS_EXPIRED:
                        mock.hasExpiredClientSideHook = (ctx, stage, docId) -> {
                            boolean out = false;

                            if (hasExpired.get()) {
                                logger.info("Has already expired on a hook, so returning true for expiry");
                                out = true;
                            }
                            else {
                                switch (hook.getHookCondition()) {
                                    case ALWAYS:
                                        out = true;
                                        break;

                                    case EQUALS_BOTH: {
                                        if (!docId.isPresent()) {
                                            // Cannot perform EQUALS_BOTH if docId not present
                                            out = false;
                                        } else {
                                            out = stage.equals(hook.getHookConditionParam3())
                                                    && docId.get().equals(hook.getHookConditionParam2());

                                            logger.info("Evaluating whether to expire at stage={} (want {}) on id={} (want {}) = {}",
                                                    stage, hook.getHookConditionParam3(), docId.get(), hook.getHookConditionParam2(), out);
                                        }
                                        break;
                                    }
                                    
                                    case EQUALS: {
                                        out = stage.equals(hook.getHookConditionParam2());
                                        if (out) {
                                            logger.info("Injecting expiry at stage={}", stage);
                                        }
                                        break;
                                    }

                                    default:
                                        throw new InternalPerformerFailure(
                                                new IllegalStateException("Cannot handle hook condition " + hook.getHookCondition()));
                                }

                                if (out) {
                                    hasExpired.set(true);
                                }
                            }

                            return out;
                        };
                        break;

                    case ATR_ID_FOR_VBUCKET:
                        mock.randomAtrIdForVbucket = (ctx) -> {
                            Object r = configureHookRaw(ctx, callCount, hook, clusterConn, null).block();
                            return Optional.of((String) r);
                        };
                        break;

                    case BEFORE_QUERY:
                        mock.beforeQuery = (ctx, query) -> configureHook(ctx, callCount, hook, clusterConn, query);
                        break;

                    case AFTER_QUERY:
                        mock.afterQuery = (ctx, query) -> configureHook(ctx, callCount, hook, clusterConn, query);
                        break;

                    case BEFORE_REMOVE_STAGED_INSERT:
                        mock.beforeRemoveStagedInsert = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case AFTER_REMOVE_STAGED_INSERT:
                        mock.afterRemoveStagedInsert = (ctx, id) -> configureHook(ctx, callCount, hook, clusterConn, id);
                        break;

                    case BEFORE_DOC_CHANGED_DURING_COMMIT:
                        setHookIfExists(hook, mock, "beforeDocChangedDuringCommit",
                                (ctx, query) -> configureHook(ctx, callCount, hook, clusterConn, query));
                        break;

                    case BEFORE_DOC_CHANGED_DURING_ROLLBACK:
                        setHookIfExists(hook, mock, "beforeDocChangedDuringRollback",
                                (ctx, query) -> configureHook(ctx, callCount, hook, clusterConn, query));
                        break;

                    case BEFORE_DOC_CHANGED_DURING_STAGING:
                        setHookIfExists(hook, mock, "beforeDocChangedDuringStaging",
                                (ctx, query) -> configureHook(ctx, callCount, hook, clusterConn, query));
                        break;

                    case CLEANUP_BEFORE_ATR_REMOVE:
                    case CLEANUP_BEFORE_COMMIT_DOC:
                    case CLEANUP_BEFORE_DOC_GET:
                    case CLEANUP_BEFORE_REMOVE_DOC:
                    case CLEANUP_BEFORE_REMOVE_DOC_LINKS:
                    case CLEANUP_BEFORE_REMOVE_DOC_STAGED_FOR_REMOVAL:
                        // These will be configured by configureCleanupHooks
                        break;

                    default:
                        throw new InternalPerformerFailure(
                                new IllegalStateException("Cannot handle hook point " + hook.getHookPoint()));
                }
            }

            return new TestTransactionAttemptContextFactory(mock);
        }

        return new TransactionAttemptContextFactory();
    }

    public static CleanerHooks configureCleanupHooks(List<Hook> hooks,
                                                     Supplier<ClusterConnection> clusterConn) {
        final CallCounts callCount = new CallCounts();
        final CleanerHooks mock = new CleanerHooks();

        for (int i = 0; i < hooks.size(); i++) {
            Hook hook = hooks.get(i);

            switch (hook.getHookPoint()) {
                case CLEANUP_BEFORE_COMMIT_DOC:
                    mock.beforeCommitDoc = (id) -> configureHook(null, callCount, hook, clusterConn, id);
                    break;

                case CLEANUP_BEFORE_REMOVE_DOC_STAGED_FOR_REMOVAL:
                    mock.beforeRemoveDocStagedForRemoval = (id) -> configureHook(null, callCount, hook, clusterConn, id);
                    break;

                case CLEANUP_BEFORE_DOC_GET:
                    mock.beforeDocGet = (id) -> configureHook(null, callCount, hook, clusterConn, id);
                    break;

                case CLEANUP_BEFORE_REMOVE_DOC:
                    mock.beforeRemoveDoc = (id) -> configureHook(null, callCount, hook, clusterConn, id);
                    break;

                case CLEANUP_BEFORE_REMOVE_DOC_LINKS:
                    mock.beforeRemoveLinks = (id) -> configureHook(null, callCount, hook, clusterConn, id);
                    break;

                case CLEANUP_BEFORE_ATR_REMOVE:
                    mock.beforeAtrRemove = () -> configureHook(null, callCount, hook, clusterConn, null);
                    break;

                default:
                    break;
            }
        }

        return mock;
    }

    public static ClientRecord configureClientRecordHooks(List<Hook> hooks,
                                                          ClusterConnection clusterConn) {
        final CallCounts callCount = new CallCounts();
        ClientRecordFactoryMock mock = new ClientRecordFactoryMock();

        for (Hook hook : hooks) {
            Supplier<Mono<Integer>> basic = () -> configureHook(null, callCount, hook, () -> clusterConn, null);

            switch (hook.getHookPoint()) {
                case CLIENT_RECORD_BEFORE_CREATE:
                    mock.beforeCreateRecord = basic;
                    break;

                case CLIENT_RECORD_BEFORE_GET:
                    mock.beforeGetRecord = basic;
                    break;

                case CLIENT_RECORD_BEFORE_UPDATE:
                    mock.beforeUpdateRecord = basic;
                    break;

                case CLIENT_RECORD_BEFORE_REMOVE_CLIENT:
                    mock.beforeRemoveClient = basic;
                    break;

                default:
                    throw new InternalPerformerFailure(
                            new IllegalArgumentException("Unknown client record hook " + hook.getHookPoint())
                    );
            }
        }

        return mock.create(clusterConn.core());
    }
}