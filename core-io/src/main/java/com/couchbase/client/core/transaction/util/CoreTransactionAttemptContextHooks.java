/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Can be used for testing transactions, but is for internal use only.
 */
@Stability.Internal
public class CoreTransactionAttemptContextHooks {
    public static final String HOOK_ROLLBACK = "rollback";
    public static final String HOOK_GET = "get";
    public static final String HOOK_INSERT = "insert";
    public static final String HOOK_REPLACE = "replace";
    public static final String HOOK_REMOVE = "remove";
    public static final String HOOK_BEFORE_COMMIT = "commit";
    public static final String HOOK_ROLLBACK_DOC = "rollbackDoc";
    public static final String HOOK_DELETE_INSERTED = "deleteInserted";
    public static final String HOOK_REMOVE_STAGED_INSERT = "removeStagedInsert";
    public static final String HOOK_CREATE_STAGED_INSERT = "createdStagedInsert";
    public static final String HOOK_REMOVE_DOC = "removeDoc";
    public static final String HOOK_COMMIT_DOC = "commitDoc";
    public static final String HOOK_COMMIT_DOC_CHANGED = "commitDocChanged";
    public static final String HOOK_STAGING_DOC_CHANGED = "stagingDocChanged";
    public static final String HOOK_ROLLBACK_DOC_CHANGED = "rollbackDocChanged";
    public static final String HOOK_QUERY = "query";
    public static final String HOOK_QUERY_BEGIN_WORK = "queryBeginWork";
    public static final String HOOK_QUERY_COMMIT = "queryCommit";
    public static final String HOOK_QUERY_ROLLBACK = "queryRollback";
    public static final String HOOK_QUERY_KV_GET = "queryKvGet";
    public static final String HOOK_QUERY_KV_REPLACE = "queryKvReplace";
    public static final String HOOK_QUERY_KV_REMOVE = "queryKvRemove";
    public static final String HOOK_QUERY_KV_INSERT = "queryKvInsert";
    public static final String HOOK_BEFORE_RETRY = "beforeRetry";

    public static final String HOOK_ATR_COMMIT = "atrCommit";
    public static final String HOOK_ATR_COMMIT_AMBIGUITY_RESOLUTION = "atrCommitAmbiguityResolution";
    public static final String HOOK_ATR_ABORT = "atrAbort";
    public static final String HOOK_ATR_ROLLBACK_COMPLETE = "atrRollbackComplete";
    public static final String HOOK_ATR_PENDING = "atrPending";
    public static final String HOOK_ATR_COMPLETE = "atrComplete";

    public Mono<Integer> standard = Mono.just(1);
    public Function<CoreTransactionAttemptContext, Mono<Integer>> beforeAtrCommit = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> beforeAtrCommitAmbiguityResolution = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> afterAtrCommit = (ctx) -> standard;
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeDocCommitted = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeRollbackDeleteInserted = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterDocCommittedBeforeSavingCAS = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterDocCommitted = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeStagedInsert = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeStagedRemove = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeStagedReplace = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeDocRemoved = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeDocRolledBack = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterDocRemovedPreRetry = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterDocRemovedPostRetry = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterGetComplete = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterStagedReplaceComplete = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterStagedRemoveComplete = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterStagedInsertComplete = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterRollbackReplaceOrRemove = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterRollbackDeleteInserted = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeCheckATREntryForBlockingDoc = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeDocGet = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeGetDocInExistsDuringStagedInsert = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeOverwritingStagedInsertRemoval = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeRemoveStagedInsert = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterRemoveStagedInsert = (ctx, x) -> Mono.just(1);
    public Function<CoreTransactionAttemptContext, Mono<Integer>> afterDocsCommitted = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> afterDocsRemoved = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> afterAtrPending = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> beforeAtrPending = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> beforeAtrComplete = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> beforeAtrRolledBack = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> afterAtrComplete = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> beforeAtrAborted = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> afterAtrAborted = (ctx) -> standard;
    public Function<CoreTransactionAttemptContext, Mono<Integer>> afterAtrRolledBack = (ctx) -> standard;
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeQuery = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> afterQuery = (ctx, x) -> Mono.just(1);
    public Function<CoreTransactionAttemptContext, Optional<String>> randomAtrIdForVbucket = (ctx) -> Optional.empty();
    public TriFunction<CoreTransactionAttemptContext, String, Optional<String>, Boolean> hasExpiredClientSideHook = (ctx, place, docId) -> false;
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeUnlockGet = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeUnlockInsert = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeUnlockReplace = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeUnlockRemove = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeUnlockQuery = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeDocChangedDuringStaging = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeDocChangedDuringCommit = (ctx, x) -> Mono.just(1);
    public BiFunction<CoreTransactionAttemptContext, String, Mono<Integer>> beforeDocChangedDuringRollback = (ctx, x) -> Mono.just(1);
}
