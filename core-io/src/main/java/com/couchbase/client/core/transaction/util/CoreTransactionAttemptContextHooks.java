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

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
    public static final String HOOK_GET_MULTI = "getMulti";
    public static final String HOOK_GET_MULTI_INDIVIDUAL_DOCUMENT = "getMultiIndividualDocument";

    public static final String HOOK_ATR_COMMIT = "atrCommit";
    public static final String HOOK_ATR_COMMIT_AMBIGUITY_RESOLUTION = "atrCommitAmbiguityResolution";
    public static final String HOOK_ATR_ABORT = "atrAbort";
    public static final String HOOK_ATR_ROLLBACK_COMPLETE = "atrRollbackComplete";
    public static final String HOOK_ATR_PENDING = "atrPending";
    public static final String HOOK_ATR_COMPLETE = "atrComplete";

    public Consumer<CoreTransactionAttemptContext> beforeAtrCommit = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> beforeAtrCommitAmbiguityResolution = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> afterAtrCommit = (ctx) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeDocCommitted = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeRollbackDeleteInserted = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterDocCommittedBeforeSavingCAS = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterDocCommitted = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeStagedInsert = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeStagedRemove = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeStagedReplace = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeDocRemoved = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeDocRolledBack = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterDocRemovedPreRetry = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterDocRemovedPostRetry = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterGetComplete = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterStagedReplaceComplete = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterStagedRemoveComplete = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterStagedInsertComplete = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterRollbackReplaceOrRemove = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterRollbackDeleteInserted = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeCheckATREntryForBlockingDoc = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeDocGet = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeGetDocInExistsDuringStagedInsert = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeOverwritingStagedInsertRemoval = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeRemoveStagedInsert = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterRemoveStagedInsert = (ctx, x) -> {};
    public Consumer<CoreTransactionAttemptContext> afterDocsCommitted = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> afterDocsRemoved = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> afterAtrPending = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> beforeAtrPending = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> beforeAtrComplete = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> beforeAtrRolledBack = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> afterAtrComplete = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> beforeAtrAborted = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> afterAtrAborted = (ctx) -> {};
    public Consumer<CoreTransactionAttemptContext> afterAtrRolledBack = (ctx) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeQuery = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> afterQuery = (ctx, x) -> {};
    public Function<CoreTransactionAttemptContext, Optional<String>> randomAtrIdForVbucket = (ctx) -> Optional.empty();
    public TriFunction<CoreTransactionAttemptContext, String, Optional<String>, Boolean> hasExpiredClientSideHook = (ctx, place, docId) -> false;
    public BiConsumer<CoreTransactionAttemptContext, String> beforeUnlockGet = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeUnlockInsert = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeUnlockReplace = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeUnlockRemove = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeUnlockQuery = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeDocChangedDuringStaging = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeDocChangedDuringCommit = (ctx, x) -> {};
    public BiConsumer<CoreTransactionAttemptContext, String> beforeDocChangedDuringRollback = (ctx, x) -> {};
}
