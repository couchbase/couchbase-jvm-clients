/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.api.manager.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The same interface is used for ScopeSearchIndexManager and SearchIndexManager, as there is
 * no API difference between them at this level.
 */
@Stability.Internal
public
interface CoreSearchIndexManager {

  CompletableFuture<CoreSearchIndex> getIndex(String name, CoreCommonOptions options);

  CompletableFuture<List<CoreSearchIndex>> getAllIndexes(CoreCommonOptions options);

  CompletableFuture<Long> getIndexedDocumentsCount(String name, CoreCommonOptions options);

  CompletableFuture<Void> upsertIndex(CoreSearchIndex index, CoreCommonOptions options);

  CompletableFuture<Void> dropIndex(String name, CoreCommonOptions options);

  CompletableFuture<List<ObjectNode>> analyzeDocument(String name, ObjectNode document, CoreCommonOptions options);

  CompletableFuture<Void> pauseIngest(String name, CoreCommonOptions options);

  CompletableFuture<Void> resumeIngest(String name, CoreCommonOptions options);

  CompletableFuture<Void> allowQuerying(String name, CoreCommonOptions options);

  CompletableFuture<Void> disallowQuerying(String name, CoreCommonOptions options);

  CompletableFuture<Void> freezePlan(String name, CoreCommonOptions options);

  CompletableFuture<Void> unfreezePlan(String name, CoreCommonOptions options);
}
