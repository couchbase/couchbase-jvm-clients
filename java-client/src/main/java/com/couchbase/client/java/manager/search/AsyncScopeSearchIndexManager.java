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

package com.couchbase.client.java.manager.search;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.manager.search.CoreSearchIndex;
import com.couchbase.client.core.api.manager.search.CoreSearchIndexManager;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.AsyncScope;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.java.manager.search.AllowQueryingSearchIndexOptions.allowQueryingSearchIndexOptions;
import static com.couchbase.client.java.manager.search.AnalyzeDocumentOptions.analyzeDocumentOptions;
import static com.couchbase.client.java.manager.search.DisallowQueryingSearchIndexOptions.disallowQueryingSearchIndexOptions;
import static com.couchbase.client.java.manager.search.DropSearchIndexOptions.dropSearchIndexOptions;
import static com.couchbase.client.java.manager.search.FreezePlanSearchIndexOptions.freezePlanSearchIndexOptions;
import static com.couchbase.client.java.manager.search.GetAllSearchIndexesOptions.getAllSearchIndexesOptions;
import static com.couchbase.client.java.manager.search.GetIndexedSearchIndexOptions.getIndexedSearchIndexOptions;
import static com.couchbase.client.java.manager.search.GetSearchIndexOptions.getSearchIndexOptions;
import static com.couchbase.client.java.manager.search.PauseIngestSearchIndexOptions.pauseIngestSearchIndexOptions;
import static com.couchbase.client.java.manager.search.ResumeIngestSearchIndexOptions.resumeIngestSearchIndexOptions;
import static com.couchbase.client.java.manager.search.UnfreezePlanSearchIndexOptions.unfreezePlanSearchIndexOptions;
import static com.couchbase.client.java.manager.search.UpsertSearchIndexOptions.upsertSearchIndexOptions;

/**
 * The {@link AsyncScopeSearchIndexManager} allows to manage scope level search indexes.
 * <p> 
 * All management is done with scope FTS indexes.  For global FTS indexes, use @{@link AsyncSearchIndexManager}.
 */
@Stability.Volatile
public class AsyncScopeSearchIndexManager {
  private final CoreSearchIndexManager internal;

  public AsyncScopeSearchIndexManager(CoreCouchbaseOps couchbaseOps, AsyncScope scope) {
    this.internal = couchbaseOps.scopeSearchIndexManager(new CoreBucketAndScope(scope.bucketName(), scope.name()));
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public CompletableFuture<SearchIndex> getIndex(String name) {
    return getIndex(name, getSearchIndexOptions());
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public CompletableFuture<SearchIndex> getIndex(String name, GetSearchIndexOptions options) {
    return internal.getIndex(name, options.build())
            .thenApply(AsyncScopeSearchIndexManager::convert);
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return a {@link CompletableFuture} with all index definitions once complete.
   */
  public CompletableFuture<List<SearchIndex>> getAllIndexes() {
    return getAllIndexes(getAllSearchIndexesOptions());
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return a {@link CompletableFuture} with all index definitions once complete.
   */
  public CompletableFuture<List<SearchIndex>> getAllIndexes(GetAllSearchIndexesOptions options) {
    return internal.getAllIndexes(options.build())
            .thenApply(indexes -> indexes.stream()
                    .map(AsyncScopeSearchIndexManager::convert)
                    .collect(Collectors.toList()));
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public CompletableFuture<Long> getIndexedDocumentsCount(String name) {
    return getIndexedDocumentsCount(name, getIndexedSearchIndexOptions());
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public CompletableFuture<Long> getIndexedDocumentsCount(String name, GetIndexedSearchIndexOptions options) {
    return internal.getIndexedDocumentsCount(name, options.build());
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> upsertIndex(SearchIndex index) {
    return upsertIndex(index, upsertSearchIndexOptions());
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> upsertIndex(SearchIndex index, UpsertSearchIndexOptions options) {
    return internal.upsertIndex(convert(index), options.build());
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> dropIndex(String name) {
    return dropIndex(name, dropSearchIndexOptions());
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> dropIndex(String name, DropSearchIndexOptions options) {
    return internal.dropIndex(name, options.build());
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public CompletableFuture<List<JsonObject>> analyzeDocument(String name, JsonObject document) {
    return analyzeDocument(name, document, analyzeDocumentOptions());
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public CompletableFuture<List<JsonObject>> analyzeDocument(String name, JsonObject document, AnalyzeDocumentOptions options) {
    try {
      ObjectNode json = (ObjectNode) Mapper.reader().readTree(document.toBytes());
      return internal.analyzeDocument(name, json, options.build())
              .thenApply(list -> list.stream().map(o -> JsonObject.fromJson(o.toString())).collect(Collectors.toList()));
    } catch (IOException e) {
      throw new DecodingFailureException("Failed to decode document", e);
    }
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> pauseIngest(String name) {
    return pauseIngest(name, pauseIngestSearchIndexOptions());
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> pauseIngest(String name, PauseIngestSearchIndexOptions options) {
    return internal.pauseIngest(name, options.build());
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> resumeIngest(String name) {
    return resumeIngest(name, resumeIngestSearchIndexOptions());
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> resumeIngest(String name, ResumeIngestSearchIndexOptions options) {
    return internal.resumeIngest(name, options.build());
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> allowQuerying(String name) {
    return allowQuerying(name, allowQueryingSearchIndexOptions());
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> allowQuerying(String name, AllowQueryingSearchIndexOptions options) {
    return internal.allowQuerying(name, options.build());
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> disallowQuerying(String name) {
    return disallowQuerying(name, disallowQueryingSearchIndexOptions());
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> disallowQuerying(String name, DisallowQueryingSearchIndexOptions options) {
    return internal.disallowQuerying(name, options.build());
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> freezePlan(String name) {
    return freezePlan(name, freezePlanSearchIndexOptions());
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> freezePlan(String name, FreezePlanSearchIndexOptions options) {
    return internal.freezePlan(name, options.build());
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> unfreezePlan(String name) {
    return unfreezePlan(name, unfreezePlanSearchIndexOptions());
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> unfreezePlan(String name, UnfreezePlanSearchIndexOptions options) {
    return internal.unfreezePlan(name, options.build());
  }

  private static SearchIndex convert(CoreSearchIndex index) {
    return new SearchIndex(index.uuid(),
            index.name(),
            index.type(),
            index.params(),
            index.sourceUuid(),
            index.sourceName(),
            index.sourceParams(),
            index.sourceType(),
            index.planParams());
  }

  private static CoreSearchIndex convert(SearchIndex index) {
    return new CoreSearchIndex(index.uuid(),
            index.name(),
            index.type(),
            index.params(),
            index.sourceUuid(),
            index.sourceName(),
            index.sourceParams(),
            index.sourceType(),
            index.planParams());
  }
}
