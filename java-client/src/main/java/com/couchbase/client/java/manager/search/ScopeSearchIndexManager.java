/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.CoreAsyncUtils.block;
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
 * The {@link ScopeSearchIndexManager} allows to manage search indexes.
 * <p> 
 * All management is done with scope FTS indexes.  For global FTS indexes, use @{@link SearchIndexManager}.
 */
@Stability.Volatile
public class ScopeSearchIndexManager {
  private final AsyncScopeSearchIndexManager internal;

  public ScopeSearchIndexManager(AsyncScopeSearchIndexManager internal) {
    this.internal = internal;
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public SearchIndex getIndex(String name) {
    return getIndex(name, getSearchIndexOptions());
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public SearchIndex getIndex(String name, GetSearchIndexOptions options) {
    return block(internal.getIndex(name, options));
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return a {@link CompletableFuture} with all index definitions once complete.
   */
  public List<SearchIndex> getAllIndexes() {
    return getAllIndexes(getAllSearchIndexesOptions());
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return a {@link CompletableFuture} with all index definitions once complete.
   */
  public List<SearchIndex> getAllIndexes(GetAllSearchIndexesOptions options) {
    return block(internal.getAllIndexes(options));
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public Long getIndexedDocumentsCount(String name) {
    return getIndexedDocumentsCount(name, getIndexedSearchIndexOptions());
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public Long getIndexedDocumentsCount(String name, GetIndexedSearchIndexOptions options) {
    return block(internal.getIndexedDocumentsCount(name, options));
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   */
  public void upsertIndex(SearchIndex index) {
    upsertIndex(index, upsertSearchIndexOptions());
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   */
  public void upsertIndex(SearchIndex index, UpsertSearchIndexOptions options) {
    block(internal.upsertIndex(index, options));
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void dropIndex(String name) {
    dropIndex(name, dropSearchIndexOptions());
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void dropIndex(String name, DropSearchIndexOptions options) {
    block(internal.dropIndex(name, options));
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public List<JsonObject> analyzeDocument(String name, JsonObject document) {
    return analyzeDocument(name, document, analyzeDocumentOptions());
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public List<JsonObject> analyzeDocument(String name, JsonObject document, AnalyzeDocumentOptions options) {
    return block(internal.analyzeDocument(name, document, options));
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void pauseIngest(String name) {
    pauseIngest(name, pauseIngestSearchIndexOptions());
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void pauseIngest(String name, PauseIngestSearchIndexOptions options) {
    block(internal.pauseIngest(name, options));
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void resumeIngest(String name) {
    resumeIngest(name, resumeIngestSearchIndexOptions());
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void resumeIngest(String name, ResumeIngestSearchIndexOptions options) {
    block(internal.resumeIngest(name, options));
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void allowQuerying(String name) {
    allowQuerying(name, allowQueryingSearchIndexOptions());
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void allowQuerying(String name, AllowQueryingSearchIndexOptions options) {
    block(internal.allowQuerying(name, options));
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void disallowQuerying(String name) {
    disallowQuerying(name, disallowQueryingSearchIndexOptions());
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void disallowQuerying(String name, DisallowQueryingSearchIndexOptions options) {
    block(internal.disallowQuerying(name, options));
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void freezePlan(String name) {
    freezePlan(name, freezePlanSearchIndexOptions());
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void freezePlan(String name, FreezePlanSearchIndexOptions options) {
    block(internal.freezePlan(name, options));
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void unfreezePlan(String name) {
    unfreezePlan(name, unfreezePlanSearchIndexOptions());
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.  This should just be the index name, rather than "bucket.scope.indexName".
   */
  public void unfreezePlan(String name, UnfreezePlanSearchIndexOptions options) {
    block(internal.unfreezePlan(name, options));
  }
}
