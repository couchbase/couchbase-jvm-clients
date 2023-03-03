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

import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.api.manager.search.CoreSearchIndexManager;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.json.Mapper;
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
 * The {@link AsyncSearchIndexManager} allows to manage search index structures in a couchbase cluster.
 *
 * @since 3.0.0
 */
public class AsyncSearchIndexManager {
  private final CoreSearchIndexManager internal;

  public AsyncSearchIndexManager(CoreCouchbaseOps couchbaseOps) {
    this.internal = couchbaseOps.clusterSearchIndexManager();
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public CompletableFuture<SearchIndex> getIndex(final String name) {
    return getIndex(name, getSearchIndexOptions());
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} the found index once complete.
   */
  public CompletableFuture<SearchIndex> getIndex(final String name, GetSearchIndexOptions options) {
    return internal.getIndex(name, options.build())
            .thenApply(SearchIndexManagerUtil::convert);  }

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
  public CompletableFuture<List<SearchIndex>> getAllIndexes(final GetAllSearchIndexesOptions options) {
    return internal.getAllIndexes(options.build())
            .thenApply(indexes -> indexes.stream()
                    .map(SearchIndexManagerUtil::convert)
                    .collect(Collectors.toList()));
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public CompletableFuture<Long> getIndexedDocumentsCount(final String name) {
    return getIndexedDocumentsCount(name, getIndexedSearchIndexOptions());
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} with the indexed documents count once complete.
   */
  public CompletableFuture<Long> getIndexedDocumentsCount(final String name, final GetIndexedSearchIndexOptions options) {
    return internal.getIndexedDocumentsCount(name, options.build());
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> upsertIndex(final SearchIndex index) {
    return upsertIndex(index, upsertSearchIndexOptions());
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition to upsert.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> upsertIndex(final SearchIndex index, final UpsertSearchIndexOptions options) {
    return internal.upsertIndex(SearchIndexManagerUtil.convert(index), options.build());
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> dropIndex(final String name) {
    return dropIndex(name, dropSearchIndexOptions());
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> dropIndex(final String name, final DropSearchIndexOptions options) {
    return internal.dropIndex(name, options.build());
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public CompletableFuture<List<JsonObject>> analyzeDocument(final String name, final JsonObject document) {
    return analyzeDocument(name, document, analyzeDocumentOptions());
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.
   * @param document the document to analyze.
   * @return a {@link CompletableFuture} with analyzed document parts once complete.
   */
  public CompletableFuture<List<JsonObject>> analyzeDocument(final String name, final JsonObject document,
                                                             final AnalyzeDocumentOptions options) {
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
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> pauseIngest(final String name) {
    return pauseIngest(name, pauseIngestSearchIndexOptions());
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> pauseIngest(final String name, final PauseIngestSearchIndexOptions options) {
    return internal.pauseIngest(name, options.build());
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> resumeIngest(final String name) {
    return resumeIngest(name, resumeIngestSearchIndexOptions());
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> resumeIngest(final String name, final ResumeIngestSearchIndexOptions options) {
    return internal.resumeIngest(name, options.build());
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> allowQuerying(final String name) {
    return allowQuerying(name, allowQueryingSearchIndexOptions());
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> allowQuerying(final String name, final AllowQueryingSearchIndexOptions options) {
    return internal.allowQuerying(name, options.build());
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> disallowQuerying(final String name) {
    return disallowQuerying(name, disallowQueryingSearchIndexOptions());
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> disallowQuerying(final String name, final DisallowQueryingSearchIndexOptions options) {
    return internal.disallowQuerying(name, options.build());
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> freezePlan(final String name) {
    return freezePlan(name, freezePlanSearchIndexOptions());
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> freezePlan(final String name, final FreezePlanSearchIndexOptions options) {
    return internal.freezePlan(name, options.build());
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> unfreezePlan(final String name) {
    return unfreezePlan(name, unfreezePlanSearchIndexOptions());
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} indicating request completion.
   */
  public CompletableFuture<Void> unfreezePlan(final String name, final UnfreezePlanSearchIndexOptions options) {
    return internal.unfreezePlan(name, options.build());
  }

}
