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

package com.couchbase.client.java.manager.search;

import com.couchbase.client.core.util.ReactorOps;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.Reactor.toFlux;
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
import static java.util.Objects.requireNonNull;

/**
 * The {@link ReactiveSearchIndexManager} allows to manage search index structures in a couchbase cluster.
 *
 * @since 3.0.0
 */
public class ReactiveSearchIndexManager {

  private final AsyncSearchIndexManager asyncIndexManager;
  private final ReactorOps reactor;

  public ReactiveSearchIndexManager(final ReactorOps reactor, final AsyncSearchIndexManager asyncIndexManager) {
    this.reactor = requireNonNull(reactor);
    this.asyncIndexManager = requireNonNull(asyncIndexManager);
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.
   * @return the index definition if it exists.
   */
  public Mono<SearchIndex> getIndex(final String name, final GetSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.getIndex(name, options));
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return all currently present indexes.
   */
  public Flux<SearchIndex> getAllIndexes(final GetAllSearchIndexesOptions options) {
    return reactor.publishOnUserScheduler(toFlux(() -> asyncIndexManager.getAllIndexes(options)));
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.
   * @return the number of indexed documents.
   */
  public Mono<Long> getIndexedDocumentsCount(final String name, final GetIndexedSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.getIndexedDocumentsCount(name, options));
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.
   * @param document the document to be analyzed.
   * @return the analyzed sections for the document.
   */
  public Flux<JsonObject> analyzeDocument(final String name, final JsonObject document, final AnalyzeDocumentOptions options) {
    return reactor.publishOnUserScheduler(toFlux(() -> asyncIndexManager.analyzeDocument(name, document, options)));
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition including name and settings.
   */
  public Mono<Void> upsertIndex(final SearchIndex index, final UpsertSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.upsertIndex(index, options));
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> dropIndex(final String name, final DropSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.dropIndex(name, options));
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> pauseIngest(final String name, final PauseIngestSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.pauseIngest(name, options));
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> resumeIngest(final String name, ResumeIngestSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.resumeIngest(name, options));
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> allowQuerying(final String name, AllowQueryingSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.allowQuerying(name, options));
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> disallowQuerying(final String name, final DisallowQueryingSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.disallowQuerying(name, options));
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> freezePlan(final String name, final FreezePlanSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.freezePlan(name, options));
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> unfreezePlan(final String name, final UnfreezePlanSearchIndexOptions options) {
    return reactor.publishOnUserScheduler(() -> asyncIndexManager.unfreezePlan(name, options));
  }

  /**
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.
   * @return the index definition if it exists.
   */
  public Mono<SearchIndex> getIndex(final String name) {
    return getIndex(name, getSearchIndexOptions());
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return all currently present indexes.
   */
  public Flux<SearchIndex> getAllIndexes() {
    return getAllIndexes(getAllSearchIndexesOptions());
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.
   * @return the number of indexed documents.
   */
  public Mono<Long> getIndexedDocumentsCount(final String name) {
    return getIndexedDocumentsCount(name, getIndexedSearchIndexOptions());
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.
   * @param document the document to be analyzed.
   * @return the analyzed sections for the document.
   */
  public Flux<JsonObject> analyzeDocument(final String name, final JsonObject document) {
    return analyzeDocument(name, document, analyzeDocumentOptions());
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition including name and settings.
   */
  public Mono<Void> upsertIndex(final SearchIndex index) {
    return upsertIndex(index, upsertSearchIndexOptions());
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> dropIndex(final String name) {
    return dropIndex(name, dropSearchIndexOptions());
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> pauseIngest(final String name) {
    return pauseIngest(name, pauseIngestSearchIndexOptions());
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> resumeIngest(final String name) {
    return resumeIngest(name, resumeIngestSearchIndexOptions());
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> allowQuerying(final String name) {
   return allowQuerying(name, allowQueryingSearchIndexOptions());
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> disallowQuerying(final String name) {
    return disallowQuerying(name, disallowQueryingSearchIndexOptions());
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> freezePlan(final String name) {
    return freezePlan(name, freezePlanSearchIndexOptions());
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   */
  public Mono<Void> unfreezePlan(final String name) {
    return unfreezePlan(name, unfreezePlanSearchIndexOptions());
  }

}
