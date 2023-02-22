/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.query;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.core.manager.CoreCollectionQueryIndexManager;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.java.manager.query.BuildQueryIndexOptions.buildDeferredQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.CreateQueryIndexOptions.createQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropPrimaryQueryIndexOptions.dropPrimaryQueryIndexOptions;
import static com.couchbase.client.java.manager.query.DropQueryIndexOptions.dropQueryIndexOptions;
import static com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions.getAllQueryIndexesOptions;
import static com.couchbase.client.java.manager.query.WatchQueryIndexesOptions.watchQueryIndexesOptions;

/**
 * Performs management operations on query indexes at the Collection level.
 */
@Stability.Volatile
public class AsyncCollectionQueryIndexManager {

  private final CoreCollectionQueryIndexManager internal;

  /**
   * Creates a new {@link AsyncCollectionQueryIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link com.couchbase.client.java.AsyncCollection#queryIndexes()}
   * instead.
   */
  @Stability.Internal
  public AsyncCollectionQueryIndexManager(
    final CoreQueryOps queryOps,
    final RequestTracer requestTracer,
    final CoreKeyspace collection
  ) {
    this.internal = new CoreCollectionQueryIndexManager(queryOps, requestTracer, collection);
  }

  /**
   * Creates a named query index on this collection.
   *
   * @param indexName the name of the query index.
   * @param fields the collection of fields that are part of the index.
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String indexName,
                                             final Collection<String> fields) {
    return createIndex(indexName, fields, createQueryIndexOptions());
  }

  /**
   * Creates a named query index with custom options, on this collection.
   *
   * @param indexName the name of the query index.
   * @param fields the collection of fields that are part of the index.
   * @param options the custom options to apply.
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String indexName,
                                             final Collection<String> fields,
                                             final CreateQueryIndexOptions options) {
    return internal.createIndex(indexName, fields, options.build());
  }

  /**
   * Creates a primary query index on this collection.
   *
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createPrimaryIndex() {
    return createPrimaryIndex(createPrimaryQueryIndexOptions());
  }

  /**
   * Creates a primary query index with custom options, on this collection.
   *
   * @param options the custom options to apply.
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createPrimaryIndex(final CreatePrimaryQueryIndexOptions options) {
    return internal.createPrimaryIndex(options.build());
  }

  /**
   * Fetches all indexes on this collection.
   *
   * @return a {@link CompletableFuture} completing with a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<QueryIndex>> getAllIndexes() {
    return getAllIndexes(getAllQueryIndexesOptions());
  }

  /**
   * Fetches all indexes from this collection with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<QueryIndex>> getAllIndexes(final GetAllQueryIndexesOptions options) {
    return internal.getAllIndexes(options.build())
            .thenApply(rows -> rows.stream()
                    .map(row -> new QueryIndex(row))
                    .collect(Collectors.toList()));
  }

  /**
   * Drops the primary index from this collection.
   *
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropPrimaryIndex() {
    return dropPrimaryIndex(dropPrimaryQueryIndexOptions());
  }

  /**
   * Drops the primary index from this collection with custom options.
   *
   * @param options the custom options to apply.
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropPrimaryIndex(final DropPrimaryQueryIndexOptions options) {
    return internal.dropPrimaryIndex(options.build());
  }

  /**
   * Drops a query index from this collection.
   *
   * @param indexName the name of the index to drop.
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String indexName) {
    return dropIndex(indexName, dropQueryIndexOptions());
  }

  /**
   * Drops a query index from this collection with custom options.
   *
   * @param indexName the name of the index to drop.
   * @param options the custom options to apply.
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String indexName,
                                           final DropQueryIndexOptions options) {
    return internal.dropIndex(indexName, options.build());
  }

  /**
   * Builds all currently deferred indexes on this collection.
   *
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> buildDeferredIndexes() {
    return buildDeferredIndexes(buildDeferredQueryIndexesOptions());
  }

  /**
   * Builds all currently deferred indexes on this collection, with custom options.
   *
   * @param options the custom options to apply.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> buildDeferredIndexes(final BuildQueryIndexOptions options) {
    return internal.buildDeferredIndexes(options.build());
  }

  /**
   * Watches/Polls indexes on this collection until they are online.
   *
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> watchIndexes(final Collection<String> indexNames,
                                              final Duration timeout) {
    return watchIndexes(indexNames, timeout, watchQueryIndexesOptions());
  }

  /**
   * Watches/Polls indexes on this collection until they are online with custom options.
   *
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @param options the custom options to apply.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> watchIndexes(final Collection<String> indexNames,
                                              final Duration timeout, final WatchQueryIndexesOptions options) {
    return internal.watchIndexes(indexNames, timeout, options.build());
  }
}
