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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.error.IndexNotFoundException;
import com.couchbase.client.java.ReactiveCluster;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import static com.couchbase.client.core.Reactor.toFlux;
import static com.couchbase.client.core.Reactor.toMono;
import static java.util.Objects.requireNonNull;

/**
 * Performs management operations on query indexes at the ReactiveCollection level.
 */
@Stability.Volatile
public class ReactiveCollectionQueryIndexManager {

  /**
   * The underlying async query index manager which performs the actual ops and does the conversions.
   */
  private final AsyncCollectionQueryIndexManager async;

  /**
   * Creates a new {@link ReactiveCollectionQueryIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link ReactiveCluster#queryIndexes()}
   * instead.
   *
   * @param async the async index manager.
   */
  @Stability.Internal
  public ReactiveCollectionQueryIndexManager(final AsyncCollectionQueryIndexManager async) {
    this.async = requireNonNull(async);
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
  public Mono<Void> createIndex(final String indexName, final Collection<String> fields) {
    return toMono(() -> async.createIndex(indexName, fields));
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
  public Mono<Void> createIndex(final String indexName, final Collection<String> fields,
                                final CreateQueryIndexOptions options) {
    return toMono(() -> async.createIndex(indexName, fields, options));
  }

  /**
   * Creates a primary query index on this collection.
   *
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createPrimaryIndex() {
    return toMono(() -> async.createPrimaryIndex());
  }

  /**
   * Creates a primary query index with custom options, on this collection.
   *
   * @param options the custom options to apply.
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createPrimaryIndex(final CreatePrimaryQueryIndexOptions options) {
    return toMono(() -> async.createPrimaryIndex(options));
  }

  /**
   * Fetches all indexes on this collection.
   *
   * @return a (potentially empty) {@link Flux} of indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Flux<QueryIndex> getAllIndexes() {
    return toFlux(() -> async.getAllIndexes());
  }

  /**
   * Fetches all indexes from this collection with custom options.
   *
   * @param options the custom options to apply.
   * @return a (potentially empty) {@link Flux} of indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Flux<QueryIndex> getAllIndexes(final GetAllQueryIndexesOptions options) {
    return toFlux(() -> async.getAllIndexes(options));
  }

  /**
   * Drops the primary index from this collection.
   *
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropPrimaryIndex() {
    return toMono(() -> async.dropPrimaryIndex());
  }

  /**
   * Drops the primary index from this collection.
   *
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropPrimaryIndex(final DropPrimaryQueryIndexOptions options) {
    return toMono(() -> async.dropPrimaryIndex(options));
  }

  /**
   * Drops a query index from this collection.
   *
   * @param indexName the name of the index to drop.
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropIndex(final String indexName) {
    return toMono(() -> async.dropIndex(indexName));
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
  public Mono<Void> dropIndex(final String indexName, final DropQueryIndexOptions options) {
    return toMono(() -> async.dropIndex(indexName, options));
  }

  /**
   * Builds all currently deferred indexes on this collection.
   *
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> buildDeferredIndexes() {
    return toMono(() -> async.buildDeferredIndexes());
  }

  /**
   * Builds all currently deferred indexes on this collection, with custom options.
   *
   * @param options the custom options to apply.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> buildDeferredIndexes(final BuildQueryIndexOptions options) {
    return toMono(() -> async.buildDeferredIndexes(options));
  }

  /**
   * Watches/Polls indexes on this collection until they are online.
   *
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> watchIndexes(final Collection<String> indexNames,
                                 final Duration timeout) {
    return toMono(() -> async.watchIndexes(indexNames, timeout));
  }

  /**
   * Watches/Polls indexes on this collection until they are online with custom options.
   *
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @param options the custom options to apply.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> watchIndexes(final Collection<String> indexNames, final Duration timeout,
                                 final WatchQueryIndexesOptions options) {
    return toMono(() -> async.watchIndexes(indexNames, timeout, options));
  }
}
