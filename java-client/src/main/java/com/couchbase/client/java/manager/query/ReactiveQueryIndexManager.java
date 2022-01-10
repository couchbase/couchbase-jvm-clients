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
 * Performs management operations on query indexes.
 */
public class ReactiveQueryIndexManager {

  /**
   * The underlying async query index manager which performs the actual ops and does the conversions.
   */
  private final AsyncQueryIndexManager async;

  /**
   * Creates a new {@link ReactiveQueryIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link ReactiveCluster#queryIndexes()}
   * instead.
   *
   * @param async the async index manager.
   */
  @Stability.Internal
  public ReactiveQueryIndexManager(final AsyncQueryIndexManager async) {
    this.async = requireNonNull(async);
  }

  /**
   * Provides access to the {@link AsyncQueryIndexManager}.
   */
  public AsyncQueryIndexManager async() {
    return async;
  }

  /**
   * Creates a named query index.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreateQueryIndexOptions#scopeName(String)} and {@link CreateQueryIndexOptions#collectionName(String)}
   * must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @param indexName the name of the query index.
   * @param fields the collection of fields that are part of the index.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createIndex(final String bucketName, final String indexName, final Collection<String> fields) {
    return toMono(() -> async.createIndex(bucketName, indexName, fields));
  }

  /**
   * Creates a named query index with custom options.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreateQueryIndexOptions#scopeName(String)} and {@link CreateQueryIndexOptions#collectionName(String)}
   * must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @param indexName the name of the query index.
   * @param fields the collection of fields that are part of the index.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createIndex(final String bucketName, final String indexName, final Collection<String> fields,
                                final CreateQueryIndexOptions options) {
    return toMono(() -> async.createIndex(bucketName, indexName, fields, options));
  }

  /**
   * Creates a primary query index.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreatePrimaryQueryIndexOptions#scopeName(String)} and
   * {@link CreatePrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createPrimaryIndex(final String bucketName) {
    return toMono(() -> async.createPrimaryIndex(bucketName));
  }

  /**
   * Creates a primary query index with custom options.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreatePrimaryQueryIndexOptions#scopeName(String)} and
   * {@link CreatePrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> createPrimaryIndex(final String bucketName, final CreatePrimaryQueryIndexOptions options) {
    return toMono(() -> async.createPrimaryIndex(bucketName, options));
  }

  /**
   * Fetches all indexes from the bucket.
   * <p>
   * By default, this method will fetch all index on the bucket. If the indexes should be loaded for a collection,
   * both {@link GetAllQueryIndexesOptions#scopeName(String)} and
   * {@link GetAllQueryIndexesOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to load the indexes from.
   * @return a (potentially empty) {@link Flux} of indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<QueryIndex> getAllIndexes(final String bucketName) {
    return toFlux(() -> async.getAllIndexes(bucketName));
  }

  /**
   * Fetches all indexes from the bucket with custom options.
   * <p>
   * By default, this method will fetch all index on the bucket. If the indexes should be loaded for a collection,
   * both {@link GetAllQueryIndexesOptions#scopeName(String)} and
   * {@link GetAllQueryIndexesOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to load the indexes from.
   * @param options the custom options to apply.
   * @return a (potentially empty) {@link Flux} of indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<QueryIndex> getAllIndexes(final String bucketName, final GetAllQueryIndexesOptions options) {
    return toFlux(() -> async.getAllIndexes(bucketName, options));
  }

  /**
   * Drops the primary index from a bucket.
   * <p>
   * By default, this method will drop the primary index on the bucket. If the index should be dropped on a collection,
   * both {@link DropPrimaryQueryIndexOptions#scopeName(String)} and
   * {@link DropPrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropPrimaryIndex(final String bucketName) {
    return toMono(() -> async.dropPrimaryIndex(bucketName));
  }

  /**
   * Drops the primary index from a bucket with custom options.
   * <p>
   * By default, this method will drop the primary index on the bucket. If the index should be dropped on a collection,
   * both {@link DropPrimaryQueryIndexOptions#scopeName(String)} and
   * {@link DropPrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropPrimaryIndex(final String bucketName, final DropPrimaryQueryIndexOptions options) {
    return toMono(() -> async.dropPrimaryIndex(bucketName, options));
  }

  /**
   * Drops a query index from a bucket.
   * <p>
   * By default, this method will drop the index on the bucket. If the index should be dropped on a collection,
   * both {@link DropQueryIndexOptions#scopeName(String)} and
   * {@link DropQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @param indexName the name of the index top drop.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropIndex(final String bucketName, final String indexName) {
    return toMono(() -> async.dropIndex(bucketName, indexName));
  }

  /**
   * Drops a query index from a bucket with custom options.
   * <p>
   * By default, this method will drop the index on the bucket. If the index should be dropped on a collection,
   * both {@link DropQueryIndexOptions#scopeName(String)} and
   * {@link DropQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @param indexName the name of the index top drop.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropIndex(final String bucketName, final String indexName, final DropQueryIndexOptions options) {
    return toMono(() -> async.dropIndex(bucketName, indexName, options));
  }

  /**
   * Builds all currently deferred indexes.
   * <p>
   * By default, this method will build the indexes on the bucket. If the indexes should be built on a collection,
   * both {@link BuildQueryIndexOptions#scopeName(String)} and
   * {@link BuildQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to build deferred indexes for.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> buildDeferredIndexes(final String bucketName) {
    return toMono(() -> async.buildDeferredIndexes(bucketName));
  }

  /**
   * Builds all currently deferred indexes.
   * <p>
   * By default, this method will build the indexes on the bucket. If the indexes should be built on a collection,
   * both {@link BuildQueryIndexOptions#scopeName(String)} and
   * {@link BuildQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to build deferred indexes for.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> buildDeferredIndexes(final String bucketName, final BuildQueryIndexOptions options) {
    return toMono(() -> async.buildDeferredIndexes(bucketName, options));
  }

  /**
   * Watches/Polls indexes until they are online.
   * <p>
   * By default, this method will watch the indexes on the bucket. If the indexes should be watched on a collection,
   * both {@link WatchQueryIndexesOptions#scopeName(String)} and
   * {@link WatchQueryIndexesOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket where the indexes should be watched.
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> watchIndexes(final String bucketName, final Collection<String> indexNames,
                                 final Duration timeout) {
    return toMono(() -> async.watchIndexes(bucketName, indexNames, timeout));
  }

  /**
   * Watches/Polls indexes until they are online with custom options.
   * <p>
   * By default, this method will watch the indexes on the bucket. If the indexes should be watched on a collection,
   * both {@link WatchQueryIndexesOptions#scopeName(String)} and
   * {@link WatchQueryIndexesOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket where the indexes should be watched.
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> watchIndexes(final String bucketName, final Collection<String> indexNames, final Duration timeout,
                                 final WatchQueryIndexesOptions options) {
    return toMono(() -> async.watchIndexes(bucketName, indexNames, timeout, options));
  }

}
