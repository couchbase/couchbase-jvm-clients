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
import com.couchbase.client.core.manager.CoreQueryIndexManager;
import com.couchbase.client.java.AsyncCluster;

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
import static java.util.Objects.requireNonNull;

/**
 * Performs management operations on query indexes.
 */
public class AsyncQueryIndexManager {

  private final CoreQueryIndexManager internal;

  /**
   * Creates a new {@link AsyncQueryIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link AsyncCluster#queryIndexes()}
   * instead.
   *
   * @param cluster the async cluster to perform the queries on.
   */
  @Stability.Internal
  public AsyncQueryIndexManager(final AsyncCluster cluster) {
    this.internal = new CoreQueryIndexManager(requireNonNull(cluster).core());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String bucketName, final String indexName,
                                             final Collection<String> fields) {
    return createIndex(bucketName, indexName, fields, createQueryIndexOptions());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createIndex(final String bucketName, final String indexName,
                                             final Collection<String> fields, final CreateQueryIndexOptions options) {
    return internal.createIndex(bucketName, indexName, fields, options.build());
  }

  /**
   * Creates a primary query index.
   * <p>
   * By default, this method will create an index on the bucket. If an index needs to be created on a collection,
   * both {@link CreatePrimaryQueryIndexOptions#scopeName(String)} and
   * {@link CreatePrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to create the index on.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createPrimaryIndex(final String bucketName) {
    return createPrimaryIndex(bucketName, createPrimaryQueryIndexOptions());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexFailureException (async) if creating the index failed (see reason for details).
   * @throws IndexExistsException (async) if an index already exists with the given name on the keyspace.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> createPrimaryIndex(final String bucketName,
                                                    final CreatePrimaryQueryIndexOptions options) {
    return internal.createPrimaryIndex(bucketName, options.build());
  }

  /**
   * Fetches all indexes from the bucket.
   * <p>
   * By default, this method will fetch all index on the bucket. If the indexes should be loaded for a collection,
   * both {@link GetAllQueryIndexesOptions#scopeName(String)} and
   * {@link GetAllQueryIndexesOptions#collectionName(String)} must be set. If all indexes for a scope should be loaded,
   * only the {@link GetAllQueryIndexesOptions#scopeName(String)} can be set.
   *
   * @param bucketName the name of the bucket to load the indexes from.
   * @return a {@link CompletableFuture} completing with a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<QueryIndex>> getAllIndexes(final String bucketName) {
    return getAllIndexes(bucketName, getAllQueryIndexesOptions());
  }

  /**
   * Fetches all indexes from the bucket with custom options.
   * <p>
   * By default, this method will fetch all index on the bucket. If the indexes should be loaded for a collection,
   * both {@link GetAllQueryIndexesOptions#scopeName(String)} and
   * {@link GetAllQueryIndexesOptions#collectionName(String)} must be set. If all indexes for a scope should be loaded,
   * only the {@link GetAllQueryIndexesOptions#scopeName(String)} can be set.
   *
   * @param bucketName the name of the bucket to load the indexes from.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<QueryIndex>> getAllIndexes(final String bucketName,
                                                           final GetAllQueryIndexesOptions options) {
    return internal.getAllIndexes(bucketName, options.build())
            .thenApply(rows -> rows.stream()
                    .map(row -> new QueryIndex(row))
                    .collect(Collectors.toList()));
  }

  /**
   * Drops the primary index from a bucket.
   * <p>
   * By default, this method will drop the primary index on the bucket. If the index should be dropped on a collection,
   * both {@link DropPrimaryQueryIndexOptions#scopeName(String)} and
   * {@link DropPrimaryQueryIndexOptions#collectionName(String)} must be set.
   *
   * @param bucketName the name of the bucket to drop the indexes from.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropPrimaryIndex(final String bucketName) {
    return dropPrimaryIndex(bucketName, dropPrimaryQueryIndexOptions());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropPrimaryIndex(final String bucketName, final DropPrimaryQueryIndexOptions options) {
    return internal.dropPrimaryIndex(bucketName, options.build());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String bucketName, final String indexName) {
    return dropIndex(bucketName, indexName, dropQueryIndexOptions());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws IndexNotFoundException (async) if the index does not exist.
   * @throws IndexFailureException (async) if dropping the index failed (see reason for details).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropIndex(final String bucketName, final String indexName,
                                           final DropQueryIndexOptions options) {
    return internal.dropIndex(bucketName, indexName, options.build());
  }

  /**
   * Builds all currently deferred indexes in the bucket's default collection.
   * <p>
   * To target a different collection, see {@link #buildDeferredIndexes(String, BuildQueryIndexOptions)}.
   *
   * @param bucketName the name of the bucket to build deferred indexes for.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> buildDeferredIndexes(final String bucketName) {
    return buildDeferredIndexes(bucketName, buildDeferredQueryIndexesOptions());
  }

  /**
   * Builds all currently deferred indexes in a collection.
   * <p>
   * By default, this method targets the bucket's default collection.
   * To target a different collection, specify both 
   * {@link BuildQueryIndexOptions#scopeName(String)} and
   * {@link BuildQueryIndexOptions#collectionName(String)}.
   *
   * @param bucketName the name of the bucket to build deferred indexes for.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> buildDeferredIndexes(final String bucketName, final BuildQueryIndexOptions options) {
    return internal.buildDeferredIndexes(bucketName, options.build());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> watchIndexes(final String bucketName, final Collection<String> indexNames,
                                              final Duration timeout) {
    return watchIndexes(bucketName, indexNames, timeout, watchQueryIndexesOptions());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> watchIndexes(final String bucketName, final Collection<String> indexNames,
                                              final Duration timeout, final WatchQueryIndexesOptions options) {
    return internal.watchIndexes(bucketName, indexNames, timeout, options.build());
  }
}
