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

package com.couchbase.client.java.manager.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.IndexExistsException;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.core.error.IndexNotFoundException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static com.couchbase.client.java.AsyncUtils.block;

/**
 * Performs management operations on query indexes at the Collection level.
 */
@Stability.Volatile
public class CollectionQueryIndexManager {

  private final AsyncCollectionQueryIndexManager async;

  /**
   * Creates a new {@link CollectionQueryIndexManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link com.couchbase.client.java.Collection#queryIndexes()}
   * instead.
   */
  @Stability.Internal
  public CollectionQueryIndexManager(AsyncCollectionQueryIndexManager async) {
    this.async = async;
  }

  /**
   * Creates a primary query index on this collection.
   *
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createPrimaryIndex() {
    block(async.createPrimaryIndex());
  }

  /**
   * Creates a primary query index with custom options, on this collection.
   *
   * @param options the custom options to apply.
   * @throws IndexFailureException if creating the index failed (see reason for details).
   * @throws IndexExistsException if an index already exists with the given name on this collection.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void createPrimaryIndex(final CreatePrimaryQueryIndexOptions options) {
    block(async.createPrimaryIndex(options));
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
  public void createIndex(final String indexName, final Collection<String> fields) {
    block(async.createIndex(indexName, fields));
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
  public void createIndex(final String indexName, final Collection<String> fields,
                          final CreateQueryIndexOptions options) {
    block(async.createIndex(indexName, fields, options));
  }

  /**
   * Fetches all indexes on this collection.
   *
   * @return a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<QueryIndex> getAllIndexes() {
    return block(async.getAllIndexes());
  }

  /**
   * Fetches all indexes from this collection with custom options.
   *
   * @param options the custom options to apply.
   * @return a list of (potentially empty) indexes or failed with an error.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<QueryIndex> getAllIndexes(final GetAllQueryIndexesOptions options) {
    return block(async.getAllIndexes(options));
  }

  /**
   * Drops the primary index from this collection.
   *
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropPrimaryIndex() {
    block(async.dropPrimaryIndex());
  }

  /**
   * Drops the primary index from this collection with custom options.
   *
   * @param options the custom options to apply.
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropPrimaryIndex(final DropPrimaryQueryIndexOptions options) {
    block(async.dropPrimaryIndex(options));
  }

  /**
   * Drops a query index from this collection.
   *
   * @param indexName the name of the index to drop.
   * @throws IndexNotFoundException if the index does not exist.
   * @throws IndexFailureException if dropping the index failed (see reason for details).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropIndex(final String indexName) {
    block(async.dropIndex(indexName));
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
  public void dropIndex(final String indexName, final DropQueryIndexOptions options) {
    block(async.dropIndex(indexName, options));
  }

  /**
   * Builds all currently deferred indexes on this collection.
   *
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void buildDeferredIndexes() {
    block(async.buildDeferredIndexes());
  }

  /**
   * Builds all currently deferred indexes on this collection, with custom options.
   *
   * @param options the custom options to apply.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void buildDeferredIndexes(final BuildQueryIndexOptions options) {
    block(async.buildDeferredIndexes(options));
  }

  /**
   * Watches/Polls indexes on this collection until they are online.
   *
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void watchIndexes(final Collection<String> indexNames, final Duration timeout) {
    block(async.watchIndexes(indexNames, timeout));
  }

  /**
   * Watches/Polls indexes on this collection until they are online with custom options.
   *
   * @param indexNames the names of the indexes to watch.
   * @param timeout the maximum amount of time the indexes should be watched.
   * @param options the custom options to apply.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void watchIndexes(final Collection<String> indexNames, final Duration timeout,
                           final WatchQueryIndexesOptions options) {
    block(async.watchIndexes(indexNames, timeout, options));
  }
}
