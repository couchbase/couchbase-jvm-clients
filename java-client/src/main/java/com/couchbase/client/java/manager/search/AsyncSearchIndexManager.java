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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.search.GetSearchIndexRequest;
import com.couchbase.client.core.msg.search.RemoveSearchIndexRequest;
import com.couchbase.client.core.msg.search.UpsertSearchIndexRequest;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link AsyncSearchIndexManager} allows to manage search index structures in a couchbase cluster.
 *
 * @since 3.0.0
 */
public class AsyncSearchIndexManager {

  private final Core core;
  private final CoreEnvironment environment;

  public AsyncSearchIndexManager(final Core core) {
    this.core = core;
    this.environment = core.context().environment();
  }

  /**
   * Fetches an already created index from the server.
   *
   * @param name the name of the search index.
   * @return a {@link CompletableFuture} that will complete once the index definition is loaded.
   */
  public CompletableFuture<SearchIndex> get(final String name) {
    GetSearchIndexRequest request = getRequest(name);
    core.send(request);
    return request.response().thenApply(response -> SearchIndex.fromJson(response.content()));
  }

  GetSearchIndexRequest getRequest(final String name) {
    Duration timeout = environment.timeoutConfig().managementTimeout();
    RetryStrategy retryStrategy = environment.retryStrategy();

    return new GetSearchIndexRequest(timeout, core.context(), retryStrategy,
      environment.credentials(), name);
  }

  /**
   * Inserts a search index which does not exist already.
   *
   * <p>Note that you must create a new index with a new name using {@link SearchIndex#from(String, SearchIndex)} when
   * fetching an index definition via {@link #get(String)}. Otherwise only replace can be used since
   * a UUID is present which uniquely identifies an index definition on the cluster.</p>
   *
   * @param index the name of the index.
   * @return a {@link CompletableFuture} that will complete once the index definition is inserted.
   */
  public CompletableFuture<Void> insert(final SearchIndex index) {
    UpsertSearchIndexRequest request = insertRequest(index);
    core.send(request);
    return request.response().thenApply(response -> null);
  }

  UpsertSearchIndexRequest insertRequest(final SearchIndex index) {
    if (index.uuid().isPresent()) {
      throw new IllegalArgumentException("No UUID in the index must be present to insert it.");
    }

    Duration timeout = environment.timeoutConfig().managementTimeout();
    RetryStrategy retryStrategy = environment.retryStrategy();

    return new UpsertSearchIndexRequest(timeout, core.context(), retryStrategy,
      environment.credentials(), index.name(), index.toJson());
  }

    /**
     * Updates a previously loaded index with new params.
     *
     * <p>It is important that the index needs to be loaded from the server when calling this method, since a
     * UUID is present in the response and needs to be subsequently sent to the server on an update request. If
     * you just want to create a new one, use {@link #insert(SearchIndex)} instead.</p>
     *
     * @param index the index previously loaded via {@link #get(String)}.
     * @return a {@link CompletableFuture} that will complete once the index definition is updated.
     */
  public CompletableFuture<Void> replace(final SearchIndex index) {
    UpsertSearchIndexRequest request = replaceRequest(index);
    core.send(request);
    return request.response().thenApply(response -> null);
  }

  UpsertSearchIndexRequest replaceRequest(final SearchIndex index) {
    if (!index.uuid().isPresent()) {
      throw new IllegalArgumentException("A UUID in the index must be present to replace it.");
    }

    Duration timeout = environment.timeoutConfig().managementTimeout();
    RetryStrategy retryStrategy = environment.retryStrategy();

    return new UpsertSearchIndexRequest(timeout, core.context(), retryStrategy,
      environment.credentials(), index.name(), index.toJson());
  }

  /**
   * Removes a search index from the cluster.
   *
   * @param name the name of the index.
   * @return a {@link CompletableFuture} that completes once the index is removed.
   */
  public CompletableFuture<Void> remove(final String name) {
    RemoveSearchIndexRequest request = removeRequest(name);
    core.send(request);
    return request.response().thenApply(response -> null);
  }

  RemoveSearchIndexRequest removeRequest(final String name) {
    Duration timeout = environment.timeoutConfig().managementTimeout();
    RetryStrategy retryStrategy = environment.retryStrategy();

    return new RemoveSearchIndexRequest(timeout, core.context(), retryStrategy,
      environment.credentials(), name);
  }

}
