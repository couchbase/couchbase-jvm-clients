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

package com.couchbase.client.java.manager;

import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.msg.search.GetSearchIndexRequest;
import com.couchbase.client.core.msg.search.RemoveSearchIndexRequest;
import com.couchbase.client.core.msg.search.UpsertSearchIndexRequest;
import reactor.core.publisher.Mono;

/**
 * The {@link ReactiveSearchIndexManager} allows to manage search index structures in a couchbase cluster.
 *
 * @since 3.0.0
 */
public class ReactiveSearchIndexManager {

  private final AsyncSearchIndexManager asyncSearchIndexManager;

  public ReactiveSearchIndexManager(AsyncSearchIndexManager asyncSearchIndexManager) {
    this.asyncSearchIndexManager = asyncSearchIndexManager;
  }

  /**
   * Fetches an already created index from the server.
   *
   * @param name the name of the search index.
   * @return a {@link Mono} that will complete once the index definition is loaded.
   */
  public Mono<SearchIndex> get(final String name) {
    return Mono.defer(() ->{
      GetSearchIndexRequest request = asyncSearchIndexManager.getRequest(name);
      return Reactor
        .wrap(request, request.response(), true)
        .map(r -> SearchIndex.fromJson(r.content()));
    });
  }

  /**
   * Inserts a search index which does not exist already.
   *
   * <p>Note that you must create a new index with a new name using {@link SearchIndex#from(String, SearchIndex)} when
   * fetching an index definition via {@link #get(String)}. Otherwise only replace can be used since
   * a UUID is present which uniquely identifies an index definition on the cluster.</p>
   *
   * @param index the name of the index.
   * @return a {@link Mono} that will complete once the index definition is inserted.
   */
  public Mono<Void> insert(final SearchIndex index) {
    return Mono.defer(() ->{
      UpsertSearchIndexRequest request = asyncSearchIndexManager.insertRequest(index);
      return Reactor
        .wrap(request, request.response(), true)
        .then();
    });
  }

  /**
   * Updates a previously loaded index with new params.
   *
   * <p>It is important that the index needs to be loaded from the server when calling this method, since a
   * UUID is present in the response and needs to be subsequently sent to the server on an update request. If
   * you just want to create a new one, use {@link #insert(SearchIndex)} (SearchIndex)} instead.</p>
   *
   * @param index the index previously loaded via {@link #get(String)} (String)}.
   * @return a {@link Mono} that will complete once the index definition is updated.
   */
  public Mono<Void> replace(final SearchIndex index) {
    return Mono.defer(() ->{
      UpsertSearchIndexRequest request = asyncSearchIndexManager.replaceRequest(index);
      return Reactor
        .wrap(request, request.response(), true)
        .then();
    });
  }

  /**
   * Removes a search index from the cluster.
   *
   * @param name the name of the index.
   * @return a {@link Mono} that completes once the index is removed.
   */
  public Mono<Void> remove(final String name) {
    return Mono.defer(() ->{
      RemoveSearchIndexRequest request = asyncSearchIndexManager.removeRequest(name);
      return Reactor
        .wrap(request, request.response(), true)
        .then();
    });
  }

}
