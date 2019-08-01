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

import static com.couchbase.client.java.AsyncUtils.block;

/**
 * The {@link SearchIndexManager} allows to manage search index structures in a couchbase cluster.
 *
 * @since 3.0.0
 */
public class SearchIndexManager {

  private final AsyncSearchIndexManager asyncIndexManager;

  public SearchIndexManager(final AsyncSearchIndexManager asyncIndexManager) {
    this.asyncIndexManager = asyncIndexManager;
  }

  /**
   * Fetches an already created index from the server.
   *
   * @param name the name of the search index.
   * @return once the index definition is loaded.
   */
  public SearchIndex get(final String name) {
    return block(asyncIndexManager.get(name));
  }

  /**
   * Inserts a search index which does not exist already.
   *
   * <p>Note that you must create a new index with a new name using {@link SearchIndex#from(String, SearchIndex)} when
   * fetching an index definition via {@link #get(String)}. Otherwise only replace can be used since
   * a UUID is present which uniquely identifies an index definition on the cluster.</p>
   *
   * @param index the name of the index.
   */
  public void insert(final SearchIndex index) {
    block(asyncIndexManager.insert(index));
  }

  /**
   * Updates a previously loaded index with new params.
   *
   * <p>It is important that the index needs to be loaded from the server when calling this method, since a
   * UUID is present in the response and needs to be subsequently sent to the server on an update request. If
   * you just want to create a new one, use {@link #insert(SearchIndex)} instead.</p>
   *
   * @param index the index previously loaded via {@link #get(String)}.
   */
  public void replace(final SearchIndex index) {
    block(asyncIndexManager.replace(index));
  }

  /**
   * Removes a search index from the cluster.
   *
   * @param name the name of the index.
   */
  public void remove(final String name) {
    block(asyncIndexManager.remove(name));
  }

}