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

import com.couchbase.client.java.json.JsonObject;

import java.util.List;

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
   * Fetches an index from the server if it exists.
   *
   * @param name the name of the search index.
   * @return the index definition if it exists.
   */
  public SearchIndex getIndex(final String name) {
    return block(asyncIndexManager.getIndex(name));
  }

  /**
   * Fetches all indexes from the server.
   *
   * @return all currently present indexes.
   */
  public List<SearchIndex> getAllIndexes() {
    return block(asyncIndexManager.getAllIndexes());
  }

  /**
   * Retrieves the number of documents that have been indexed for an index.
   *
   * @param name the name of the search index.
   * @return the number of indexed documents.
   */
  public long getIndexedDocumentsCount(final String name) {
    return block(asyncIndexManager.getIndexedDocumentsCount(name));
  }

  /**
   * Allows to see how a document is analyzed against a specific index.
   *
   * @param name the name of the search index.
   * @param document the document to be analyzed.
   * @return the analyzed sections for the document.
   */
  public List<JsonObject> analyzeDocument(final String name, final JsonObject document) {
    return block(asyncIndexManager.analyzeDocument(name, document));
  }

  /**
   * Creates, or updates, an index.
   *
   * @param index the index definition including name and settings.
   */
  public void upsertIndex(final SearchIndex index) {
    block(asyncIndexManager.upsertIndex(index));
  }

  /**
   * Drops an index.
   *
   * @param name the name of the search index.
   */
  public void dropIndex(final String name) {
    block(asyncIndexManager.dropIndex(name));
  }

  /**
   * Pauses updates and maintenance for an index.
   *
   * @param name the name of the search index.
   */
  public void pauseIngest(final String name) {
    block(asyncIndexManager.pauseIngest(name));
  }

  /**
   * Resumes updates and maintenance for an index.
   *
   * @param name the name of the search index.
   */
  public void resumeIngest(final String name) {
    block(asyncIndexManager.resumeIngest(name));
  }

  /**
   * Allows querying against an index.
   *
   * @param name the name of the search index.
   */
  public void allowQuerying(final String name) {
    block(asyncIndexManager.allowQuerying(name));
  }

  /**
   * Disallows querying against an index.
   *
   * @param name the name of the search index.
   */
  public void disallowQuerying(final String name) {
    block(asyncIndexManager.disallowQuerying(name));
  }

  /**
   * Freeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   */
  public void freezePlan(final String name) {
    block(asyncIndexManager.freezePlan(name));
  }

  /**
   * Unfreeze the assignment of index partitions to nodes.
   *
   * @param name the name of the search index.
   */
  public void unfreezePlan(final String name) {
    block(asyncIndexManager.unfreezePlan(name));
  }

}