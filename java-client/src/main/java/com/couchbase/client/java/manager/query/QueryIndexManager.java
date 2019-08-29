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

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static com.couchbase.client.java.AsyncUtils.block;
import static java.util.Objects.requireNonNull;

public class QueryIndexManager {
  private final AsyncQueryIndexManager async;
  private final ReactiveQueryIndexManager reactive;

  public QueryIndexManager(AsyncQueryIndexManager async) {
    this.async = requireNonNull(async);
    this.reactive = new ReactiveQueryIndexManager(async);
  }

  public AsyncQueryIndexManager async() {
    return async;
  }

  public ReactiveQueryIndexManager reactive() {
    return reactive;
  }

  public void createPrimaryIndex(String bucketName) {
    block(async.createPrimaryIndex(bucketName));
  }

  public void createPrimaryIndex(String bucketName, CreatePrimaryQueryIndexOptions options) {
    block(async.createPrimaryIndex(bucketName, options));
  }

  public void createIndex(String bucketName, String indexName, Collection<String> fields) {
    block(async.createIndex(bucketName, indexName, fields));
  }

  public void createIndex(String bucketName, String indexName, Collection<String> fields, CreateQueryIndexOptions options) {
    block(async.createIndex(bucketName, indexName, fields, options));
  }

  public List<QueryIndex> getAllIndexes(String bucketName) {
    return block(async.getAllIndexes(bucketName));
  }

  public List<QueryIndex> getAllIndexes(String bucketName, GetAllQueryIndexesOptions options) {
    return block(async.getAllIndexes(bucketName, options));
  }

  public void dropPrimaryIndex(String bucketName) {
    block(async.dropPrimaryIndex(bucketName));
  }

  public void dropPrimaryIndex(String bucketName, DropPrimaryQueryIndexOptions options) {
    block(async.dropPrimaryIndex(bucketName, options));
  }

  public void dropIndex(String bucketName, String indexName) {
    block(async.dropIndex(bucketName, indexName));
  }

  public void dropIndex(String bucketName, String indexName, DropQueryIndexOptions options) {
    block(async.dropIndex(bucketName, indexName, options));
  }

  public void buildDeferredIndexes(String bucketName) {
    block(async.buildDeferredIndexes(bucketName));
  }

  public void buildDeferredIndexes(String bucketName, BuildQueryIndexOptions options) {
    block(async.buildDeferredIndexes(bucketName, options));
  }

  public void watchIndexes(String bucketName, Collection<String> indexNames, Duration timeout) {
    block(async.watchIndexes(bucketName, indexNames, timeout));
  }

  public void watchIndexes(String bucketName, Collection<String> indexNames, Duration timeout, WatchQueryIndexesOptions options) {
    block(async.watchIndexes(bucketName, indexNames, timeout, options));
  }
}
