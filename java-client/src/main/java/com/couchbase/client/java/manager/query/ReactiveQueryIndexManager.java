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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

import static com.couchbase.client.core.Reactor.toFlux;
import static com.couchbase.client.core.Reactor.toMono;
import static java.util.Objects.requireNonNull;

public class ReactiveQueryIndexManager {
  private final AsyncQueryIndexManager async;

  public ReactiveQueryIndexManager(AsyncQueryIndexManager async) {
    this.async = requireNonNull(async);
  }

  public Mono<Void> createIndex(String bucketName, String indexName, Collection<String> fields) {
    return Mono.defer(() -> toMono(() -> async.createIndex(bucketName, indexName, fields)));
  }

  public Mono<Void> createIndex(String bucketName, String indexName, Collection<String> fields, CreateQueryIndexOptions options) {
    return Mono.defer(() -> toMono(() -> async.createIndex(bucketName, indexName, fields, options)));
  }

  public Mono<Void> createPrimaryIndex(String bucketName) {
    return Mono.defer(() -> toMono(() -> async.createPrimaryIndex(bucketName)));
  }

  public Mono<Void> createPrimaryIndex(String bucketName, CreatePrimaryQueryIndexOptions options) {
    return Mono.defer(() -> toMono(() -> async.createPrimaryIndex(bucketName, options)));
  }

  public Flux<QueryIndex> getAllIndexes(String bucketName) {
    return Flux.defer(() -> toFlux(() -> async.getAllIndexes(bucketName)));
  }

  public Flux<QueryIndex> getAllIndexes(String bucketName, GetAllQueryIndexesOptions options) {
    return Flux.defer(() -> toFlux(() -> async.getAllIndexes(bucketName, options)));
  }

  public Mono<Void> dropPrimaryIndex(String bucketName) {
    return Mono.defer(() -> toMono(() -> async.dropPrimaryIndex(bucketName)));
  }

  public Mono<Void> dropPrimaryIndex(String bucketName, DropPrimaryQueryIndexOptions options) {
    return Mono.defer(() -> toMono(() -> async.dropPrimaryIndex(bucketName, options)));
  }

  public Mono<Void> dropIndex(String bucketName, String indexName) {
    return Mono.defer(() -> toMono(() -> async.dropIndex(bucketName, indexName)));
  }

  public Mono<Void> dropIndex(String bucketName, String indexName, DropQueryIndexOptions options) {
    return Mono.defer(() -> toMono(() -> async.dropIndex(bucketName, indexName, options)));
  }

  public Mono<Void> buildDeferredIndexes(String bucketName) {
    return Mono.defer(() -> toMono(() -> async.buildDeferredIndexes(bucketName)));
  }

  public Mono<Void> buildDeferredIndexes(String bucketName, BuildQueryIndexOptions options) {
    return Mono.defer(() -> toMono(() -> async.buildDeferredIndexes(bucketName, options)));
  }

  public Mono<Void> watchIndexes(String bucketName, Collection<String> indexNames, Duration timeout) {
    return Mono.defer(() -> toMono(() -> async.watchIndexes(bucketName, indexNames, timeout)));
  }

  public Mono<Void> watchIndexes(String bucketName, Collection<String> indexNames, Duration timeout, WatchQueryIndexesOptions options) {
    return Mono.defer(() -> toMono(() -> async.watchIndexes(bucketName, indexNames, timeout, options)));
  }
}
