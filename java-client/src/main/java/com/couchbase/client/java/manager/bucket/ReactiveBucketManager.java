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

package com.couchbase.client.java.manager.bucket;

import reactor.core.publisher.Mono;

import java.util.Map;

import static com.couchbase.client.core.Reactor.toMono;
import static java.util.Objects.requireNonNull;

public class ReactiveBucketManager {

  private final AsyncBucketManager async;

  public ReactiveBucketManager(final AsyncBucketManager async) {
    this.async = requireNonNull(async);
  }

  public AsyncBucketManager async() {
    return async;
  }

  public Mono<Void> createBucket(final BucketSettings settings) {
    return toMono(() -> async.createBucket(settings));
  }

  public Mono<Void> createBucket(final BucketSettings settings, final CreateBucketOptions options) {
    return toMono(() -> async.createBucket(settings, options));
  }

  public Mono<Void> updateBucket(final BucketSettings settings) {
    return toMono(() -> async.updateBucket(settings));
  }

  public Mono<Void> updateBucket(final BucketSettings settings, final UpdateBucketOptions options) {
    return toMono(() -> async.updateBucket(settings, options));
  }

  public Mono<Void> dropBucket(final String bucketName) {
    return toMono(() -> async.dropBucket(bucketName));
  }

  public Mono<Void> dropBucket(final String bucketName, final DropBucketOptions options) {
    return toMono(() -> async.dropBucket(bucketName, options));
  }

  public Mono<BucketSettings> getBucket(final String bucketName) {
    return toMono(() -> async.getBucket(bucketName));
  }

  public Mono<BucketSettings> getBucket(final String bucketName, final GetBucketOptions options) {
    return toMono(() -> async.getBucket(bucketName, options));
  }

  public Mono<Map<String, BucketSettings>> getAllBuckets() {
    return toMono(() -> async.getAllBuckets());
  }

  public Mono<Map<String, BucketSettings>> getAllBuckets(final GetAllBucketOptions options) {
    return toMono(() -> async.getAllBuckets(options));
  }

  public Mono<Void> flushBucket(final String bucketName) {
    return toMono(() -> async.flushBucket(bucketName));
  }

  public Mono<Void> flushBucket(final String bucketName, final FlushBucketOptions options) {
    return toMono(() -> async.flushBucket(bucketName, options));
  }
}
