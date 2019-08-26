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

import com.couchbase.client.core.annotation.Stability;

import java.util.Map;

import static com.couchbase.client.java.AsyncUtils.block;

@Stability.Volatile
public class BucketManager {

  private final AsyncBucketManager async;

  public BucketManager(final AsyncBucketManager async) {
    this.async = async;
  }

  public void createBucket(final BucketSettings settings) {
    block(async.createBucket(settings));
  }

  public void createBucket(final BucketSettings settings, final CreateBucketOptions options) {
    block(async.createBucket(settings, options));
  }

  public void updateBucket(final BucketSettings settings) {
    block(async.updateBucket(settings));
  }

  public void updateBucket(final BucketSettings settings, final UpdateBucketOptions options) {
    block(async.updateBucket(settings, options));
  }

  public void dropBucket(final String bucketName) {
    block(async.dropBucket(bucketName));
  }

  public void dropBucket(final String bucketName, final DropBucketOptions options) {
    block(async.dropBucket(bucketName, options));
  }

  public BucketSettings getBucket(final String bucketName) {
    return block(async.getBucket(bucketName));
  }

  public BucketSettings getBucket(final String bucketName, final GetBucketOptions options) {
    return block(async.getBucket(bucketName, options));
  }

  public Map<String, BucketSettings> getAllBuckets() {
    return block(async.getAllBuckets());
  }

  public Map<String, BucketSettings> getAllBuckets(final GetAllBucketOptions options) {
    return block(async.getAllBuckets(options));
  }

  public void flushBucket(final String bucketName) {
    block(async.flushBucket(bucketName));
  }

  public void flushBucket(final String bucketName, final FlushBucketOptions options) {
    block(async.flushBucket(bucketName, options));
  }

}
