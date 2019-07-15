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

  public void create(final BucketSettings settings) {
    block(async.create(settings));
  }

  public void create(final BucketSettings settings, final CreateBucketOptions options) {
    block(async.create(settings, options));
  }

  public void upsert(final BucketSettings settings) {
    block(async.upsert(settings));
  }

  public void upsert(final BucketSettings settings, final UpsertBucketOptions options) {
    block(async.upsert(settings, options));
  }

  public void drop(final String bucketName) {
    block(async.drop(bucketName));
  }

  public void drop(final String bucketName, final DropBucketOptions options) {
    block(async.drop(bucketName, options));
  }

  public BucketSettings get(final String bucketName) {
    return block(async.get(bucketName));
  }

  public BucketSettings get(final String bucketName, final GetBucketOptions options) {
    return block(async.get(bucketName, options));
  }

  public Map<String, BucketSettings> getAll() {
    return block(async.getAll());
  }

  public Map<String, BucketSettings> getAll(final GetAllBucketOptions options) {
    return block(async.getAll(options));
  }

  public void flush(final String bucketName) {
    block(async.flush(bucketName));
  }

  public void flush(final String bucketName, final FlushBucketOptions options) {
    block(async.flush(bucketName, options));
  }

}
