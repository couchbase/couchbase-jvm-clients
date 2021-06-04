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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreBucketManager;
import com.couchbase.client.core.msg.kv.DurabilityLevel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.couchbase.client.core.util.CbCollections.transformValues;
import static com.couchbase.client.java.manager.bucket.BucketType.MEMCACHED;
import static com.couchbase.client.java.manager.bucket.CreateBucketOptions.createBucketOptions;
import static com.couchbase.client.java.manager.bucket.DropBucketOptions.dropBucketOptions;
import static com.couchbase.client.java.manager.bucket.FlushBucketOptions.flushBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetAllBucketOptions.getAllBucketOptions;
import static com.couchbase.client.java.manager.bucket.GetBucketOptions.getBucketOptions;
import static com.couchbase.client.java.manager.bucket.UpdateBucketOptions.updateBucketOptions;

@Stability.Volatile
public class AsyncBucketManager {
  private final CoreBucketManager coreBucketManager;

  public AsyncBucketManager(Core core) {
    this.coreBucketManager = new CoreBucketManager(core);
  }

  public CompletableFuture<Void> createBucket(BucketSettings settings) {
    return createBucket(settings, createBucketOptions());
  }

  public CompletableFuture<Void> createBucket(BucketSettings settings, CreateBucketOptions options) {
    return coreBucketManager.createBucket(toMap(settings), options.build());
  }

  public CompletableFuture<Void> updateBucket(BucketSettings settings) {
    return updateBucket(settings, updateBucketOptions());
  }

  public CompletableFuture<Void> updateBucket(BucketSettings settings, UpdateBucketOptions options) {
    return coreBucketManager.updateBucket(toMap(settings), options.build());
  }

  public CompletableFuture<Void> dropBucket(String bucketName) {
    return dropBucket(bucketName, dropBucketOptions());
  }

  public CompletableFuture<Void> dropBucket(String bucketName, DropBucketOptions options) {
    return coreBucketManager.dropBucket(bucketName, options.build());
  }

  public CompletableFuture<BucketSettings> getBucket(String bucketName) {
    return getBucket(bucketName, getBucketOptions());
  }

  public CompletableFuture<BucketSettings> getBucket(String bucketName, GetBucketOptions options) {
    return coreBucketManager.getBucket(bucketName, options.build())
        .thenApply(parseBucketSettings());
  }

  private static Function<byte[], BucketSettings> parseBucketSettings() {
    return bucketBytes -> {
      JsonNode tree = Mapper.decodeIntoTree(bucketBytes);
      return BucketSettings.create(tree);
    };
  }

  public CompletableFuture<Map<String, BucketSettings>> getAllBuckets() {
    return getAllBuckets(getAllBucketOptions());
  }

  public CompletableFuture<Map<String, BucketSettings>> getAllBuckets(GetAllBucketOptions options) {
    return coreBucketManager.getAllBuckets(options.build())
        .thenApply(bucketNameToBytes -> transformValues(bucketNameToBytes, parseBucketSettings()));
  }

  public CompletableFuture<Void> flushBucket(String bucketName) {
    return flushBucket(bucketName, flushBucketOptions());
  }

  public CompletableFuture<Void> flushBucket(String bucketName, FlushBucketOptions options) {
    return coreBucketManager.flushBucket(bucketName, options.build());
  }

  private Map<String, String> toMap(BucketSettings settings) {
    Map<String, String> params = new HashMap<>();

    params.put("ramQuotaMB", String.valueOf(settings.ramQuotaMB()));
    if (settings.bucketType() != MEMCACHED) {
      params.put("replicaNumber", String.valueOf(settings.numReplicas()));
    }
    params.put("flushEnabled", String.valueOf(settings.flushEnabled() ? 1 : 0));
    long maxTTL = settings.maxExpiry().getSeconds();
    // Do not send if it's been left at default, else will get an error on CE
    if (maxTTL != 0) {
      params.put("maxTTL", String.valueOf(maxTTL));
    }
    if (settings.evictionPolicy() != null) {
      // let server assign the default policy for this bucket type
      params.put("evictionPolicy", settings.evictionPolicy().alias());
    }
    // Do not send if it's been left at default, else will get an error on CE
    if (settings.compressionMode() != CompressionMode.PASSIVE) {
      params.put("compressionMode", settings.compressionMode().alias());
    }

    if (settings.minimumDurabilityLevel() != DurabilityLevel.NONE) {
      params.put("durabilityMinLevel", settings.minimumDurabilityLevel().encodeForManagementApi());
    }

    params.put("name", settings.name());
    params.put("bucketType", settings.bucketType().alias());
    params.put("conflictResolutionType", settings.conflictResolutionType().alias());
    if (settings.bucketType() != BucketType.EPHEMERAL) {
      params.put("replicaIndex", String.valueOf(settings.replicaIndexes() ? 1 : 0));
    }

    return params;
  }
}
