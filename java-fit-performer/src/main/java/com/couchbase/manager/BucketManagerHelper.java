/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.manager;

// [skip:<3.2.4]

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketSettings;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.CreateBucketOptions;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.CreateBucketSettings;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.DropBucketOptions;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.FlushBucketOptions;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetAllBucketsOptions;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetBucketOptions;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.StorageBackend;
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.UpdateBucketOptions;
import com.couchbase.client.protocol.shared.Durability;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.JavaSdkCommandExecutor.setSuccess;


public class BucketManagerHelper {

  private BucketManagerHelper() {
  }

  public static void handleBucketManger(Cluster cluster,
                                        ConcurrentHashMap<String, RequestSpan> spans,
                                        com.couchbase.client.protocol.sdk.Command command,
                                        Result.Builder result) {

    var bm = command.getClusterCommand().getBucketManager();
    var start = System.nanoTime();
    if (bm.hasGetBucket()) {
      var request = bm.getGetBucket();
      var bucketName = request.getBucketName();
      com.couchbase.client.java.manager.bucket.BucketSettings response;

      if (!request.hasOptions()) {
        response = cluster.buckets().getBucket(bucketName);
      } else {
        var options = createGetBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().getBucket(bucketName, options);
      }
      populateResult(start, result, response);
    } else if (bm.hasCreateBucket()) {
      var request = bm.getCreateBucket();

      if (!request.hasOptions()) {
        cluster.buckets().createBucket(createBucketSettings(request.getSettings(), spans));
      } else {
        var options = createBucketOptions(request.getOptions(), spans);
        cluster.buckets().createBucket(createBucketSettings(request.getSettings(), spans), options);
      }
      populateResult(start, result, null);
    } else if (bm.hasUpdateBucket()) {
      var request = bm.getUpdateBucket();

      if (!request.hasOptions()) {
        cluster.buckets().updateBucket(updateBucketSettings(request.getSettings(), spans));
      } else {
        var options = updateBucketOptions(request.getOptions(), spans);
        cluster.buckets().updateBucket(updateBucketSettings(request.getSettings(), spans), options);
      }
      populateResult(start, result, null);
    } else if (bm.hasDropBucket()) {
      var request = bm.getDropBucket();

      if (!request.hasOptions()) {
        cluster.buckets().dropBucket(request.getBucketName());
      } else {
        var options = dropBucketOptions(request.getOptions(), spans);
        cluster.buckets().dropBucket(request.getBucketName(), options);
      }
      populateResult(start, result, null);
    } else if (bm.hasFlushBucket()) {
      var request = bm.getFlushBucket();
      if (!request.hasOptions()) {
        cluster.buckets().flushBucket(request.getBucketName());
      } else {
        var options = flushBucketOptions(request.getOptions(), spans);
        cluster.buckets().flushBucket(request.getBucketName(), options);
      }
      populateResult(start, result, null);
    } else if (bm.hasGetAllBuckets()) {
      var request = bm.getGetAllBuckets();
      Map<String, com.couchbase.client.java.manager.bucket.BucketSettings> response;

      if (!request.hasOptions()) {
        response = cluster.buckets().getAllBuckets();
      } else {
        var options = createGetAllBucketsOptions(request.getOptions(), spans);
        response = cluster.buckets().getAllBuckets(options);
      }
      response.forEach((name, settings) ->
              populateResult(start, result, settings));
    } else {
      throw new UnsupportedOperationException(command.toString());
    }
  }

  public static Mono<Result> handleBucketManagerReactive(ReactiveCluster cluster,
                                                         ConcurrentHashMap<String, RequestSpan> spans,
                                                         com.couchbase.client.protocol.sdk.Command command,
                                                         Result.Builder result) {

    long start = System.currentTimeMillis();
    var bm = command.getClusterCommand().getBucketManager();
    if (bm.hasGetBucket()) {
      Mono<com.couchbase.client.java.manager.bucket.BucketSettings> response;
      var request = bm.getGetBucket();
      var bucketName = request.getBucketName();
      if (!request.hasOptions()) {
        response = cluster.buckets().getBucket(bucketName);
      } else {
        var options = createGetBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().getBucket(bucketName, options);
      }
      return response.map(rr -> {
        populateResult(start, result, rr);
        return result.build();
      });

    } else if (bm.hasCreateBucket()) {
      var request = bm.getCreateBucket();
      Mono<Void> response;

      if (!request.hasOptions()) {
        response = cluster.buckets().createBucket(createBucketSettings(request.getSettings(), spans));
      } else {
        var options = createBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().createBucket(createBucketSettings(request.getSettings(), spans), options);
      }
      return response.then(Mono.fromCallable(() -> {
        populateResult(start, result, null);
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (bm.hasUpdateBucket()) {
      var request = bm.getUpdateBucket();
      Mono<Void> response;

      if (!request.hasOptions()) {
        response = cluster.buckets().updateBucket(updateBucketSettings(request.getSettings(), spans));
      } else {
        var options = updateBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().updateBucket(updateBucketSettings(request.getSettings(), spans), options);
      }
      return response.then(Mono.fromCallable(() -> {
        populateResult(start, result, null);
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (bm.hasDropBucket()) {
      var request = bm.getDropBucket();
      Mono<Void> response;

      if (!request.hasOptions()) {
        response = cluster.buckets().dropBucket(request.getBucketName());
      } else {
        var options = dropBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().dropBucket(request.getBucketName(), options);
      }
      return response.then(Mono.fromCallable(() -> {
        populateResult(start, result, null);
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (bm.hasFlushBucket()) {
      var request = bm.getFlushBucket();
      Mono<Void> response;

      if (!request.hasOptions()) {
        response = cluster.buckets().flushBucket(request.getBucketName());
      } else {
        var options = flushBucketOptions(request.getOptions(), spans);
        response = cluster.buckets().flushBucket(request.getBucketName(), options);
      }
      return response.then(Mono.fromCallable(() -> {
        populateResult(start, result, null);
        result.setElapsedNanos(System.nanoTime() - start);
        setSuccess(result);
        return result.build();
      }));
    } else if (bm.hasGetAllBuckets()) {
      Mono<Map<String, com.couchbase.client.java.manager.bucket.BucketSettings>> response;
      var request = bm.getGetAllBuckets();
      if (!request.hasOptions()) {
        response = cluster.buckets().getAllBuckets();
      } else {
        var options = createGetAllBucketsOptions(request.getOptions(), spans);
        response = cluster.buckets().getAllBuckets(options);
      }
      return response.map(rr -> {
        rr.forEach((name, settings) ->
                populateResult(start, result, settings));
        return result.build();
      });

    } else {
      return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
    }

  }

  public static void populateResult(long start, Result.Builder result, com.couchbase.client.java.manager.bucket.BucketSettings response) {
    var builder = BucketSettings.newBuilder();
    result.setElapsedNanos(System.nanoTime() - start);
    if (response == null) {
      result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder().setSuccess(true));
    } else {

      builder.setRamQuotaMB(response.ramQuotaMB());
      builder.setFlushEnabled(response.flushEnabled());
      builder.setNumReplicas(response.numReplicas());
      builder.setReplicaIndexes(response.replicaIndexes());

      if (response.name() != null) {
        builder.setName(response.name());
      }
      if (response.storageBackend() != null) {
        builder.setStorageBackend(StorageBackend.valueOf(response.storageBackend().toString().toUpperCase()));
      }
      if (response.bucketType() != null) {
        builder.setBucketType(BucketType.valueOf(response.bucketType().toString()));
      }
      if (response.compressionMode() != null) {
        builder.setCompressionMode(CompressionMode.valueOf(response.compressionMode().toString()));
      }
      if (response.evictionPolicy() != null) {
        builder.setEvictionPolicy(EvictionPolicyType.valueOf(response.evictionPolicy().toString()));
      }
      if (response.minimumDurabilityLevel() != null) {
        builder.setMinimumDurabilityLevel(Durability.valueOf(response.minimumDurabilityLevel().toString()));
      }
      if (response.maxExpiry() != null) {
        builder.setMaxExpirySeconds((int) response.maxExpiry().toSeconds());
      }
      // [if:3.4.12]
      if (response.historyRetentionBytes() != null) {
        builder.setHistoryRetentionBytes(response.historyRetentionBytes());
      }
      if (response.historyRetentionDuration() != null) {
        builder.setHistoryRetentionSeconds(response.historyRetentionDuration().toSeconds());
      }
      if (response.historyRetentionCollectionDefault() != null) {
        builder.setHistoryRetentionCollectionDefault(response.historyRetentionCollectionDefault());
      }
      // [end]

      result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder().setSuccess(true)
              .setBucketManagerResult(com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result.newBuilder()
                      .setBucketSettings(builder)));
    }
  }

  private static com.couchbase.client.java.manager.bucket.GetBucketOptions createGetBucketOptions(GetBucketOptions getBucketOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.bucket.GetBucketOptions.getBucketOptions();

    if (getBucketOptions.hasTimeoutMsecs()) options.timeout(java.time.Duration.ofMillis(getBucketOptions.getTimeoutMsecs()));
    if (getBucketOptions.hasParentSpanId()) options.parentSpan(spans.get(getBucketOptions.getParentSpanId()));

    return options;
  }

  private static com.couchbase.client.java.manager.bucket.CreateBucketOptions createBucketOptions(CreateBucketOptions createBucketOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.bucket.CreateBucketOptions.createBucketOptions();

    if (createBucketOptions.hasTimeoutMsecs()) options.timeout(java.time.Duration.ofMillis(createBucketOptions.getTimeoutMsecs()));
    if (createBucketOptions.hasParentSpanId()) options.parentSpan(spans.get(createBucketOptions.getParentSpanId()));

    return options;
  }

  private static com.couchbase.client.java.manager.bucket.BucketSettings createBucketSettings(CreateBucketSettings createBucketSettings, ConcurrentHashMap<String, RequestSpan> spans) {
    try {
      var bucketSettings = createBucketSettings.getSettings();

      var settings = com.couchbase.client.java.manager.bucket.BucketSettings.create(bucketSettings.getName());

      // Conflict resolution type \'custom\' is supported only with developer preview enabled
      if (createBucketSettings.hasConflictResolutionType()) {
        settings.conflictResolutionType(com.couchbase.client.java.manager.bucket.ConflictResolutionType.valueOf(createBucketSettings.getConflictResolutionType().name()));
      }
      settings.ramQuotaMB(bucketSettings.getRamQuotaMB());
      if (bucketSettings.hasBucketType()) {
        settings.bucketType(com.couchbase.client.java.manager.bucket.BucketType.valueOf(bucketSettings.getBucketType().name()));
      }
      if (bucketSettings.hasCompressionMode()) {
        settings.compressionMode(com.couchbase.client.java.manager.bucket.CompressionMode.valueOf(bucketSettings.getCompressionMode().name()));
      }
      if (bucketSettings.hasEvictionPolicy()) {
        settings.evictionPolicy(com.couchbase.client.java.manager.bucket.EvictionPolicyType.valueOf(bucketSettings.getEvictionPolicy().name()));
      }
      if (bucketSettings.hasStorageBackend()) {
        settings.storageBackend(com.couchbase.client.java.manager.bucket.StorageBackend.of(bucketSettings.getStorageBackend().name().toLowerCase()));
      }
      // [if:3.4.12]
      // "History Retention can only used with Magma"
      if (bucketSettings.hasHistoryRetentionBytes()) {
        settings.historyRetentionBytes(bucketSettings.getHistoryRetentionBytes());
      }
      if (bucketSettings.hasHistoryRetentionSeconds()) {
        settings.historyRetentionDuration(Duration.ofSeconds(bucketSettings.getHistoryRetentionSeconds()));
      }
      if (bucketSettings.hasHistoryRetentionCollectionDefault()) {
        settings.historyRetentionCollectionDefault(bucketSettings.getHistoryRetentionCollectionDefault());
      }
      // [end]
      if (bucketSettings.hasFlushEnabled() && bucketSettings.getFlushEnabled()) {
        settings.flushEnabled(bucketSettings.getFlushEnabled());
      }
      if (bucketSettings.hasMaxExpirySeconds()) {
        settings.maxExpiry(Duration.ofSeconds(bucketSettings.getMaxExpirySeconds()));
      }
      if (bucketSettings.hasMinimumDurabilityLevel()) {
        settings.minimumDurabilityLevel(DurabilityLevel.valueOf(bucketSettings.getMinimumDurabilityLevel().name()));
      }
      if (bucketSettings.hasNumReplicas()) {
        settings.numReplicas(bucketSettings.getNumReplicas());
      }
      if (bucketSettings.hasReplicaIndexes()) {
        settings.replicaIndexes(bucketSettings.getReplicaIndexes());
      }
      return settings;
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }


  private static com.couchbase.client.java.manager.bucket.UpdateBucketOptions updateBucketOptions(UpdateBucketOptions updateBucketOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.bucket.UpdateBucketOptions.updateBucketOptions();

    if (updateBucketOptions.hasTimeoutMsecs()) options.timeout(java.time.Duration.ofMillis(updateBucketOptions.getTimeoutMsecs()));
    if (updateBucketOptions.hasParentSpanId()) options.parentSpan(spans.get(updateBucketOptions.getParentSpanId()));

    return options;
  }

  private static com.couchbase.client.java.manager.bucket.BucketSettings updateBucketSettings(BucketSettings bucketSettings, ConcurrentHashMap<String, RequestSpan> spans) {
    try {

      var settings = com.couchbase.client.java.manager.bucket.BucketSettings.create(bucketSettings.getName());

      if (bucketSettings.getRamQuotaMB() != 0) {
        settings.ramQuotaMB(bucketSettings.getRamQuotaMB());
      }
      if (bucketSettings.hasBucketType()) {
        settings.bucketType(com.couchbase.client.java.manager.bucket.BucketType.valueOf(bucketSettings.getBucketType().name()));
      }
      if (bucketSettings.hasCompressionMode()) {
        settings.compressionMode(com.couchbase.client.java.manager.bucket.CompressionMode.valueOf(bucketSettings.getCompressionMode().name()));
      }
      if (bucketSettings.hasEvictionPolicy()) {
        settings.evictionPolicy(com.couchbase.client.java.manager.bucket.EvictionPolicyType.valueOf(bucketSettings.getEvictionPolicy().name()));
      }
      if (bucketSettings.hasStorageBackend()) {
        settings.storageBackend(com.couchbase.client.java.manager.bucket.StorageBackend.of(bucketSettings.getStorageBackend().name().toLowerCase()));
      }
      // [if:3.4.12]
      // "History Retention can only used with Magma"
      if (bucketSettings.hasHistoryRetentionBytes()) {
        settings.historyRetentionBytes(bucketSettings.getHistoryRetentionBytes());
      }
      if (bucketSettings.hasHistoryRetentionSeconds()) {
        settings.historyRetentionDuration(Duration.ofSeconds(bucketSettings.getHistoryRetentionSeconds()));
      }
      if (bucketSettings.hasHistoryRetentionCollectionDefault()) {
        settings.historyRetentionCollectionDefault(bucketSettings.getHistoryRetentionCollectionDefault());
      }
      // [end]
      if (bucketSettings.hasFlushEnabled() && bucketSettings.getFlushEnabled()) {
        settings.flushEnabled(bucketSettings.getFlushEnabled());
      }
      if (bucketSettings.hasMaxExpirySeconds()) {
        settings.maxExpiry(Duration.ofSeconds(bucketSettings.getMaxExpirySeconds()));
      }
      if (bucketSettings.hasMinimumDurabilityLevel()) {
        settings.minimumDurabilityLevel(DurabilityLevel.valueOf(bucketSettings.getMinimumDurabilityLevel().name()));
      }
      if (bucketSettings.hasNumReplicas()) {
        settings.numReplicas(bucketSettings.getNumReplicas());
      }
      if (bucketSettings.hasReplicaIndexes()) {
        settings.replicaIndexes(bucketSettings.getReplicaIndexes());
      }
      System.err.println("update settings: " + settings);
      return settings;
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static com.couchbase.client.java.manager.bucket.FlushBucketOptions flushBucketOptions(FlushBucketOptions flushBucketOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.bucket.FlushBucketOptions.flushBucketOptions();
    if (flushBucketOptions.hasTimeoutMsecs()) options.timeout(java.time.Duration.ofMillis(flushBucketOptions.getTimeoutMsecs()));
    if (flushBucketOptions.hasParentSpanId()) options.parentSpan(spans.get(flushBucketOptions.getParentSpanId()));
    return options;
  }

  private static com.couchbase.client.java.manager.bucket.DropBucketOptions dropBucketOptions(DropBucketOptions dropBucketOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.bucket.DropBucketOptions.dropBucketOptions();
    if (dropBucketOptions.hasTimeoutMsecs()) options.timeout(java.time.Duration.ofMillis(dropBucketOptions.getTimeoutMsecs()));
    if (dropBucketOptions.hasParentSpanId()) options.parentSpan(spans.get(dropBucketOptions.getParentSpanId()));
    return options;
  }

  private static com.couchbase.client.java.manager.bucket.GetAllBucketOptions createGetAllBucketsOptions(GetAllBucketsOptions getAllBucketsOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.bucket.GetAllBucketOptions.getAllBucketOptions();
    if (getAllBucketsOptions.hasTimeoutMsecs()) options.timeout(java.time.Duration.ofMillis(getAllBucketsOptions.getTimeoutMsecs()));
    if (getAllBucketsOptions.hasParentSpanId()) options.parentSpan(spans.get(getAllBucketsOptions.getParentSpanId()));
    return options;
  }

}
