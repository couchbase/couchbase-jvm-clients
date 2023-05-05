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

package com.couchbase.client.core.protostellar.manager;

import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.BucketType;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.manager.CoreBucketManagerOps;
import com.couchbase.client.core.manager.bucket.CoreBucketSettings;
import com.couchbase.client.core.manager.bucket.CoreCompressionMode;
import com.couchbase.client.core.manager.bucket.CoreCreateBucketSettings;
import com.couchbase.client.core.manager.bucket.CoreEvictionPolicyType;
import com.couchbase.client.core.manager.bucket.CoreStorageBackend;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.protostellar.CoreProtostellarAccessors;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.protostellar.admin.bucket.v1.CreateBucketRequest;
import com.couchbase.client.protostellar.admin.bucket.v1.DeleteBucketRequest;
import com.couchbase.client.protostellar.admin.bucket.v1.ListBucketsRequest;
import com.couchbase.client.protostellar.admin.bucket.v1.ListBucketsResponse;
import com.couchbase.client.protostellar.admin.bucket.v1.UpdateBucketRequest;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.incompatibleProtostellar;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ProtostellarCoreBucketManager implements CoreBucketManagerOps {
  private final CoreProtostellar core;

  public ProtostellarCoreBucketManager(CoreProtostellar core) {
    this.core = requireNonNull(core);
  }

  @Override
  public CompletableFuture<Void> createBucket(CoreBucketSettings settings, @Nullable CoreCreateBucketSettings createSpecificSettings, CoreCommonOptions options) {
    ProtostellarRequest<CreateBucketRequest> request = ProtostellarCoreBucketManagerRequests.createBucketRequest(core, settings, createSpecificSettings, options);
    return CoreProtostellarAccessors.async(core,
        request,
        (endpoint) -> endpoint.bucketAdminStub().withDeadline(request.deadline()).createBucket(request.request()),
        (response) -> null)
      .thenApply(obj -> null);
  }

  @Override
  public CompletableFuture<Void> updateBucket(CoreBucketSettings settings, CoreCommonOptions options) {
    ProtostellarRequest<UpdateBucketRequest> request = ProtostellarCoreBucketManagerRequests.updateBucketRequest(core, settings, options);
    return CoreProtostellarAccessors.async(core,
        request,
        (endpoint) -> endpoint.bucketAdminStub().withDeadline(request.deadline()).updateBucket(request.request()),
        (response) -> null)
      .thenApply(obj -> null);
  }

  @Override
  public CompletableFuture<Void> dropBucket(String bucketName, CoreCommonOptions options) {
    ProtostellarRequest<DeleteBucketRequest> request = ProtostellarCoreBucketManagerRequests.deleteBucketRequest(core, bucketName, options);
    return CoreProtostellarAccessors.async(core,
        request,
        (endpoint) -> endpoint.bucketAdminStub().withDeadline(request.deadline()).deleteBucket(request.request()),
        (response) -> null)
      .thenApply(obj -> null);
  }

  @Override
  public CompletableFuture<CoreBucketSettings> getBucket(String bucketName, CoreCommonOptions options) {
    return getAllBuckets(options)
      .thenApply(buckets -> {
        if (!buckets.containsKey(bucketName)) {
          throw new BucketNotFoundException(bucketName);
        }
        return buckets.get(bucketName);
      });
  }

  @Override
  public CompletableFuture<Map<String, CoreBucketSettings>> getAllBuckets(CoreCommonOptions options) {
    ProtostellarRequest<ListBucketsRequest> request = ProtostellarCoreBucketManagerRequests.listBucketsRequest(core, options);
    return CoreProtostellarAccessors.async(core,
      request,
      (endpoint) -> endpoint.bucketAdminStub().withDeadline(request.deadline()).listBuckets(request.request()),
      (response) -> {
        Map<String, CoreBucketSettings> out = new HashMap<>();
        response.getBucketsList().forEach(bucket -> out.put(bucket.getBucketName(), extracted(bucket)));
        return out;
      }).thenApply(v -> v);
  }

  private static CoreBucketSettings extracted(ListBucketsResponse.Bucket bucket) {
    return new CoreBucketSettings() {
      @Override
      public String name() {
        return bucket.getBucketName();
      }

      @Override
      public Boolean flushEnabled() {
        return bucket.getFlushEnabled();
      }

      @Override
      public long ramQuotaMB() {
        return bucket.getRamQuotaBytes() / 1_000_000; // todo
      }

      @Override
      public Integer numReplicas() {
        return bucket.getNumReplicas();
      }

      @Override
      public Boolean replicaIndexes() {
        return bucket.getReplicaIndexes();
      }

      @Override
      public BucketType bucketType() {
        switch (bucket.getBucketType()) {
          case BUCKET_TYPE_COUCHBASE:
            return BucketType.COUCHBASE;
          case BUCKET_TYPE_MEMCACHED:
            return BucketType.MEMCACHED;
          case BUCKET_TYPE_EPHEMERAL:
            return BucketType.EPHEMERAL;
          default:
            throw incompatibleProtostellar("Unknown bucket type " + bucket.getBucketType());
        }
      }

      @Override
      public CoreEvictionPolicyType evictionPolicy() {
        switch (bucket.getEvictionMode()) {
          case EVICTION_MODE_FULL:
            return CoreEvictionPolicyType.FULL;
          case EVICTION_MODE_NOT_RECENTLY_USED:
            return CoreEvictionPolicyType.NOT_RECENTLY_USED;
          case EVICTION_MODE_VALUE_ONLY:
            return CoreEvictionPolicyType.VALUE_ONLY;
          case EVICTION_MODE_NONE:
            return CoreEvictionPolicyType.NO_EVICTION;
          default:
            throw incompatibleProtostellar("Unknown eviction policy " + bucket.getEvictionMode());
        }
      }

      @Override
      public Duration maxExpiry() {
        return Duration.ofSeconds(bucket.getMaxExpirySecs());
      }

      @Override
      public CoreCompressionMode compressionMode() {
        switch (bucket.getCompressionMode()) {
          case COMPRESSION_MODE_OFF:
            return CoreCompressionMode.OFF;
          case COMPRESSION_MODE_PASSIVE:
            return CoreCompressionMode.PASSIVE;
          case COMPRESSION_MODE_ACTIVE:
            return CoreCompressionMode.ACTIVE;
          default:
            throw incompatibleProtostellar("Unknown compression mode " + bucket.getCompressionMode());
        }
      }

      @Override
      public DurabilityLevel minimumDurabilityLevel() {
        switch (bucket.getMinimumDurabilityLevel()) {
          case DURABILITY_LEVEL_MAJORITY:
            return DurabilityLevel.MAJORITY;
          case DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE:
            return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
          case DURABILITY_LEVEL_PERSIST_TO_MAJORITY:
            return DurabilityLevel.PERSIST_TO_MAJORITY;
          case UNRECOGNIZED:
          default:
            throw incompatibleProtostellar("Unknown min durability level " + bucket.getMinimumDurabilityLevel());
        }
      }

      @Override
      public CoreStorageBackend storageBackend() {
        switch (bucket.getStorageBackend()) {
          case STORAGE_BACKEND_COUCHSTORE:
            return CoreStorageBackend.COUCHSTORE;
          case STORAGE_BACKEND_MAGMA:
            return CoreStorageBackend.MAGMA;
          default:
            throw incompatibleProtostellar("Unknown storage backend " + bucket.getStorageBackend());
        }
      }
    };
  }

  @Override
  public CompletableFuture<Void> flushBucket(String bucketName, CoreCommonOptions options) {
    throw unsupportedInProtostellar("flushing buckets");
  }
}
