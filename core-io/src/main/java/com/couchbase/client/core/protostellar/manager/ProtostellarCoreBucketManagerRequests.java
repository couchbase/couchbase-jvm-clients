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
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.manager.bucket.CoreBucketSettings;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.protostellar.ProtostellarRequest;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.protostellar.admin.bucket.v1.BucketType;
import com.couchbase.client.protostellar.admin.bucket.v1.CompressionMode;
import com.couchbase.client.protostellar.admin.bucket.v1.ConflictResolutionType;
import com.couchbase.client.protostellar.admin.bucket.v1.CreateBucketRequest;
import com.couchbase.client.protostellar.admin.bucket.v1.DeleteBucketRequest;
import com.couchbase.client.protostellar.admin.bucket.v1.EvictionMode;
import com.couchbase.client.protostellar.admin.bucket.v1.ListBucketsRequest;
import com.couchbase.client.protostellar.admin.bucket.v1.StorageBackend;
import com.couchbase.client.protostellar.admin.bucket.v1.UpdateBucketRequest;
import com.couchbase.client.protostellar.kv.v1.DurabilityLevel;

import java.time.Duration;

import static com.couchbase.client.core.manager.bucket.CoreStorageBackend.COUCHSTORE;
import static com.couchbase.client.core.manager.bucket.CoreStorageBackend.MAGMA;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.createSpan;
import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedCurrentlyInProtostellar;

@Stability.Internal
public class ProtostellarCoreBucketManagerRequests {
  private ProtostellarCoreBucketManagerRequests() {
  }

  public static ProtostellarRequest<CreateBucketRequest> createBucketRequest(
    CoreProtostellar core,
    CoreBucketSettings settings,
    CoreCommonOptions opts
  ) {
    CreateBucketRequest.Builder request = CreateBucketRequest.newBuilder()
      .setBucketName(settings.name());

    if (settings.ramQuotaMB() != null) {
      request.setRamQuotaMb(settings.ramQuotaMB());
    }

    if (settings.bucketType() != null) {
      switch (settings.bucketType()) {
        case COUCHBASE:
          request.setBucketType(BucketType.BUCKET_TYPE_COUCHBASE);
          break;
        case EPHEMERAL:
          request.setBucketType(BucketType.BUCKET_TYPE_EPHEMERAL);
          break;
        case MEMCACHED:
          throw new FeatureNotAvailableException("Memcached buckets are not supported when using couchbase2");
        default:
          throw InvalidArgumentException.fromMessage("Unknown bucket type");
      }
    }

    if (settings.flushEnabled() != null) {
      request.setFlushEnabled(settings.flushEnabled());
    }

    if (settings.numReplicas() != null) {
      request.setNumReplicas(settings.numReplicas());
    }

    if (settings.replicaIndexes() != null) {
      request.setReplicaIndexes(settings.replicaIndexes());
    }

    if (settings.evictionPolicy() != null) {
      switch (settings.evictionPolicy()) {
        case FULL:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_FULL);
          break;
        case VALUE_ONLY:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_VALUE_ONLY);
          break;
        case NOT_RECENTLY_USED:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_NOT_RECENTLY_USED);
          break;
        case NO_EVICTION:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_NONE);
          break;
        default:
          throw InvalidArgumentException.fromMessage("Unknown eviction policy");
      }
    }

    if (settings.maxExpiry() != null) {
      request.setMaxExpirySecs((int) settings.maxExpiry().toMillis() / 1_000);
    }

    if (settings.compressionMode() != null) {
      switch (settings.compressionMode()) {
        case OFF:
          request.setCompressionMode(CompressionMode.COMPRESSION_MODE_OFF);
          break;
        case PASSIVE:
          request.setCompressionMode(CompressionMode.COMPRESSION_MODE_PASSIVE);
          break;
        case ACTIVE:
          request.setCompressionMode(CompressionMode.COMPRESSION_MODE_ACTIVE);
          break;
        default:
          throw InvalidArgumentException.fromMessage("Unknown compression mode");
      }
    }

    if (settings.minimumDurabilityLevel() != null) {
      switch (settings.minimumDurabilityLevel()) {
        case NONE:
          break;
        case MAJORITY:
          request.setMinimumDurabilityLevel(DurabilityLevel.DURABILITY_LEVEL_MAJORITY);
          break;
        case MAJORITY_AND_PERSIST_TO_ACTIVE:
          request.setMinimumDurabilityLevel(DurabilityLevel.DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE);
          break;
        case PERSIST_TO_MAJORITY:
          request.setMinimumDurabilityLevel(DurabilityLevel.DURABILITY_LEVEL_PERSIST_TO_MAJORITY);
          break;
        default:
          throw InvalidArgumentException.fromMessage("Unknown durability level");
      }
    }

    if (settings.storageBackend() != null) {
      if (settings.storageBackend() == COUCHSTORE) {
        request.setStorageBackend(StorageBackend.STORAGE_BACKEND_COUCHSTORE);
      } else if (settings.storageBackend() == MAGMA) {
        request.setStorageBackend(StorageBackend.STORAGE_BACKEND_MAGMA);
      } else {
        throw InvalidArgumentException.fromMessage("Unknown storage backend");
      }
    }

    if (settings.numVBuckets() != null) {
      // Requires https://jira.issues.couchbase.com/browse/ING-1036
      throw new UnsupportedOperationException("numVBuckets is not yet supported when using couchbase2");
    }

    if (settings.historyRetentionCollectionDefault() != null
      || settings.historyRetentionDuration() != null
      || settings.historyRetentionBytes() != null) {
      throw unsupportedCurrentlyInProtostellar();
    }

    if (settings.conflictResolutionType() != null) {
      switch (settings.conflictResolutionType()) {
        case TIMESTAMP:
          request.setConflictResolutionType(ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_TIMESTAMP);
          break;
        case SEQUENCE_NUMBER:
          request.setConflictResolutionType(ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER);
          break;
        case CUSTOM:
          request.setConflictResolutionType(ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_CUSTOM);
          break;
        default:
          throw InvalidArgumentException.fromMessage("Unknown conflict resolution type");
      }
    }

    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    return new ProtostellarRequest<>(request.build(),
      core,
      ServiceType.MANAGER,
      TracingIdentifiers.SPAN_REQUEST_MB_CREATE_BUCKET,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MB_CREATE_BUCKET, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0,
      null);
  }

  public static ProtostellarRequest<UpdateBucketRequest> updateBucketRequest(CoreProtostellar core,
                                                                             CoreBucketSettings settings,
                                                                             CoreCommonOptions opts) {
    UpdateBucketRequest.Builder request = UpdateBucketRequest.newBuilder()
      .setBucketName(settings.name());

    if (settings.ramQuotaMB() != null) {
      request.setRamQuotaMb(settings.ramQuotaMB());
    }

    if (settings.flushEnabled() != null) {
      request.setFlushEnabled(settings.flushEnabled());
    }

    if (settings.numReplicas() != null) {
      request.setNumReplicas(settings.numReplicas());
    }

    if (settings.evictionPolicy() != null) {
      switch (settings.evictionPolicy()) {
        case FULL:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_FULL);
          break;
        case VALUE_ONLY:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_VALUE_ONLY);
          break;
        case NOT_RECENTLY_USED:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_NOT_RECENTLY_USED);
          break;
        case NO_EVICTION:
          request.setEvictionMode(EvictionMode.EVICTION_MODE_NONE);
          break;
        default:
          throw InvalidArgumentException.fromMessage("Unknown eviction policy");
      }
    }

    if (settings.maxExpiry() != null) {
      request.setMaxExpirySecs((int) settings.maxExpiry().getSeconds());
    }

    if (settings.compressionMode() != null) {
      switch (settings.compressionMode()) {
        case OFF:
          request.setCompressionMode(CompressionMode.COMPRESSION_MODE_OFF);
          break;
        case PASSIVE:
          request.setCompressionMode(CompressionMode.COMPRESSION_MODE_PASSIVE);
          break;
        case ACTIVE:
          request.setCompressionMode(CompressionMode.COMPRESSION_MODE_ACTIVE);
          break;
        default:
          throw InvalidArgumentException.fromMessage("Unknown compression mode");
      }
    }

    if (settings.minimumDurabilityLevel() != null) {
      request.setMinimumDurabilityLevel(CoreProtostellarUtil.convert(settings.minimumDurabilityLevel()));
    }

    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    return new ProtostellarRequest<>(request.build(),
      core,
      ServiceType.MANAGER,
      TracingIdentifiers.SPAN_REQUEST_MB_UPDATE_BUCKET,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MB_UPDATE_BUCKET, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0,
      null);
  }

  public static ProtostellarRequest<DeleteBucketRequest> deleteBucketRequest(CoreProtostellar core,
                                                                             String bucketName,
                                                                             CoreCommonOptions opts) {
    DeleteBucketRequest request = DeleteBucketRequest.newBuilder()
      .setBucketName(bucketName)
      .build();

    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    return new ProtostellarRequest<>(request,
      core,
      ServiceType.MANAGER,
      TracingIdentifiers.SPAN_REQUEST_MB_DROP_BUCKET,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MB_DROP_BUCKET, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0,
      null);
  }

  public static ProtostellarRequest<ListBucketsRequest> listBucketsRequest(CoreProtostellar core, CoreCommonOptions opts) {
    Duration timeout = CoreProtostellarUtil.managementTimeout(opts.timeout(), core);

    return new ProtostellarRequest<>(ListBucketsRequest.getDefaultInstance(),
      core,
      ServiceType.MANAGER,
      TracingIdentifiers.SPAN_REQUEST_MB_GET_ALL_BUCKETS,
      createSpan(core, TracingIdentifiers.SPAN_REQUEST_MB_GET_ALL_BUCKETS, CoreDurability.NONE, opts.parentSpan().orElse(null)),
      timeout,
      false,
      opts.retryStrategy().orElse(core.context().environment().retryStrategy()),
      opts.clientContext(),
      0,
      null);
  }
}
