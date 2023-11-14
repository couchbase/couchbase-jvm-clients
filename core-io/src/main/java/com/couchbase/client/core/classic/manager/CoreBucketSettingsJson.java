/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.classic.manager;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.BucketType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.bucket.CoreBucketSettings;
import com.couchbase.client.core.manager.bucket.CoreCompressionMode;
import com.couchbase.client.core.manager.bucket.CoreConflictResolutionType;
import com.couchbase.client.core.manager.bucket.CoreEvictionPolicyType;
import com.couchbase.client.core.manager.bucket.CoreStorageBackend;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

/**
 * Helper class for working with bucket JSON.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreBucketSettingsJson implements CoreBucketSettings {

  private final String name;
  private final Boolean flushEnabled;
  private final long ramQuotaMB;
  private final int numReplicas;
  private final @Nullable Boolean replicaIndexes;
  private final @Nullable Duration maxExpiry;
  private final @Nullable CoreCompressionMode compressionMode;
  private final @Nullable BucketType bucketType;
  private final @Nullable CoreConflictResolutionType conflictResolutionType;
  private final @Nullable CoreEvictionPolicyType evictionPolicy;
  private final @Nullable DurabilityLevel minimumDurabilityLevel;
  private final @Nullable CoreStorageBackend storageBackend;
  private final @Nullable Boolean historyRetentionCollectionDefault;
  private final @Nullable Long historyRetentionBytes;
  private final @Nullable Duration historyRetentionDuration;

  @Stability.Internal
  @JsonCreator
  public CoreBucketSettingsJson(
      @JsonProperty("name") final String name,
      @JsonProperty("controllers") final Map<String, String> controllers,
      @JsonProperty("quota") final Map<String, Long> quota,
      @JsonProperty("replicaNumber") final int numReplicas,
      @JsonProperty("replicaIndex") final boolean replicaIndex,
      @JsonProperty("maxTTL") final int maxTTL,
      @JsonProperty("compressionMode") final CoreCompressionMode compressionMode,
      @JsonProperty("bucketType") final BucketType bucketType,
      @JsonProperty("conflictResolutionType") final CoreConflictResolutionType conflictResolutionType,
      @JsonProperty("evictionPolicy") final CoreEvictionPolicyType evictionPolicy,
      @JsonProperty("durabilityMinLevel") final String durabilityMinLevel,
      @JsonProperty("storageBackend") final CoreStorageBackend storageBackend,
      @JsonProperty("historyRetentionCollectionDefault") final Boolean historyRetentionCollectionDefault,
      @JsonProperty("historyRetentionBytes") final Long historyRetentionBytes,
      @JsonProperty("historyRetentionSeconds") final Long historyRetentionDurationSeconds
  ) {
    this.name = name;
    this.flushEnabled = controllers.containsKey("flush");
    this.ramQuotaMB = ramQuotaToMB(quota.get("rawRAM"));
    this.numReplicas = numReplicas;
    this.replicaIndexes = replicaIndex;
    this.maxExpiry = Duration.ofSeconds(maxTTL);
    // Couchbase 5.0 and CE doesn't send a compressionMode
    this.compressionMode = compressionMode;
    this.bucketType = bucketType;
    this.conflictResolutionType = conflictResolutionType;
    this.evictionPolicy = evictionPolicy;
    this.minimumDurabilityLevel = DurabilityLevel.decodeFromManagementApi(durabilityMinLevel);
    this.storageBackend = storageBackend;
    this.historyRetentionCollectionDefault = historyRetentionCollectionDefault;
    this.historyRetentionBytes = historyRetentionBytes;
    this.historyRetentionDuration = historyRetentionDurationSeconds == null ? null : Duration.ofSeconds(historyRetentionDurationSeconds);
  }

  static CoreBucketSettings create(final byte[] bytes) {
    JsonNode node = Mapper.decodeIntoTree(bytes);
    return create(node);
  }

  static CoreBucketSettings create(final JsonNode node) {
    return Mapper.convertValue(node, CoreBucketSettingsJson.class);
  }

  public static long ramQuotaToMB(long ramQuotaBytes) {
    final long BYTES_PER_MEGABYTE = 1024 * 1024;
    return ramQuotaBytes == 0 ? 0 : ramQuotaBytes / BYTES_PER_MEGABYTE;
  }

  public String name() {
    return name;
  }

  public Boolean flushEnabled() {
    return flushEnabled;
  }

  public Long ramQuotaMB() {
    return ramQuotaMB;
  }

  public Integer numReplicas() {
    return numReplicas;
  }

  public Boolean replicaIndexes() {
    return replicaIndexes;
  }

  public DurabilityLevel minimumDurabilityLevel() {
    return minimumDurabilityLevel;
  }

  public Duration maxExpiry() {
    return maxExpiry;
  }

  public CoreCompressionMode compressionMode() {
    return compressionMode;
  }

  public BucketType bucketType() {
    return bucketType;
  }

  public CoreConflictResolutionType conflictResolutionType() {
    return conflictResolutionType;
  }

  public CoreStorageBackend storageBackend() {
    return storageBackend;
  }

  public CoreEvictionPolicyType evictionPolicy() {
    return evictionPolicy;
  }

  @Nullable
  public Boolean historyRetentionCollectionDefault() {
    return historyRetentionCollectionDefault;
  }

  @Nullable
  public Long historyRetentionBytes() {
    return historyRetentionBytes;
  }

  @Nullable
  public Duration historyRetentionDuration() {
    return historyRetentionDuration;
  }

  @Override
  public String toString() {
    return "BucketSettings{" +
        "name='" + redactMeta(name) + '\'' +
        ", flushEnabled=" + flushEnabled +
        ", ramQuotaMB=" + ramQuotaMB +
        ", numReplicas=" + numReplicas +
        ", replicaIndexes=" + replicaIndexes +
        ", maxExpiry=" + maxExpiry.getSeconds() +
        ", compressionMode=" + compressionMode +
        ", bucketType=" + bucketType +
        ", conflictResolutionType=" + conflictResolutionType +
        ", evictionPolicy=" + evictionPolicy +
        ", minimumDurabilityLevel=" + minimumDurabilityLevel +
        ", storageBackend=" + storageBackend +
        ", historyRetentionCollectionDefault=" + historyRetentionCollectionDefault +
        ", historyRetentionBytes=" + historyRetentionBytes +
        ", historyRetentionDuration=" + historyRetentionDuration +
        '}';
  }

}
