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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.DurabilityLevel;

import java.time.Duration;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.Validators.notNull;
import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketSettings {

  private final String name;
  private boolean flushEnabled = false;
  private long ramQuotaMB = 100;
  private int numReplicas = 1;
  private boolean replicaIndexes = false;
  private Duration maxExpiry = Duration.ZERO;
  private CompressionMode compressionMode = CompressionMode.PASSIVE;
  private BucketType bucketType = BucketType.COUCHBASE;
  private ConflictResolutionType conflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER;
  private EvictionPolicyType evictionPolicy = null; // null means default for the bucket type
  private DurabilityLevel minimumDurabilityLevel = DurabilityLevel.NONE;
  private StorageBackend storageBackend = null;
  private boolean healthy = true;

  public static BucketSettings create(final String name) {
    return new BucketSettings(name);
  }

  static BucketSettings create(final JsonNode tree) {
    BucketSettings settings = Mapper.convertValue(tree, BucketSettings.class);
    JsonNode nodes = tree.get("nodes");
    if (nodes.isArray() && !nodes.isEmpty()) {
      for (final JsonNode node : nodes) {
        String status = node.get("status").asText();
        if (!status.equals("healthy")) {
          settings.healthy = false;
        }
      }
    }
    else {
      settings.healthy = false;
    }
    return settings;
  }

  private BucketSettings(final String name) {
    this.name = name;
  }

  @JsonCreator
  public BucketSettings(
    @JsonProperty("name") final String name,
    @JsonProperty("controllers") final Map<String, String> controllers,
    @JsonProperty("quota") final Map<String, Long> quota,
    @JsonProperty("replicaNumber") final int numReplicas,
    @JsonProperty("replicaIndex") final boolean replicaIndex,
    @JsonProperty("maxTTL") final int maxTTL,
    @JsonProperty("compressionMode") final CompressionMode compressionMode,
    @JsonProperty("bucketType") final BucketType bucketType,
    @JsonProperty("conflictResolutionType") final ConflictResolutionType conflictResolutionType,
    @JsonProperty("evictionPolicy") final EvictionPolicyType evictionPolicy,
    @JsonProperty("durabilityMinLevel") final String durabilityMinLevel,
    @JsonProperty("storageBackend") final StorageBackend storageBackend
  ) {
    this.name = name;
    this.flushEnabled = controllers.containsKey("flush");
    this.ramQuotaMB = ramQuotaToMB(quota.get("rawRAM"));
    this.numReplicas = numReplicas;
    this.replicaIndexes = replicaIndex;
    this.maxExpiry = Duration.ofSeconds(maxTTL);
    if (compressionMode != null) {
      this.compressionMode = compressionMode;
    }
    else {
      // Couchbase 5.0 doesn't send a compressionMode
      this.compressionMode = CompressionMode.OFF;
    }
    this.bucketType = bucketType;
    this.conflictResolutionType = conflictResolutionType;
    this.evictionPolicy = evictionPolicy;
    this.minimumDurabilityLevel = DurabilityLevel.decodeFromManagementApi(durabilityMinLevel);
    this.storageBackend = storageBackend;
  }

  /**
   * Converts the server ram quota from bytes to megabytes.
   *
   * @param ramQuotaBytes the input quota in bytes
   * @return converted to megabytes
   */
  private long ramQuotaToMB(long ramQuotaBytes) {
    final long BYTES_PER_MEGABYTE = 1024 * 1024;
    return ramQuotaBytes == 0 ? 0 : ramQuotaBytes / BYTES_PER_MEGABYTE;
  }

  public String name() {
    return name;
  }

  public boolean flushEnabled() {
    return flushEnabled;
  }

  public long ramQuotaMB() {
    return ramQuotaMB;
  }

  public int numReplicas() {
    return numReplicas;
  }

  public boolean replicaIndexes() {
    return replicaIndexes;
  }

  /**
   * Returns the minimum durability level set for the bucket.
   *
   * Note that if the bucket does not support it, and by default, it is set to {@link DurabilityLevel#NONE}.
   * @return the minimum durability level for that bucket.
   */
  public DurabilityLevel minimumDurabilityLevel() {
    return minimumDurabilityLevel;
  }

  /**
   * Returns the maximum expiry (time-to-live) for all documents in the bucket in seconds.
   *
   * @deprecated please use {@link #maxExpiry()} instead.
   */
  @Deprecated
  public int maxTTL() {
    return (int) maxExpiry().getSeconds();
  }

  /**
   * Returns the maximum expiry (time-to-live) for all documents in the bucket.
   */
  public Duration maxExpiry() {
    return maxExpiry;
  }

  public CompressionMode compressionMode() {
    return compressionMode;
  }

  public BucketType bucketType() {
    return bucketType;
  }

  public ConflictResolutionType conflictResolutionType() {
    return conflictResolutionType;
  }

  @Stability.Uncommitted
  public StorageBackend storageBackend() {
    return storageBackend;
  }

  /**
   * @deprecated Please use {@link #evictionPolicy} instead.
   */
  @Deprecated
  public EjectionPolicy ejectionPolicy() {
    return EjectionPolicy.of(evictionPolicy);
  }

  public EvictionPolicyType evictionPolicy() {
    return evictionPolicy;
  }

  public BucketSettings flushEnabled(boolean flushEnabled) {
    this.flushEnabled = flushEnabled;
    return this;
  }

  public BucketSettings ramQuotaMB(long ramQuotaMB) {
    this.ramQuotaMB = ramQuotaMB;
    return this;
  }

  public BucketSettings numReplicas(int numReplicas) {
    this.numReplicas = numReplicas;
    return this;
  }

  public BucketSettings replicaIndexes(boolean replicaIndexes) {
    this.replicaIndexes = replicaIndexes;
    return this;
  }

  /**
   * Specifies the maximum expiry (time-to-live) for all documents in the bucket in seconds.
   *
   * @param maxTTL the maximum expiry in seconds.
   * @return this {@link BucketSettings} for chaining purposes.
   * @deprecated please use {@link #maxExpiry(Duration)} instead.
   */
  @Deprecated
  public BucketSettings maxTTL(int maxTTL) {
    return maxExpiry(Duration.ofSeconds(maxTTL));
  }

  /**
   * Specifies the maximum expiry (time-to-live) for all documents in the bucket.
   *
   * @param maxExpiry the maximum expiry.
   * @return this {@link BucketSettings} for chaining purposes.
   */
  public BucketSettings maxExpiry(final Duration maxExpiry) {
    this.maxExpiry = notNull(maxExpiry, "MaxExpiry");
    return this;
  }

  public BucketSettings compressionMode(CompressionMode compressionMode) {
    this.compressionMode = requireNonNull(compressionMode);
    return this;
  }

  public BucketSettings bucketType(BucketType bucketType) {
    this.bucketType = requireNonNull(bucketType);
    return this;
  }

  public BucketSettings conflictResolutionType(ConflictResolutionType conflictResolutionType) {
    this.conflictResolutionType = requireNonNull(conflictResolutionType);
    return this;
  }

  /**
   * @param ejectionPolicy (nullable) policy to use, or null for default policy for the bucket type.
   * @deprecated Please use {@link #evictionPolicy} instead.
   */
  @Deprecated
  public BucketSettings ejectionPolicy(EjectionPolicy ejectionPolicy) {
    this.evictionPolicy = ejectionPolicy == null ? null : ejectionPolicy.toEvictionPolicy();
    return this;
  }

  /**
   * @param evictionPolicy (nullable) policy to use, or null for default policy for the bucket type.
   */
  public BucketSettings evictionPolicy(EvictionPolicyType evictionPolicy) {
    this.evictionPolicy = evictionPolicy;
    return this;
  }

  /**
   * Allows to provide a custom minimum {@link DurabilityLevel} for this bucket.
   *
   * @param durabilityLevel the minimum level to use for all KV operations.
   * @return this {@link BucketSettings} object for chainability.
   */
  public BucketSettings minimumDurabilityLevel(final DurabilityLevel durabilityLevel) {
    this.minimumDurabilityLevel = notNull(durabilityLevel, "DurabilityLevel");
    return this;
  }

  @Stability.Uncommitted
  public BucketSettings storageBackend(final StorageBackend storageBackend) {
    this.storageBackend = notNull(storageBackend, "storageBackend");
    return this;
  }

  @Stability.Internal
  public boolean healthy() {
    return healthy;
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
      ", storageBackend=" +storageBackend +
      '}';
  }

}
