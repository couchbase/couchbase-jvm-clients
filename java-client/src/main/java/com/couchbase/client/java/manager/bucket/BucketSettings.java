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
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.Validators.notNull;
import static java.util.Objects.requireNonNull;

/**
 * Represents all properties of a Couchbase Server Bucket.
 * <p>
 * The {@link BucketSettings} interact with the bucket management APIs: {@link BucketManager}, {@link ReactiveBucketManager}
 * and {@link AsyncBucketManager}, which can be obtained through {@link Cluster#buckets()}, {@link ReactiveCluster#buckets()}
 * and {@link AsyncCluster#buckets()}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketSettings {

  private final String name;

  private boolean flushEnabled = false;
  private long ramQuotaMB = 100;
  private int numReplicas = 1;
  private boolean replicaIndexes = false;
  private Duration maxExpiry = Duration.ZERO;
  // Package-private to allow access to the raw, nullable value for passthrough purposes
  @Nullable CompressionMode compressionMode = null;
  private BucketType bucketType = BucketType.COUCHBASE;
  private ConflictResolutionType conflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER;
  private EvictionPolicyType evictionPolicy = null; // null means default for the bucket type
  private DurabilityLevel minimumDurabilityLevel = DurabilityLevel.NONE;
  private StorageBackend storageBackend = null; // null means default for the bucket type
  private boolean healthy = true;

  /**
   * Creates {@link BucketSettings} from a raw JSON payload.
   * <p>
   * Do not use this API directly, it's internal! Instead use {@link #BucketSettings(String)} and then apply
   * the customizations through its setters.
   *
   * @param name the name of the bucket.
   * @param controllers the configured controllers.
   * @param quota the current bucket quota.
   * @param numReplicas the number of replicas configured.
   * @param replicaIndex the replica index.
   * @param maxTTL the maximum TTL currently configured.
   * @param compressionMode which compression mode is used, if any.
   * @param bucketType which bucket type is used.
   * @param conflictResolutionType which conflict resolution type is currently in use.
   * @param evictionPolicy the eviction policy in use.
   * @param durabilityMinLevel the minimum durability level configured, if any.
   * @param storageBackend the storage backend in use.
   */
  @Stability.Internal
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
    // Couchbase 5.0 and CE doesn't send a compressionMode
    this.compressionMode = compressionMode;
    this.bucketType = bucketType;
    this.conflictResolutionType = conflictResolutionType;
    this.evictionPolicy = evictionPolicy;
    this.minimumDurabilityLevel = DurabilityLevel.decodeFromManagementApi(durabilityMinLevel);
    this.storageBackend = storageBackend;
  }

  /**
   * Creates {@link BucketSettings} with just the name and default settings.
   *
   * @param name the name of the bucket.
   */
  private BucketSettings(final String name) {
    this.name = name;
  }

  /**
   * Creates {@link BucketSettings} with the bucket name and all default properties.
   *
   * @param name the name of the bucket.
   * @return the {@link BucketSettings} with all its defaults set.
   */
  public static BucketSettings create(final String name) {
    return new BucketSettings(name);
  }

  /**
   * Helper method to create {@link BucketSettings} from a Jackson JsonNode.
   *
   * @param tree the node tree to evaluate.
   * @return the decoded {@link BucketSettings}.
   */
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

  /**
   * Converts the server ram quota from bytes to megabytes.
   *
   * @param ramQuotaBytes the input quota in bytes
   * @return converted to megabytes
   */
  private static long ramQuotaToMB(long ramQuotaBytes) {
    final long BYTES_PER_MEGABYTE = 1024 * 1024;
    return ramQuotaBytes == 0 ? 0 : ramQuotaBytes / BYTES_PER_MEGABYTE;
  }

  /**
   * Returns the name of the bucket.
   */
  public String name() {
    return name;
  }

  /**
   * Returns true if flush is enabled on the bucket.
   */
  public boolean flushEnabled() {
    return flushEnabled;
  }

  /**
   * Returns the bucket RAM quota in megabytes.
   */
  public long ramQuotaMB() {
    return ramQuotaMB;
  }

  /**
   * Returns the configured number of replicas.
   */
  public int numReplicas() {
    return numReplicas;
  }

  /**
   * Returns the number of replica indexes configured.
   */
  public boolean replicaIndexes() {
    return replicaIndexes;
  }

  /**
   * Returns the minimum durability level set for the bucket.
   * <p>
   * Note that if the bucket does not support it, and by default, it is set to {@link DurabilityLevel#NONE}.
   * @return the minimum durability level for that bucket.
   */
  public DurabilityLevel minimumDurabilityLevel() {
    return minimumDurabilityLevel;
  }

  /**
   * Returns the maximum expiry (time-to-live) for all documents in the bucket.
   */
  public Duration maxExpiry() {
    return maxExpiry;
  }

  /**
   * Returns the {@link CompressionMode} used for the bucket.
   */
  public CompressionMode compressionMode() {
    if (compressionMode == null) {
      return CompressionMode.OFF;
    }
    return compressionMode;
  }

  /**
   * Returns the bucket type.
   */
  public BucketType bucketType() {
    return bucketType;
  }

  /**
   * Returns the conflict resolution mode in use.
   */
  public ConflictResolutionType conflictResolutionType() {
    return conflictResolutionType;
  }

  /**
   * Returns the storage backend for the bucket.
   */
  public StorageBackend storageBackend() {
    return storageBackend;
  }

  /**
   * Returns the eviction policy used on the bucket.
   */
  public EvictionPolicyType evictionPolicy() {
    return evictionPolicy;
  }

  /**
   * Returns true if the bucket is identified as healthy by the cluster manager.
   */
  @Stability.Internal
  public boolean healthy() {
    return healthy;
  }

  /**
   * Allows enabling flush on the bucket.
   *
   * @param flushEnabled if flush should be enabled (not recommended for production!).
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings flushEnabled(boolean flushEnabled) {
    this.flushEnabled = flushEnabled;
    return this;
  }

  /**
   * Sets the ram quota in MB for this bucket.
   *
   * @param ramQuotaMB the bucket quota in megabytes.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings ramQuotaMB(long ramQuotaMB) {
    this.ramQuotaMB = ramQuotaMB;
    return this;
  }

  /**
   * Sets the number of replica copies for the bucket.
   *
   * @param numReplicas the number of replicas.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings numReplicas(int numReplicas) {
    this.numReplicas = numReplicas;
    return this;
  }

  /**
   * Sets the number of replica indexes on the bucket.
   *
   * @param replicaIndexes the number of replica indexes.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings replicaIndexes(boolean replicaIndexes) {
    this.replicaIndexes = replicaIndexes;
    return this;
  }

  /**
   * Specifies the maximum expiry (time-to-live) for all documents in the bucket.
   *
   * @param maxExpiry the maximum expiry.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings maxExpiry(final Duration maxExpiry) {
    this.maxExpiry = notNull(maxExpiry, "MaxExpiry");
    return this;
  }

  /**
   * Sets the compression mode on the bucket.
   *
   * @param compressionMode the compression mode to use.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings compressionMode(final CompressionMode compressionMode) {
    this.compressionMode = notNull(compressionMode, "CompressionMode");
    return this;
  }

  /**
   * Configures the {@link BucketType}.
   *
   * @param bucketType the type of the bucket.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings bucketType(final BucketType bucketType) {
    this.bucketType = notNull(bucketType, "BucketType");
    return this;
  }

  /**
   * Configures the conflict resolution mode for the bucket.
   *
   * @param conflictResolutionType the type of conflict resolution to use.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings conflictResolutionType(final ConflictResolutionType conflictResolutionType) {
    this.conflictResolutionType = notNull(conflictResolutionType, "ConflictResolutionType");
    return this;
  }

  /**
   * Allows to configure a custom {@link EvictionPolicyType} as the eviction policy.
   * <p>
   * Eviction automatically removes older data from RAM to create space for new data if you reach your bucket quota.
   * How eviction behaves in detail depends on the {@link BucketType} chosen - please consult the server documentation
   * for more information on the subject.
   *
   * @param evictionPolicy (nullable) policy to use, or null for default policy for the {@link BucketType}.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings evictionPolicy(final EvictionPolicyType evictionPolicy) {
    this.evictionPolicy = evictionPolicy;
    return this;
  }

  /**
   * Configures a custom minimum {@link DurabilityLevel} for this bucket.
   * <p>
   * For {@link BucketType#COUCHBASE}, all durability levels are available. For {@link BucketType#EPHEMERAL} only
   * {@link DurabilityLevel#NONE} and {@link DurabilityLevel#MAJORITY} are available. The durability level is not
   * supported on memcached buckets (please use ephemeral buckets instead).
   *
   * @param durabilityLevel the minimum level to use for all KV operations.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings minimumDurabilityLevel(final DurabilityLevel durabilityLevel) {
    this.minimumDurabilityLevel = notNull(durabilityLevel, "DurabilityLevel");
    return this;
  }

  /**
   * Configures a {@link StorageBackend} for this bucket.
   * <p>
   * Note that {@link StorageBackend#MAGMA} is only supported in 7.0 if developer preview is enabled. It is recommended
   * to be used only with Server 7.1 and later.
   * <p>
   * Also, if a {@link BucketType} is chosen that does not have a storage backend attached (i.e. {@link BucketType#MEMCACHED}
   * or {@link BucketType#EPHEMERAL}), then this property is ignored.
   *
   * @param storageBackend the backend to use.
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings storageBackend(final StorageBackend storageBackend) {
    this.storageBackend = notNull(storageBackend, "storageBackend");
    return this;
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
   * @deprecated Please use {@link #evictionPolicy} instead.
   */
  @Deprecated
  public EjectionPolicy ejectionPolicy() {
    return EjectionPolicy.of(evictionPolicy);
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
