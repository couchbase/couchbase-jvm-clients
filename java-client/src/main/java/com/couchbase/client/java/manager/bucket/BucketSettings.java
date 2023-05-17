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
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.manager.bucket.CoreBucketSettings;
import com.couchbase.client.core.manager.bucket.CoreCompressionMode;
import com.couchbase.client.core.manager.bucket.CoreEvictionPolicyType;
import com.couchbase.client.core.manager.bucket.CoreStorageBackend;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;

import java.time.Duration;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.manager.bucket.CoreStorageBackend.COUCHSTORE;
import static com.couchbase.client.core.manager.bucket.CoreStorageBackend.MAGMA;
import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Represents all properties of a Couchbase Server Bucket.
 * <p>
 * The {@link BucketSettings} interact with the bucket management APIs: {@link BucketManager}, {@link ReactiveBucketManager}
 * and {@link AsyncBucketManager}, which can be obtained through {@link Cluster#buckets()}, {@link ReactiveCluster#buckets()}
 * and {@link AsyncCluster#buckets()}.
 */
public class BucketSettings {

  private final String name;

  private boolean flushEnabled = false;
  private long ramQuotaMB = 100;
  private int numReplicas = 1;
  private boolean replicaIndexes = false;
  private Duration maxExpiry = Duration.ZERO;
  private CompressionMode compressionMode;
  private BucketType bucketType = BucketType.COUCHBASE;
  private ConflictResolutionType conflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER;
  private EvictionPolicyType evictionPolicy = null; // null means default for the bucket type
  private DurabilityLevel minimumDurabilityLevel = DurabilityLevel.NONE;
  private StorageBackend storageBackend = null; // null means default for the bucket type
  private boolean healthy = true;

  @Stability.Internal
  public BucketSettings(CoreBucketSettings internal) {
    this.name = internal.name();
    this.flushEnabled = internal.flushEnabled();
    this.ramQuotaMB = internal.ramQuotaMB();
    this.replicaIndexes = internal.replicaIndexes();
    this.numReplicas = internal.numReplicas();
    this.maxExpiry = internal.maxExpiry();
    // Will be null for CE
    if (internal.compressionMode() != null) {
      switch (internal.compressionMode()) {
        case OFF:
          this.compressionMode = CompressionMode.OFF;
          break;
        case PASSIVE:
          this.compressionMode = CompressionMode.PASSIVE;
          break;
        case ACTIVE:
          this.compressionMode = CompressionMode.ACTIVE;
          break;
        default:
          throw new CouchbaseException("Unknown compression mode");
      }
    }
    switch (internal.bucketType()) {
      case COUCHBASE:
        this.bucketType = BucketType.COUCHBASE;
        break;
      case EPHEMERAL:
        this.bucketType = BucketType.EPHEMERAL;
        break;
      case MEMCACHED:
        this.bucketType = BucketType.MEMCACHED;
        break;
    }
    switch (internal.evictionPolicy()) {
      case FULL:
        this.evictionPolicy = EvictionPolicyType.FULL;
        break;
      case VALUE_ONLY:
        this.evictionPolicy = EvictionPolicyType.VALUE_ONLY;
        break;
      case NOT_RECENTLY_USED:
        this.evictionPolicy = EvictionPolicyType.NOT_RECENTLY_USED;
        break;
      case NO_EVICTION:
        this.evictionPolicy = EvictionPolicyType.NO_EVICTION;
        break;
    }
    this.minimumDurabilityLevel = internal.minimumDurabilityLevel();
    if (internal.storageBackend() == COUCHSTORE) {
      this.storageBackend = StorageBackend.COUCHSTORE;
    } else if (internal.storageBackend() == MAGMA) {
      this.storageBackend = StorageBackend.MAGMA;
    }
  }

  BucketSettings(String name) {
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

  @Stability.Internal
  public CoreBucketSettings toCore() {
    return new CoreBucketSettings() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Boolean flushEnabled() {
        return flushEnabled;
      }

      @Override
      public long ramQuotaMB() {
        return ramQuotaMB;
      }

      @Override
      public Integer numReplicas() {
        return numReplicas;
      }

      @Override
      public Boolean replicaIndexes() {
        return replicaIndexes;
      }

      @Override
      public com.couchbase.client.core.config.BucketType bucketType() {
        switch (bucketType) {
          case COUCHBASE: return com.couchbase.client.core.config.BucketType.COUCHBASE;
          case MEMCACHED: return com.couchbase.client.core.config.BucketType.MEMCACHED;
          case EPHEMERAL: return com.couchbase.client.core.config.BucketType.EPHEMERAL;
          default: throw new CouchbaseException("Unknown bucket type " + bucketType);
        }
      }

      @Override
      public CoreEvictionPolicyType evictionPolicy() {
        if (evictionPolicy == null) {
          return null;
        }

        switch (evictionPolicy) {
          case FULL: return CoreEvictionPolicyType.FULL;
          case VALUE_ONLY: return CoreEvictionPolicyType.VALUE_ONLY;
          case NOT_RECENTLY_USED: return CoreEvictionPolicyType.NOT_RECENTLY_USED;
          case NO_EVICTION: return CoreEvictionPolicyType.NO_EVICTION;
          default: throw new CouchbaseException("Unknown eviction policy " + evictionPolicy);
        }
      }

      @Override
      public Duration maxExpiry() {
        return maxExpiry;
      }

      @Override
      public CoreCompressionMode compressionMode() {
        if (compressionMode == null) {
          return null;
        }

        switch (compressionMode) {
          case OFF: return CoreCompressionMode.OFF;
          case PASSIVE: return CoreCompressionMode.PASSIVE;
          case ACTIVE: return CoreCompressionMode.ACTIVE;
          default: throw new CouchbaseException("Unknown compression mode " + compressionMode);
        }
      }

      @Override
      public DurabilityLevel minimumDurabilityLevel() {
        return minimumDurabilityLevel;
      }

      @Override
      public CoreStorageBackend storageBackend() {
        if (storageBackend == null) {
          return null;
        }

        return storageBackend.alias().equals(StorageBackend.MAGMA.alias()) ? CoreStorageBackend.MAGMA : CoreStorageBackend.COUCHSTORE;
      }
    };
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
