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
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.manager.bucket.CoreBucketSettings;
import com.couchbase.client.core.manager.bucket.CoreCompressionMode;
import com.couchbase.client.core.manager.bucket.CoreConflictResolutionType;
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
  private static final boolean DEFAULT_FLUSH_ENABLED = false;
  private static final long DEFAULT_RAM_QUOTA_MB = 100;
  private static final int DEFAULT_NUM_REPLICAS = 1;
  private static final boolean DEFAULT_REPLICA_INDEXES = false;
  private static final BucketType DEFAULT_BUCKET_TYPE = BucketType.COUCHBASE;
  private static final Duration DEFAULT_MAX_EXPIRY = Duration.ZERO;
  private static final DurabilityLevel DEFAULT_MINIMUM_DURABILITY_LEVEL = DurabilityLevel.NONE;
  private static final ConflictResolutionType DEFAULT_CONFLICT_RESOLUTION = ConflictResolutionType.SEQUENCE_NUMBER;
  // JVMCBC-1395: Note to future implementors: it's very unlikely that new defaults are needed.  All settings should usually be left empty,
  // e.g. at server default.

  private final String name;

  private Boolean flushEnabled;
  private Long ramQuotaMB;
  private Integer numReplicas;
  private Boolean replicaIndexes;
  private Duration maxExpiry;
  private CompressionMode compressionMode;
  private BucketType bucketType;
  private ConflictResolutionType conflictResolutionType;
  private EvictionPolicyType evictionPolicy;
  private DurabilityLevel minimumDurabilityLevel;
  private StorageBackend storageBackend;
  private Boolean historyRetentionCollectionDefault;
  private Long historyRetentionBytes;
  private Duration historyRetentionDuration;
  // Remember to add any new user settings into the merge function.

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
    if (internal.bucketType() != null) {
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
    }
    if (internal.conflictResolutionType() != null) {
      switch (internal.conflictResolutionType()) {
        case CUSTOM:
          this.conflictResolutionType = ConflictResolutionType.CUSTOM;
          break;
        case SEQUENCE_NUMBER:
          this.conflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER;
          break;
        case TIMESTAMP:
          this.conflictResolutionType = ConflictResolutionType.TIMESTAMP;
          break;
      }
    }
    if (internal.evictionPolicy() != null) {
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
    }
    this.minimumDurabilityLevel = internal.minimumDurabilityLevel();
    if (internal.storageBackend() != null) {
      if (internal.storageBackend() == COUCHSTORE) {
        this.storageBackend = StorageBackend.COUCHSTORE;
      } else if (internal.storageBackend() == MAGMA) {
        this.storageBackend = StorageBackend.MAGMA;
      }
    }

    this.historyRetentionCollectionDefault = internal.historyRetentionCollectionDefault();
    this.historyRetentionBytes = internal.historyRetentionBytes();
    this.historyRetentionDuration = internal.historyRetentionDuration();
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
   * Returns the history retention duration used on the bucket.
   */
  public Duration historyRetentionDuration() {
    return historyRetentionDuration;
  }

  /**
   * Returns the history retention bytes used on the bucket.
   */
  public Long historyRetentionBytes() {
    return historyRetentionBytes;
  }

  /**
   * Returns the history retention default used on the bucket.
   */
  public Boolean historyRetentionCollectionDefault() {
    return historyRetentionCollectionDefault;
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
   * Configures historyRetentionCollectionDefault for this bucket.
   *
   * @param historyRetentionCollectionDefault
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings historyRetentionCollectionDefault(final Boolean historyRetentionCollectionDefault) {
    this.historyRetentionCollectionDefault = notNull(historyRetentionCollectionDefault, "historyRetentionCollectionDefault");
    return this;
  }

  /**
   * Configures historyRetentionBytes for this bucket.
   *
   * @param historyRetentionBytes
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings historyRetentionBytes(final Long historyRetentionBytes) {
    this.historyRetentionBytes = notNull(historyRetentionBytes, "historyRetentionBytes");
    return this;
  }

  /**
   * Configures historyRetentionDuration for this bucket.
   *
   * @param historyRetentionDuration
   * @return this {@link BucketSettings} instance for chaining purposes.
   */
  public BucketSettings historyRetentionDuration(final Duration historyRetentionDuration) {
    this.historyRetentionDuration = notNull(historyRetentionDuration, "historyRetentionDuration");
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
      public Long ramQuotaMB() {
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
        if (bucketType != null) {
          switch (bucketType) {
            case COUCHBASE:
              return com.couchbase.client.core.config.BucketType.COUCHBASE;
            case MEMCACHED:
              return com.couchbase.client.core.config.BucketType.MEMCACHED;
            case EPHEMERAL:
              return com.couchbase.client.core.config.BucketType.EPHEMERAL;
            default:
              throw new CouchbaseException("Unknown bucket type " + bucketType);
          }
        }
        return null;
      }

      @Override
      public CoreConflictResolutionType conflictResolutionType() {
        if (conflictResolutionType == null) {
          return null;
        }
        switch (conflictResolutionType) {
          case TIMESTAMP:
            return CoreConflictResolutionType.TIMESTAMP;
          case SEQUENCE_NUMBER:
            return CoreConflictResolutionType.SEQUENCE_NUMBER;
          case CUSTOM:
            return CoreConflictResolutionType.CUSTOM;
          default:
            throw new CouchbaseException("Unknown conflict resolution type");
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
        return storageBackend == null ? null : CoreStorageBackend.of(storageBackend.alias());
      }

      @Override
      public Boolean historyRetentionCollectionDefault() {
        return historyRetentionCollectionDefault;
      }

      @Override
      public Long historyRetentionBytes() {
        return historyRetentionBytes;
      }

      @Override
      public Duration historyRetentionDuration() {
        return historyRetentionDuration;
      }
    };
  }

  @Override
  public String toString() {
    return "BucketSettings{" + // overriden in CreateBucketSettings
      "name='" + redactMeta(name) + '\'' +
      ", flushEnabled=" + flushEnabled +
      ", ramQuotaMB=" + ramQuotaMB +
      ", numReplicas=" + numReplicas +
      ", replicaIndexes=" + replicaIndexes +
      ", maxExpiry=" + (maxExpiry != null ? maxExpiry.getSeconds() : null) +
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

  /**
   * Using `base` as a baseline, merges non-null settings from `fromUser` (which, per the name, are the settings
   * provided by the user), and returns a new `BucketSettings` with the merged result.
   */
  static BucketSettings merge(BucketSettings base, BucketSettings fromUser) {
    BucketSettings out = new BucketSettings(fromUser.name != null ? fromUser.name : base.name);
    out.flushEnabled = defaultIfNull(fromUser.flushEnabled, base.flushEnabled);
    out.ramQuotaMB = defaultIfNull(fromUser.ramQuotaMB, base.ramQuotaMB);
    out.numReplicas = defaultIfNull(fromUser.numReplicas, base.numReplicas);
    out.replicaIndexes = defaultIfNull(fromUser.replicaIndexes, base.replicaIndexes);
    out.maxExpiry = defaultIfNull(fromUser.maxExpiry, base.maxExpiry);
    out.compressionMode = defaultIfNull(fromUser.compressionMode, base.compressionMode);
    out.bucketType = defaultIfNull(fromUser.bucketType, base.bucketType);
    out.conflictResolutionType = defaultIfNull(fromUser.conflictResolutionType, base.conflictResolutionType);
    out.evictionPolicy = defaultIfNull(fromUser.evictionPolicy, base.evictionPolicy);
    out.minimumDurabilityLevel = defaultIfNull(fromUser.minimumDurabilityLevel, base.minimumDurabilityLevel);
    out.storageBackend = defaultIfNull(fromUser.storageBackend, base.storageBackend);
    out.historyRetentionCollectionDefault = defaultIfNull(fromUser.historyRetentionCollectionDefault, base.historyRetentionCollectionDefault);
    out.historyRetentionBytes = defaultIfNull(fromUser.historyRetentionBytes, base.historyRetentionBytes);
    out.historyRetentionDuration = defaultIfNull(fromUser.historyRetentionDuration, base.historyRetentionDuration);
    return out;
  }

  private static <T> T defaultIfNull(T value, T defaultValue) {
    return value == null ? defaultValue : value;
  }

  static BucketSettings createDefaults(String name) {
    // JVMCBC-1395: These are the default bucket settings the Java SDK has always used.
    // Ideally none of them would be set, but that cannot be changed without potential breakage.
    BucketSettings out = new BucketSettings(name);
    out.flushEnabled = DEFAULT_FLUSH_ENABLED;
    out.ramQuotaMB = DEFAULT_RAM_QUOTA_MB;
    out.numReplicas = DEFAULT_NUM_REPLICAS;
    out.replicaIndexes = DEFAULT_REPLICA_INDEXES;
    out.bucketType = DEFAULT_BUCKET_TYPE;
    out.maxExpiry = DEFAULT_MAX_EXPIRY;
    out.minimumDurabilityLevel = DEFAULT_MINIMUM_DURABILITY_LEVEL;
    out.conflictResolutionType = DEFAULT_CONFLICT_RESOLUTION;
    return out;
  }
}
