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

import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketSettings {

  private final String name;
  private boolean flushEnabled = false;
  private long ramQuotaMB = 100;
  private int numReplicas = 1;
  private boolean replicaIndexes = false;
  private int maxTTL = 0;
  private CompressionMode compressionMode = CompressionMode.PASSIVE;
  private BucketType bucketType = BucketType.COUCHBASE;
  private ConflictResolutionType conflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER;
  private EjectionPolicy evictionPolicy = EjectionPolicy.VALUE_ONLY;
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
    @JsonProperty("evictionPolicy") final EjectionPolicy evictionPolicy
  ) {
    this.name = name;
    this.flushEnabled = controllers.containsKey("flush");
    this.ramQuotaMB = ramQuotaToMB(quota.get("rawRAM"));
    this.numReplicas = numReplicas;
    this.replicaIndexes = replicaIndex;
    this.maxTTL = maxTTL;
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

  public int maxTTL() {
    return maxTTL;
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

  public EjectionPolicy ejectionPolicy() {
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

  public BucketSettings maxTTL(int maxTTL) {
    this.maxTTL = maxTTL;
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

  public BucketSettings ejectionPolicy(EjectionPolicy ejectionPolicy) {
    this.evictionPolicy = requireNonNull(ejectionPolicy);
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
      ", maxTTL=" + maxTTL +
      ", compressionMode=" + compressionMode +
      ", bucketType=" + bucketType +
      ", conflictResolutionType=" + conflictResolutionType +
      ", evictionPolicy=" + evictionPolicy +
      '}';
  }
}
