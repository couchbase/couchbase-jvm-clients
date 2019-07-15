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

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketSettings {

  /**
   * Static to convert from bytes to megabytes.
   */
  private final long MEGABYTES = 1024 * 1024;

  private final String name;
  private boolean flushEnabled = false;
  private long ramQuotaMB = 100;
  private int numReplicas = 1;
  private boolean replicaIndexes = false;
  private Optional<String> saslPassword = Optional.empty();
  private int proxyPort = 0;
  private int maxTTL = 0;
  private CompressionMode compressionMode = CompressionMode.PASSIVE;
  private BucketType bucketType = BucketType.COUCHBASE;
  private AuthType authType = AuthType.SASL;
  private ConflictResolutionType conflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER;
  private EvictionPolicy evictionPolicy = EvictionPolicy.VALUE_ONLY;

  public static BucketSettings create(final String name) {
    return new BucketSettings(name);
  }

  private BucketSettings(final String name) {
    this.name = name;
  }

  @JsonCreator
  public BucketSettings(
    @JsonProperty("name") final String name,
    @JsonProperty("controllers") final Map<String, String> controllers,
    @JsonProperty("quota") final Map<String, Long> quota,
    @JsonProperty("numReplicas") final int numReplicas,
    @JsonProperty("replicaIndex") final boolean replicaIndex,
    @JsonProperty("proxyPort") final int proxyPort,
    @JsonProperty("maxTTL") final int maxTTL,
    @JsonProperty("compressionMode") final CompressionMode compressionMode,
    @JsonProperty("bucketType") final BucketType bucketType,
    @JsonProperty("authType") final AuthType authType,
    @JsonProperty("conflictResolutionType") final ConflictResolutionType conflictResolutionType,
    @JsonProperty("evictionPolicy") final EvictionPolicy evictionPolicy
  ) {
    this.name = name;
    this.flushEnabled = controllers.containsKey("flush");
    this.ramQuotaMB = ramQuotaToMB(quota.get("rawRAM"));
    this.numReplicas = numReplicas;
    this.replicaIndexes = replicaIndex;
    this.saslPassword = Optional.empty();
    this.proxyPort = proxyPort;
    this.maxTTL = maxTTL;
    this.compressionMode = compressionMode;
    this.bucketType = bucketType;
    this.authType = authType;
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
    return ramQuotaBytes == 0 ? 0 : ramQuotaBytes / MEGABYTES;
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

  public Optional<String> saslPassword() {
    return saslPassword;
  }

  public int proxyPort() {
    return proxyPort;
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

  public AuthType authType() {
    return authType;
  }

  public ConflictResolutionType conflictResolutionType() {
    return conflictResolutionType;
  }

  public EvictionPolicy evictionPolicy() {
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

  public BucketSettings saslPassword(String saslPassword) {
    this.saslPassword = Optional.ofNullable(saslPassword);
    return this;
  }

  public BucketSettings proxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
    return this;
  }

  public BucketSettings maxTTL(int maxTTL) {
    this.maxTTL = maxTTL;
    return this;
  }

  public BucketSettings compressionMode(CompressionMode compressionMode) {
    requireNonNull(compressionMode);
    this.compressionMode = compressionMode;
    return this;
  }

  public BucketSettings bucketType(BucketType bucketType) {
    requireNonNull(bucketType);
    this.bucketType = bucketType;
    return this;
  }

  public BucketSettings authType(AuthType authType) {
    requireNonNull(authType);
    this.authType = authType;
    return this;
  }

  public BucketSettings conflictResolutionType(ConflictResolutionType conflictResolutionType) {
    requireNonNull(conflictResolutionType);
    this.conflictResolutionType = conflictResolutionType;
    return this;
  }

  public BucketSettings evictionPolicy(EvictionPolicy evictionPolicy) {
    requireNonNull(evictionPolicy);
    this.evictionPolicy = evictionPolicy;
    return this;
  }

  @Override
  public String toString() {
    return "BucketSettings{" +
      "name='" + name + '\'' +
      ", flushEnabled=" + flushEnabled +
      ", ramQuotaMB=" + ramQuotaMB +
      ", numReplicas=" + numReplicas +
      ", replicaIndexes=" + replicaIndexes +
      ", saslPassword=" + saslPassword +
      ", proxyPort=" + proxyPort +
      ", maxTTL=" + maxTTL +
      ", compressionMode=" + compressionMode +
      ", bucketType=" + bucketType +
      ", authType=" + authType +
      ", conflictResolutionType=" + conflictResolutionType +
      ", evictionPolicy=" + evictionPolicy +
      '}';
  }
}
