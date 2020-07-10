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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

/**
 * @deprecated Please use {@link EvictionPolicyType} instead.
 */
@Stability.Volatile
@Deprecated
public enum EjectionPolicy {
  /**
   * During ejection, only the value will be ejected (key and metadata will remain in memory).
   * <p>
   * Value Ejection needs more system memory, but provides better performance than Full Ejection.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#COUCHBASE}.
   */
  VALUE_ONLY("valueOnly"),

  /**
   * During ejection, everything (including key, metadata, and value) will be ejected.
   * <p>
   * Full Ejection reduces the memory overhead requirement, at the cost of performance.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#COUCHBASE}.
   */
  FULL("fullEviction"),

  /**
   * Couchbase Server keeps all data until explicitly deleted, but will reject
   * any new data if you reach the quota (dedicated memory) you set for your bucket.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#EPHEMERAL}.
   */
  NO_EVICTION("noEviction"),

  /**
   * When the memory quota is reached, Couchbase Server ejects data that has
   * not been used recently.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#EPHEMERAL}.
   */
  NOT_RECENTLY_USED("nruEviction");

  private final String alias;

  EjectionPolicy(String alias) {
    this.alias = alias;
  }

  @JsonValue
  public String alias() {
    return alias;
  }

  @Stability.Internal
  static EjectionPolicy of(EvictionPolicyType evictionPolicy) {
    switch (evictionPolicy) {
      case FULL:
        return FULL;
      case VALUE_ONLY:
        return VALUE_ONLY;
      case NO_EVICTION:
        return NO_EVICTION;
      case NOT_RECENTLY_USED:
        return NOT_RECENTLY_USED;
      default:
        throw new RuntimeException("Unrecognized EvictionPolicyType alias: " + evictionPolicy.alias());
    }
  }

  @Stability.Internal
  EvictionPolicyType toEvictionPolicy() {
    switch (this) {
      case FULL:
        return EvictionPolicyType.FULL;
      case VALUE_ONLY:
        return EvictionPolicyType.VALUE_ONLY;
      case NO_EVICTION:
        return EvictionPolicyType.NO_EVICTION;
      case NOT_RECENTLY_USED:
        return EvictionPolicyType.NOT_RECENTLY_USED;
      default:
        throw new RuntimeException("Don't know how to convert " + this + " to " + EvictionPolicyType.class.getSimpleName());
    }
  }
}
