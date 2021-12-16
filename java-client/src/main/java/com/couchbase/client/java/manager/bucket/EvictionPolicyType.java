/*
 * Copyright 2020 Couchbase, Inc.
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

import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Objects.requireNonNull;

/**
 * The eviction policy for a bucket.
 */
public enum EvictionPolicyType {
  /**
   * During ejection, everything (including key, metadata, and value) will be ejected.
   * <p>
   * Full Ejection reduces the memory overhead requirement, at the cost of performance.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#COUCHBASE}.
   */
  FULL("fullEviction"),

  /**
   * During ejection, only the value will be ejected (key and metadata will remain in memory).
   * <p>
   * Value Ejection needs more system memory, but provides better performance than Full Ejection.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#COUCHBASE}.
   */
  VALUE_ONLY("valueOnly"),

  /**
   * When the memory quota is reached, Couchbase Server ejects data that has
   * not been used recently.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#EPHEMERAL}.
   */
  NOT_RECENTLY_USED("nruEviction"),

  /**
   * Couchbase Server keeps all data until explicitly deleted, but will reject
   * any new data if you reach the quota (dedicated memory) you set for your bucket.
   * <p>
   * This value is only valid for buckets of type {@link BucketType#EPHEMERAL}.
   */
  NO_EVICTION("noEviction");

  private final String alias;

  EvictionPolicyType(String alias) {
    this.alias = requireNonNull(alias);
  }

  @JsonValue
  @Stability.Internal
  public String alias() {
    return alias;
  }
}
