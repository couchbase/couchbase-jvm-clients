/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.config;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents the possible bucket types.
 *
 * @since 1.0
 */
public enum BucketType {
  /**
   * This bucket is a "couchbase" bucket.
   */
  COUCHBASE("membase"),
  /**
   * This bucket is an "ephemeral" bucket.
   */
  EPHEMERAL("ephemeral"),
  /**
   * This bucket is a "memcached" bucket.
   */
  MEMCACHED("memcached");

  private final String raw;

  BucketType(final String raw) {
    this.raw = raw;
  }

  @JsonValue
  public String getRaw() {
    return raw;
  }

}
