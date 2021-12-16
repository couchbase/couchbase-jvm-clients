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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;

/**
 * Specifies the main type of the bucket.
 */
@Stability.Volatile
public enum BucketType {
  /**
   * Couchbase Bucket Type.
   * <p>
   * Stores data persistently, as well as in memory. It allows data to be automatically replicated for high
   * availability, using the Database Change Protocol (DCP); and dynamically scaled across multiple clusters,
   * by means of Cross Datacenter Replication (XDCR).
   */
  @JsonProperty("membase") COUCHBASE,
  /**
   * Memcached Bucket Type.
   * <p>
   * These are now deprecated. Memcached buckets are designed to be used alongside other database platforms,
   * such as ones employing relational database technology. By caching frequently-used data, Memcached
   * buckets reduce the number of queries a database-server must perform. Each Memcached bucket provides a
   * directly addressable, distributed, in-memory key-value cache.
   */
  @JsonProperty("memcached") MEMCACHED,
  /**
   * Ephemeral Bucket Type.
   * <p>
   * Ephemeral buckets are an alternative to Couchbase buckets, to be used whenever persistence is not required: for
   * example, when repeated disk-access involves too much overhead. This allows highly consistent in-memory performance,
   * without disk-based fluctuations. It also allows faster node rebalances and restarts.
   */
  @JsonProperty("ephemeral") EPHEMERAL;

  @Stability.Internal
  public String alias() {
    return name().toLowerCase(Locale.ROOT);
  }

}
