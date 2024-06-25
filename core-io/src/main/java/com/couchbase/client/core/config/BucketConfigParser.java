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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.InjectableValues;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.TopologyParser;
import reactor.util.annotation.Nullable;

import java.io.IOException;

/**
 * An abstraction over the bucket parser which takes a raw config as a string and turns it into a
 * {@link BucketConfig}.
 *
 * @see ClusterTopology#requireBucket()
 * @see ClusterTopologyWithBucket#bucket()
 * @see LegacyConfigHelper#toLegacyBucketConfig(ClusterTopologyWithBucket)
 * @since 2.0.0
 * @deprecated In favor of {@link TopologyParser}
 */
@Deprecated
public final class BucketConfigParser {
  /**
   * Parse a raw configuration into a {@link BucketConfig}.
   *
   * @param input the raw string input.
   * @param env the environment to use.
   * @param origin the origin of the configuration. If null / none provided then localhost is assumed.
   * @return the parsed bucket configuration.
   */
  public static BucketConfig parse(
    final String input,
    final CoreEnvironment env,
    @Nullable final String origin
  ) {
    return parse(input, env.ioConfig().memcachedHashingStrategy(), origin);
  }

  @Stability.Internal
  public static BucketConfig parse(
    final String input,
    final MemcachedHashingStrategy memcachedHashingStrategy,
    @Nullable final String origin
  ) {
    try {
      InjectableValues inject = new InjectableValues.Std()
        .addValue("memcachedHashingStrategy", memcachedHashingStrategy)
        .addValue("origin", origin == null ? "127.0.0.1" : origin);
      return Mapper.reader()
        .forType(BucketConfig.class)
        .with(inject)
        .with(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
        .readValue(input);
    } catch (IOException e) {
      throw new CouchbaseException("Could not parse configuration", e);
    }
  }
}
