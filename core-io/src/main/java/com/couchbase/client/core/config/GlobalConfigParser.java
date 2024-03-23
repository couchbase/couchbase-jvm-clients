/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.InjectableValues;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.topology.ClusterTopology;

import java.io.IOException;

/**
 * @see GlobalConfig#GlobalConfig(ClusterTopology)
 * @deprecated In favor of {@link com.couchbase.client.core.topology.TopologyParser}
 */
@Deprecated
public class GlobalConfigParser {
  /**
   * Parse a raw configuration into a {@link GlobalConfig}.
   *
   * @param input the raw string input.
   * @param origin the origin of the configuration. If null / none provided then localhost is assumed.
   * @return the parsed bucket configuration.
   */
  public static GlobalConfig parse(final String input, final String origin) {
    try {
      InjectableValues inject = new InjectableValues.Std()
        .addValue("origin", origin == null ? "127.0.0.1" : origin);
      return Mapper.reader()
        .forType(GlobalConfig.class)
        .with(inject)
        .with(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
        .readValue(input);
    } catch (IOException e) {
      throw new CouchbaseException("Could not parse global configuration", e);
    }
  }

}
