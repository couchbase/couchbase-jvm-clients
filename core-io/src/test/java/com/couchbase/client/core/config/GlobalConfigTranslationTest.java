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

import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.NetworkSelector;
import com.couchbase.client.core.topology.PortSelector;
import com.couchbase.client.core.topology.TopologyParser;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.couchbase.client.test.Util.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalConfigTranslationTest {

  @Test
  void parseSingleNodeGlobalConfig() {
    GlobalConfig config = readConfig("global_config_mad_hatter_single_node.json");

    assertEquals(26, config.rev());

    assertEquals(1, config.portInfos().size());
    PortInfo node1 = config.portInfos().get(0);
    assertEquals("127.0.0.1", node1.hostname());

    assertEquals(
      Collections.singleton(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS),
      config.clusterCapabilities().get(ServiceType.QUERY)
    );
    assertTrue(config.clusterCapabilities().get(ServiceType.KV).isEmpty());
  }

  @Test
  void parseMultiNodeGlobalConfig() {
    GlobalConfig config = readConfig("global_config_mad_hatter_multi_node.json");

    assertEquals(1172, config.rev());

    assertEquals(2, config.portInfos().size());

    PortInfo node1 = config.portInfos().get(0);
    PortInfo node2 = config.portInfos().get(1);

    assertEquals("10.143.193.101", node1.hostname());
    assertEquals("10.143.193.102", node2.hostname());

    assertEquals(
      Collections.singleton(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS),
      config.clusterCapabilities().get(ServiceType.QUERY)
    );
    assertTrue(config.clusterCapabilities().get(ServiceType.KV).isEmpty());
  }


  /**
   * Helper method to load the config.
   */
  private static GlobalConfig readConfig(final String path) {
    return new GlobalConfig(readTopology(path, "127.0.0.1"));
  }


  private static ClusterTopology readTopology(String path, String originHost) {
    return readTopology(
      path,
      NetworkSelector.DEFAULT,
      PortSelector.NON_TLS,
      originHost
    );
  }


  /**
   * Helper method to load the config.
   */
  private static ClusterTopology readTopology(
    String path,
    NetworkSelector networkSelector,
    PortSelector portSelector,
    final String originHost
  ) {
    String raw = readResource(path, CouchbaseBucketConfigTranslationTest.class);

    return new TopologyParser(networkSelector, portSelector, StandardMemcachedHashingStrategy.INSTANCE)
      .parse(raw, originHost);
  }

}
