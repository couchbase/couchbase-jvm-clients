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

import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.NetworkSelector;
import com.couchbase.client.core.topology.PortSelector;
import com.couchbase.client.core.topology.TopologyParser;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.test.Util.readResource;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the result of translating a {@link ClusterTopology}
 * into a legacy {@link MemcachedBucketConfig}.
 */
class MemcachedBucketConfigTranslationTest {

  /**
   * The config loaded has 4 nodes, but only two are data nodes. This tests checks that the ketama
   * nodes are only populated for those two nodes which include the binary service type.
   */
  @Test
  void shouldOnlyUseDataNodesForKetama() {
    MemcachedBucketConfig config = readConfig("memcached_mixed_sherlock.json");

    assertEquals(0, config.revEpoch());
    assertEquals(4, config.nodes().size());
    for (Map.Entry<Long, NodeInfo> node : config.ketamaRing().toMap().entrySet()) {
      String hostname = node.getValue().hostname();
      assertTrue(hostname.equals("192.168.56.101") || hostname.equals("192.168.56.102"));
      assertTrue(node.getValue().services().containsKey(ServiceType.KV));
    }
  }

  @Test
  void shouldLoadConfigWithIPv6() {
    MemcachedBucketConfig config = readConfig("memcached_with_ipv6.json");

    assertEquals(2, config.nodes().size());
    for (Map.Entry<Long, NodeInfo> node : config.ketamaRing().toMap().entrySet()) {
      String hostname = node.getValue().hostname();
      assertTrue(hostname.equals("fd63:6f75:6368:2068:1471:75ff:fe25:a8be")
        || hostname.equals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7"));
      assertTrue(node.getValue().services().containsKey(ServiceType.KV));
    }
  }

  @Test
  void shouldReadBucketUuid() {
    MemcachedBucketConfig config = readConfig("memcached_mixed_sherlock.json");
    assertEquals("7b6c811c94f985b685d99596816a7a9f", config.uuid());
  }

  @Test
  void shouldHandleMissingBucketUuid() {
    MemcachedBucketConfig config = readConfig("memcached_without_uuid.json");
    assertEquals("", config.uuid());
  }

  /**
   * This test makes sure that the external hosts are present if set.
   */
  @Test
  void shouldUseExternalAddresses() {

    ClusterTopologyWithBucket cluster = readTopology(
      "config_with_external_memcache.json",
      NetworkSelector.EXTERNAL,
      PortSelector.NON_TLS,
      "origin.example.com"
    );

    MemcachedBucketConfig config = (MemcachedBucketConfig) LegacyConfigHelper.toLegacyBucketConfig(cluster);

    List<NodeInfo> nodes = config.nodes();
    assertEquals(3, nodes.size());

    assertEquals("192.168.132.234", nodes.get(0).hostname());
    assertEquals(emptyMap(), nodes.get(0).sslServices());
    assertEquals(
      mapOf(
        ServiceType.MANAGER, 32790,
        ServiceType.SEARCH, 32787,
        ServiceType.KV, 32775,
//        ServiceType.VIEWS, 32789,
        ServiceType.QUERY, 32788
      ),
      nodes.get(0).services()
    );

    // Ketama authority is always host and non-TLS KV port from "default" network,
    // regardless of the port selector.
    List<HostAndPort> expectedKetamaAuthorities = listOf(
      new HostAndPort("172.17.0.2", 11210),
      new HostAndPort("172.17.0.3", 11210),
      new HostAndPort("172.17.0.4", 11210)
    );

    assertEquals(
      expectedKetamaAuthorities,
      transform(config.nodes(), NodeInfo::ketamaAuthority)
    );
  }

  @Test
  void shouldOnlyTakeNodesArrayIntoAccount() {
    MemcachedBucketConfig config = readConfig("memcached_during_rebalance.json");

    assertEquals(0, config.revEpoch());
    List<String> mustContain = Arrays.asList(
      "10.0.0.1",
      "10.0.0.2",
      "10.0.0.3"
    );
    List<String> mustNotContain = Collections.singletonList("10.0.0.4");

    Collection<NodeInfo> actualRingNodes = config.ketamaRing().toMap().values();
    for (NodeInfo nodeInfo : actualRingNodes) {
      String actual = nodeInfo.hostname();
      assertTrue(mustContain.contains(actual));
      assertFalse(mustNotContain.contains(actual));
    }
  }

  /**
   * Helper method to load the config.
   */
  private static MemcachedBucketConfig readConfig(final String path) {
    return (MemcachedBucketConfig) LegacyConfigHelper.toLegacyBucketConfig(readTopology(path));
  }

  private static ClusterTopologyWithBucket readTopology(String path) {
    return readTopology(
      path,
      NetworkSelector.DEFAULT,
      PortSelector.NON_TLS,
      "origin.example.com"
    );
  }

  /**
   * Helper method to load the config.
   */
  private static ClusterTopologyWithBucket readTopology(
    String path,
    NetworkSelector networkSelector,
    PortSelector portSelector,
    String originHost
  ) {
    String raw = readResource(path, CouchbaseBucketConfigTest.class);

    return new TopologyParser(networkSelector, portSelector, StandardMemcachedHashingStrategy.INSTANCE)
      .parse(raw, originHost)
      .requireBucket();
  }
}
