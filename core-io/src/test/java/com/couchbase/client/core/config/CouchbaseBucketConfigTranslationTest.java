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

import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.NetworkSelector;
import com.couchbase.client.core.topology.PortSelector;
import com.couchbase.client.core.topology.TopologyParser;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.config.BucketCapabilities.CBHELLO;
import static com.couchbase.client.core.config.BucketCapabilities.CCCP;
import static com.couchbase.client.core.config.BucketCapabilities.COUCHAPI;
import static com.couchbase.client.core.config.BucketCapabilities.DCP;
import static com.couchbase.client.core.config.BucketCapabilities.NODES_EXT;
import static com.couchbase.client.core.config.BucketCapabilities.TOUCH;
import static com.couchbase.client.core.config.BucketCapabilities.XATTR;
import static com.couchbase.client.core.config.BucketCapabilities.XDCR_CHECKPOINTING;
import static com.couchbase.client.core.service.ServiceType.ANALYTICS;
import static com.couchbase.client.core.service.ServiceType.KV;
import static com.couchbase.client.core.service.ServiceType.MANAGER;
import static com.couchbase.client.core.service.ServiceType.QUERY;
import static com.couchbase.client.core.service.ServiceType.SEARCH;
import static com.couchbase.client.core.service.ServiceType.VIEWS;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.test.Util.readResource;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the result of translating a {@link ClusterTopology}
 * into a legacy {@link CouchbaseBucketConfig}.
 */
public class CouchbaseBucketConfigTranslationTest {

  @Test
  void shouldHavePrimaryPartitionsOnNode() {
    CouchbaseBucketConfig config = readConfig("config_7.6.0_2kv_but_only_1_has_active_partitions.json");

    assertTrue(config.hasPrimaryPartitionsOnNode("1.2.3.4"));
    assertFalse(config.hasPrimaryPartitionsOnNode("2.3.4.5"));
    assertEquals(BucketNodeLocator.VBUCKET, config.locator());
  }

  @Test
  void shouldHaveVbucketLocator() {
    CouchbaseBucketConfig config = readConfig("config_7.6.0_2kv_but_only_1_has_active_partitions.json");
    assertEquals(BucketNodeLocator.VBUCKET, config.locator());
  }

  @Test
  void shouldNotBeEphemeral() {
    CouchbaseBucketConfig config = readConfig("config_7.6.0_2kv_but_only_1_has_active_partitions.json");
    assertFalse(config.ephemeral());
  }

  @Disabled("Obsolete: nodesExt entry is missing 'hostname' field.")
  @Test
  void shouldFallbackToNodeHostnameIfNotInNodesExt() {
    CouchbaseBucketConfig config = readConfig("nodes_ext_without_hostname.json");

    String expected = "1.2.3.4";
    assertEquals(1, config.nodes().size());
    assertEquals(expected, config.nodes().get(0).hostname());
  }

  @Test
  void shouldGracefullyHandleEmptyPartitions() {
    CouchbaseBucketConfig config = readConfig("config_with_no_partitions.json");

    assertEquals(CouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForActive(24, false));
    assertEquals(CouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForReplica(24, 1, false));
  }

  @Test
  void shouldLoadEphemeralBucketConfig() {
    CouchbaseBucketConfig config = readConfig("ephemeral_bucket_config.json");

    assertTrue(config.ephemeral());
    assertTrue(config.serviceEnabled(KV));
    assertFalse(config.serviceEnabled(VIEWS));
  }

  @Disabled("Obsolete: missing 'nodesExt' field")
  @Test
  void shouldLoadConfigWithoutBucketCapabilities() {
    CouchbaseBucketConfig config = readConfig("config_without_capabilities.json");

    assertEquals(0, config.numberOfReplicas());
    assertEquals(64, config.numberOfPartitions());
    assertEquals(2, config.nodes().size());
    assertTrue(config.serviceEnabled(KV));
    assertTrue(config.serviceEnabled(VIEWS));
  }

  @Test
  void shouldLoadConfigWithSameNodesButDifferentPorts() {
    CouchbaseBucketConfig config = readConfig("cluster_run_two_nodes_same_host.json");

    assertFalse(config.ephemeral());
    assertEquals(1, config.numberOfReplicas());
    assertEquals(1024, config.numberOfPartitions());
    assertEquals(2, config.nodes().size());
    assertEquals("192.168.1.194", config.nodes().get(0).hostname());
    assertEquals(9000, (int) config.nodes().get(0).services().get(MANAGER));
    assertEquals("192.168.1.194", config.nodes().get(1).hostname());
    assertEquals(9001, (int) config.nodes().get(1).services().get(MANAGER));

    assertEquals("192.168.1.194", config.nodeAtIndex(0).hostname());
    assertEquals(12000, config.nodeAtIndex(0).services().get(KV));
    assertEquals("192.168.1.194", config.nodeAtIndex(1).hostname());
    assertEquals(12002, config.nodeAtIndex(1).services().get(KV));
  }

  @Test
  void shouldLoadConfigWithMDS() {
    CouchbaseBucketConfig config = readConfig("config_7.2.2_1kv_2query.json");

    assertEquals(
      listOf(
        "192.168.106.130",
        "192.168.106.128",
        "192.168.106.129"
      ),
      transform(config.nodes(), NodeInfo::hostname)
    );

    assertEquals(
      listOf(
        setOf(KV, VIEWS, MANAGER),
        setOf(QUERY, MANAGER),
        setOf(QUERY, MANAGER)
      ),
      transform(config.nodes(), node -> node.services().keySet())
    );

    assertEquals(
      "192.168.106.130",
      config.nodeAtIndex(0).hostname()
    );

    assertThrows(IndexOutOfBoundsException.class, () -> config.nodeAtIndex(1).hostname());
  }

  @Test
  void shouldLoadConfigWithIPv6() {
    CouchbaseBucketConfig config = readConfig("config_with_ipv6.json");

    assertEquals(2, config.nodes().size());
    assertEquals("fd63:6f75:6368:2068:1471:75ff:fe25:a8be", config.nodes().get(0).hostname());
    assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", config.nodes().get(1).hostname());

    assertEquals(1, config.numberOfReplicas());
    assertEquals(1024, config.numberOfPartitions());

    assertEquals("fd63:6f75:6368:2068:1471:75ff:fe25:a8be", config.nodeAtIndex(0).hostname());
    assertEquals(11210, config.nodeAtIndex(0).services().get(KV));
    assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", config.nodeAtIndex(1).hostname());
    assertEquals(11210, config.nodeAtIndex(1).services().get(KV));
  }

  @Test
  void shouldLoadConfigWithIPv6FromHost() {
    assertThrows(
      IllegalArgumentException.class,
      () -> readConfig("single_node_wildcard.json", "[fd63:6f75:6368:20d4:423d:37c3:e6f7:3fac]")
    );

    CouchbaseBucketConfig config = readConfig("single_node_wildcard.json", "fd63:6f75:6368:20d4:423d:37c3:e6f7:3fac");
    assertEquals("fd63:6f75:6368:20d4:423d:37c3:e6f7:3fac", config.nodeAtIndex(0).hostname());
  }

  /**
   * This is a regression test. It has been added to make sure a config with a bucket
   * capability that is not known to the client still makes it parse properly.
   */
  @Test
  void shouldIgnoreUnknownBucketCapabilities() {
    CouchbaseBucketConfig config = readConfig("config_with_invalid_capability.json");
    assertEquals(1, config.nodes().size());
    assertEquals(
      setOf(CBHELLO, TOUCH, COUCHAPI, CCCP, XDCR_CHECKPOINTING, NODES_EXT, DCP, XATTR),
      config.bucketCapabilities()
    );
  }

  @Test
  void shouldReadBucketUuid() {
    CouchbaseBucketConfig config = readConfig("config_7.6.0_2kv_but_only_1_has_active_partitions.json");
    assertEquals("f9a2d8ec38b00e67a885eb3571f47e42", config.uuid());
  }

  @Disabled("Obsolete: missing 'nodesExt' field")
  @Test
  void shouldHandleMissingBucketUuid() {
    CouchbaseBucketConfig config = readConfig("config_without_uuid.json");
    assertNull(config.uuid());
  }

  @Test
  void magmaBucketIsNotEphemeral() {
    CouchbaseBucketConfig config = readConfig("config_magma_two_nodes.json");
    assertEquals("foo", config.name());
    assertFalse(config.ephemeral());
  }

  /**
   * This test makes sure that we are properly extracting the cluster capabilities from the config
   * section while keeping all the other services empty so it is safe to look up (and not returning
   * null or anything).
   */
  @Test
  void shouldDetectClusterCapabilities() {
    CouchbaseBucketConfig config = readConfig("cluster_run_cluster_capabilities.json");
    Map<ServiceType, Set<ClusterCapabilities>> cc = config.clusterCapabilities();

    assertEquals(1, cc.get(QUERY).size());
    assertTrue(cc.get(QUERY).contains(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS));

    assertTrue(cc.get(KV).isEmpty());
    assertTrue(cc.get(SEARCH).isEmpty());
    assertTrue(cc.get(ANALYTICS).isEmpty());
    assertTrue(cc.get(MANAGER).isEmpty());
    assertTrue(cc.get(VIEWS).isEmpty());
  }

  @Test
  void shouldUseExternalTlsIfSelected() {
    CouchbaseBucketConfig config = readConfig(
      "config_with_external.json",
      NetworkSelector.EXTERNAL,
      PortSelector.TLS,
      "origin.example.com"
    );

    List<NodeInfo> nodes = config.nodes();
    assertEquals(3, nodes.size());
    for (NodeInfo node : nodes) {
      // should have been pre-resolved
      assertEquals(emptyMap(), node.alternateAddresses());
    }
    NodeInfo node = nodes.get(0);
    assertEquals("192.168.132.234", node.hostname());
    assertEquals(
      mapOf(
        MANAGER, 32773,
        VIEWS, 32772,
        QUERY, 32771,
        SEARCH, 32770,
        KV, 32776
      ),
      node.sslServices()
    );
    assertEquals(emptyMap(), node.services());
  }

  @Test
  void shouldIgnoreExternalIfNotSelected() {
    CouchbaseBucketConfig config = readConfig("config_with_external.json");

    List<NodeInfo> nodes = config.nodes();
    assertEquals(3, nodes.size());
    for (NodeInfo node : nodes) {
      // should have been pre-resolved
      assertEquals(emptyMap(), node.alternateAddresses());
    }
    NodeInfo node = nodes.get(0);
    assertEquals("172.17.0.2", node.hostname());
    assertEquals(
      mapOf(
        MANAGER, 8091,
        VIEWS, 8092,
        QUERY, 8093,
        SEARCH, 8094,
        KV, 11210
      ),
      node.services()
    );
    assertEquals(emptyMap(), node.sslServices());
  }

  @Test
  void shouldParseElixirConfig() {
    CouchbaseBucketConfig config = readConfig("cloud_tls_only.json", PortSelector.TLS);
    assertEquals("database_hSeAfu", config.name());
    assertEquals(64, config.numberOfPartitions());
    assertEquals(0, config.numberOfReplicas());

    assertEquals(12, config.nodes().size());

    Set<String> kvNodes = new HashSet<>();
    Set<String> queryNodes = new HashSet<>();
    for (NodeInfo ni : config.nodes()) {
      assertTrue(ni.services().isEmpty());

      Map<ServiceType, Integer> sslServices = ni.sslServices();
      assertTrue(sslServices.get(MANAGER) > 0);

      Integer kvPort = sslServices.get(KV);
      if (kvPort != null) {
        kvNodes.add(ni.hostname() + ":" + kvPort);
        assertTrue(kvPort > 0);
      }

      Integer queryPort = sslServices.get(QUERY);
      if (queryPort != null) {
        queryNodes.add(ni.hostname() + ":" + queryPort);
        assertTrue(queryPort > 0);
      }
    }

    Set<String> expectedKv = new HashSet<>();
    expectedKv.add("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com:11207");
    expectedKv.add("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com:21001");
    expectedKv.add("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com:21002");
    assertEquals(expectedKv, kvNodes);

    Set<String> expectedQuery = new HashSet<>();
    expectedQuery.add("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com:18093");
    expectedQuery.add("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com:23001");
    expectedQuery.add("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com:23002");
    assertEquals(expectedQuery, queryNodes);

    assertEquals("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com", config.nodeAtIndex(0).hostname());
    assertEquals(21001, config.nodeAtIndex(0).sslServices().get(KV));
    assertEquals("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com", config.nodeAtIndex(1).hostname());
    assertEquals(21002, config.nodeAtIndex(1).sslServices().get(KV));
    assertEquals("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com", config.nodeAtIndex(2).hostname());
    assertEquals(11207, config.nodeAtIndex(2).sslServices().get(KV));
  }

  @Test
  void nodeIdsComeFromInternalNetwork() {
    String originHost = "private-endpoint.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com";

    ClusterTopologyWithBucket topology = readTopology(
      "config_7.6_external_manager_ports_not_unique_with_bucket.json",
      NetworkSelector.autoDetect(setOf(SeedNode.create(originHost).withKvPort(11208))),
      PortSelector.TLS,
      originHost
    );

    assertEquals(NetworkResolution.EXTERNAL, topology.network());

    CouchbaseBucketConfig config = new CouchbaseBucketConfig(topology);

    List<NodeIdentifier> expectedIds = listOf(
      new NodeIdentifier("svc-dqisea-node-001.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091),
      new NodeIdentifier("svc-dqisea-node-002.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091),
      new NodeIdentifier("svc-dqisea-node-004.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091),
      new NodeIdentifier("svc-dqisea-node-005.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091),
      // Node 003 comes last because it's not servicing the bucket.
      new NodeIdentifier("svc-dqisea-node-003.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091)
    );

    assertEquals(
      expectedIds,
      transform(config.portInfos(), PortInfo::identifier)
    );

    assertEquals(
      expectedIds,
      transform(config.nodes(), NodeInfo::identifier)
    );
  }

  private static CouchbaseBucketConfig readConfig(final String path) {
    return readConfig(path, "origin.example.com");
  }

  private static CouchbaseBucketConfig readConfig(final String path, PortSelector portSelector) {
    return (CouchbaseBucketConfig) LegacyConfigHelper.toLegacyBucketConfig(
      readTopology(
        path,
        NetworkSelector.DEFAULT,
        portSelector,
        "origin.example.com"
      )
    );
  }

  /**
   * Helper method to load the config.
   */
  private static CouchbaseBucketConfig readConfig(final String path, final String origin) {
    return (CouchbaseBucketConfig) LegacyConfigHelper.toLegacyBucketConfig(readTopology(path, origin));
  }

  private static ClusterTopologyWithBucket readTopology(String path, String originHost) {
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
  private static CouchbaseBucketConfig readConfig(
    String path,
    NetworkSelector networkSelector,
    PortSelector portSelector,
    final String originHost
  ) {
    return (CouchbaseBucketConfig) LegacyConfigHelper.toLegacyBucketConfig(readTopology(path, networkSelector, portSelector, originHost));
  }

  /**
   * Helper method to load the config.
   */
  private static ClusterTopologyWithBucket readTopology(
    String path,
    NetworkSelector networkSelector,
    PortSelector portSelector,
    final String originHost
  ) {
    String raw = readResource(path, CouchbaseBucketConfigTranslationTest.class);

    return new TopologyParser(networkSelector, portSelector, StandardMemcachedHashingStrategy.INSTANCE)
      .parse(raw, originHost)
      .requireBucket();
  }
}
