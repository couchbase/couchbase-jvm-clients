/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.client.core.topology;

import com.couchbase.client.core.config.CouchbaseBucketConfigTranslationTest;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.TopologyTestUtils.TestTopologyParser;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalInt;

import static com.couchbase.client.core.topology.BucketCapability.CBHELLO;
import static com.couchbase.client.core.topology.BucketCapability.NODES_EXT;
import static com.couchbase.client.core.topology.TopologyTestUtils.topologyParser;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that parsing various bucket configs works as expected through the
 * {@link com.couchbase.client.core.topology.ClusterTopologyParser}.
 */
public class ClusterTopologyParserTest {
  private static final TestTopologyParser parser = topologyParser()
    .withResourcesFrom(CouchbaseBucketConfigTranslationTest.class);

  @Test
  void canParseMemcached() {
    String origin = "origin.example.com";
    ClusterTopology config = parser
      .withOriginHost(origin)
      .parseResource("config-memcached-7.6.0.json");
    MemcachedBucketTopology bucket = requireMemcachedBucket(config);
    assertEquals("mc", bucket.name());
    assertEquals("995747a3b1cc309a5cf421e1de927124", bucket.uuid());
    assertEquals(setOf(CBHELLO, NODES_EXT), bucket.capabilities());
    assertEquals(listOf(origin), transform(config.nodes(), HostAndServicePorts::host));
    assertSame(config.nodes().get(0), bucket.nodeForKey("xyzzy".getBytes(UTF_8)));
  }

  @Test
  void canParseMemcachedExternal() {
    String externalHostname = "booper";
    ClusterTopology config = parser
      .withOriginHost("origin.example.com")
      .withNetworkSelector(NetworkSelector.EXTERNAL)
      .parseResource("config-memcached-7.6.0.json");
    MemcachedBucketTopology bucket = requireMemcachedBucket(config);
    assertEquals(listOf(externalHostname), transform(config.nodes(), HostAndServicePorts::host));
    assertSame(config.nodes().get(0), bucket.nodeForKey("xyzzy".getBytes(UTF_8)));
  }

  @Test
  void magmaBucketIsNotEphemeral() {
    ClusterTopology config = parser.parseResource("config_magma_two_nodes.json");
    CouchbaseBucketTopology bucket = requireCouchbaseBucket(config);
    assertEquals("foo", bucket.name());
    assertFalse(bucket.ephemeral());
  }

  @Test
  void parsesRevEpoch() {
    ClusterTopology config = parser.parseResource("config_magma_two_nodes.json");
    assertEquals(new TopologyRevision(1, 1017), config.revision());
  }

  @Test
  void shouldReplaceHostPlaceholder() {
    ClusterTopology config = parser
      .withOriginHost("example.com")
      .parseResource("config_with_host_placeholder.json");
    assertEquals("example.com", config.nodes().get(0).host());
  }

  @Test
  void shouldReplaceHostPlaceholderIpv6() {
    ClusterTopology config = parser
      .withOriginHost(new HostAndPort("::1", 0).host())
      .parseResource("config_with_host_placeholder.json");
    assertEquals("0:0:0:0:0:0:0:1", config.nodes().get(0).host());
  }

  @Test
  void shouldGracefullyHandleEmptyPartitions() {
    ClusterTopology config = parser.parseResource("config_with_no_partitions.json");
    CouchbaseBucketTopology bucket = requireCouchbaseBucket(config);
    assertEquals(-2, bucket.nodeIndexForActive(24, false));
    assertEquals(-2, bucket.nodeIndexForReplica(24, 1, false));
    assertFalse(bucket.ephemeral());
  }

  @Test
  void shouldLoadEphemeralBucketConfig() {
    ClusterTopology config = parser.parseResource("ephemeral_bucket_config.json");

    assertTrue(requireCouchbaseBucket(config).ephemeral());
    assertTrue(hasService(config, ServiceType.KV));
    assertTrue(hasService(config, ServiceType.VIEWS));
  }

  private static boolean hasService(ClusterTopology config, ServiceType service) {
    return config.nodes().stream().anyMatch(it -> it.has(service));
  }

  @Test
  void shouldLoadConfigWithSameNodesButDifferentPorts() {
    ClusterTopology config = parser.parseResource("cluster_run_two_nodes_same_host.json");
    CouchbaseBucketTopology bucket = requireCouchbaseBucket(config);
    assertFalse(bucket.ephemeral());
    assertEquals(1, bucket.numberOfReplicas());
    assertEquals(1024, bucket.partitions().size());
    assertEquals(2, config.nodes().size());
    assertEquals("192.168.1.194", config.nodes().get(0).host());
    assertEquals(OptionalInt.of(9000), config.nodes().get(0).port(ServiceType.MANAGER));
    assertEquals("192.168.1.194", config.nodes().get(1).host());
    assertEquals(OptionalInt.of(9001), config.nodes().get(1).port(ServiceType.MANAGER));
  }


  @Test
  void shouldLoadConfigWithIPv6() {
    ClusterTopology config = parser
      .withOriginHost(new HostAndPort("::1", 0).host())
      .parseResource("config_with_ipv6.json");
    CouchbaseBucketTopology bucket = requireCouchbaseBucket(config);

    assertEquals(2, config.nodes().size());
    assertEquals("fd63:6f75:6368:2068:1471:75ff:fe25:a8be", config.nodes().get(0).host());
    assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", config.nodes().get(1).host());

    assertEquals(1, bucket.numberOfReplicas());
    assertEquals(1024, bucket.numberOfPartitions());
  }

  /**
   * This is a regression test. It has been added to make sure a config with a bucket
   * capability that is not known to the client still makes it parse properly.
   */
  @Test
  void shouldIgnoreUnknownBucketCapabilities() {
    ClusterTopology config = parser.parseResource("config_with_invalid_capability.json");
    assertEquals(1, config.nodes().size());
  }

  /**
   * This test makes sure that the external hosts are present if set.
   */
  @Test
  void shouldIncludeExternalIfPresent() {
    ClusterTopology config = parser
      .withNetworkSelector(NetworkSelector.EXTERNAL)
      .parseResource("config_with_external.json");

    List<HostAndServicePorts> nodes = config.nodes();
    assertEquals(3, nodes.size());
    assertEquals(NetworkResolution.EXTERNAL, config.network());

    for (HostAndServicePorts node : nodes) {
      assertFalse(node.inaccessible());
      assertFalse(node.ports().isEmpty());
      for (int port : node.ports().values()) {
        assertTrue(port > 0);
      }
    }

    assertEquals(32790, nodes.get(0).port(ServiceType.MANAGER).orElse(0));

    // Ketama authority is always host and non-TLS KV port from "default" network,
    // regardless of the port selector.
    List<HostAndPort> expectedKetamaAuthorities = listOf(
      new HostAndPort("172.17.0.2", 11210),
      new HostAndPort("172.17.0.3", 11210),
      new HostAndPort("172.17.0.4", 11210)
    );

    assertEquals(
      expectedKetamaAuthorities,
      transform(config.nodes(), HostAndServicePorts::ketamaAuthority)
    );

    // Again, TLS port this time
    config = parser
      .withNetworkSelector(NetworkSelector.EXTERNAL)
      .withPortSelector(PortSelector.TLS)
      .parseResource("config_with_external.json");
    assertEquals(32773, config.nodes().get(0).port(ServiceType.MANAGER).orElse(0));

    // Ketama authority is the same for non-TLS and TLS port selector.
    assertEquals(
      expectedKetamaAuthorities,
      transform(config.nodes(), HostAndServicePorts::ketamaAuthority)
    );
  }

  /**
   * It's fine if this string format changes, but it would be nice to keep it
   * relatively compact and human-readable.
   */
  @Test
  void partitionMapHasCompactStringRepresentation() {
    ClusterTopologyWithBucket config = parser.parseResource("config_with_external.json").requireBucket();
    CouchbaseBucketTopology bucket = (CouchbaseBucketTopology) config.bucket();
    assertEquals(
      "{0..170=[0,1], 171..341=[0,2], 342..512=[1,0], 513..682=[1,2], 683..853=[2,0], 854..1023=[2,1]}",
      bucket.partitions().toString()
    );
  }

  @Test
  void nodeIdsComeFromInternalNetwork() {
    String originHost = "private-endpoint.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com";

    ClusterTopology config = parser
      .withOriginHost(originHost)
      .withPortSelector(PortSelector.TLS)
      .withNetworkSelector(NetworkSelector.autoDetect(setOf(SeedNode.create(originHost).withKvPort(11208))))
      .parseResource("config_7.6_external_manager_ports_not_unique.json");

    assertEquals(NetworkResolution.EXTERNAL, config.network());

    // This config has the same external host for all nodes
    String externalHost = originHost;

    List<NodeIdentifier> nodeIds = transform(config.nodes(), HostAndServicePorts::id);
    assertEquals(
      listOf(
        new NodeIdentifier("svc-dqisea-node-001.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091, externalHost),
        new NodeIdentifier("svc-dqisea-node-002.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091, externalHost),
        new NodeIdentifier("svc-dqisea-node-003.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091, externalHost),
        new NodeIdentifier("svc-dqisea-node-004.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091, externalHost),
        new NodeIdentifier("svc-dqisea-node-005.nyarjaj-crhge67o.sandbox.nonprod-project-avengers.com", 8091, externalHost)
      ),
      nodeIds
    );

    // external host is not part of equals, so:
    assertEquals(
      listOf(externalHost, externalHost, externalHost, externalHost, externalHost),
      transform(nodeIds, NodeIdentifier::hostForNetworkConnections)
    );
  }

  @Test
  void serverGroupIsNullIfAbsent() {
    ClusterTopology config = parser.parseResource("config_7.2.2_1kv_2query.json");
    config.nodes().forEach(node -> assertNull(node.serverGroup()));
  }

  @Test
  void shouldParseServerGroupsIfPresent() {
    ClusterTopology config = parser.parseResource("config_7.6.4_server_groups.json");
    assertEquals(
      mapOf(
        "192.168.106.128", "Group 1",
        "192.168.106.129", "Group 1",
        "192.168.106.130", "Group 2"
      ),
      config.nodes().stream()
        .collect(toMap(
          HostAndServicePorts::host,
          hostAndServicePorts -> requireNonNull(hostAndServicePorts.serverGroup(), "Missing server group: " + hostAndServicePorts)
        ))
    );
  }

  @Test
  void appTelemetryPathIsNullIfAbsent() {
    ClusterTopology config = parser.parseResource("config_7.2.2_1kv_2query.json");
    config.nodes().forEach(node -> assertNull(node.appTelemetryPath()));
  }

  @Test
  void shouldParseAppTelemetryPathIfPresent() {
    // TODO: replace with config from actual 8.0.0 build
    ClusterTopology config = parser.parseResource("config_8.0.0-prerelease_app_telemetry.json");
    assertEquals(
      mapOf(
        "192.168.106.128", "/foo",
        "192.168.106.129", "/bar",
        "192.168.106.130", "/zot"
      ),
      config.nodes().stream()
        .collect(toMap(
          HostAndServicePorts::host,
          hostAndServicePorts -> requireNonNull(hostAndServicePorts.appTelemetryPath(), "Missing app telemetry path: " + hostAndServicePorts)
        ))
    );
  }

  public CouchbaseBucketTopology requireCouchbaseBucket(ClusterTopology cluster) {
    try {
      return (CouchbaseBucketTopology) cluster.requireBucket().bucket();
    } catch (Exception e) {
      throw new NoSuchElementException("cluster topology has no couchbase bucket");
    }
  }

  public MemcachedBucketTopology requireMemcachedBucket(ClusterTopology cluster) {
    try {
      return (MemcachedBucketTopology) cluster.requireBucket().bucket();
    } catch (Exception e) {
      throw new NoSuchElementException("cluster topology has no memcached bucket");
    }
  }
}
