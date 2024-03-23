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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalInt;

import static com.couchbase.client.core.topology.BucketCapability.CBHELLO;
import static com.couchbase.client.core.topology.BucketCapability.NODES_EXT;
import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.test.Util.readResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that parsing various bucket configs works as expected through the
 * {@link com.couchbase.client.core.topology.ClusterTopologyParser}.
 */
public class ClusterTopologyParserTest {

  @Test
  void canParseMemcached() {
    String origin = "origin.example.com";
    ClusterTopology config = read("config-memcached-7.6.0.json", origin);
    MemcachedBucketTopology bucket = requireMemcachedBucket(config);
    assertEquals("mc", bucket.name());
    assertEquals("995747a3b1cc309a5cf421e1de927124", bucket.uuid());
    assertEquals(setOf(CBHELLO, NODES_EXT), bucket.capabilities());
    assertEquals(listOf(origin), transform(config.nodes(), HostAndServicePorts::host));
    assertSame(config.nodes().get(0), bucket.nodeForKey("xyzzy".getBytes(UTF_8)));
  }

  @Test
  void canParseMemcachedExternal() {
    String origin = "origin.example.com";
    String externalHostname = "booper";
    ClusterTopology config = read("config-memcached-7.6.0.json", origin, PortSelector.NON_TLS, NetworkSelector.EXTERNAL);
    MemcachedBucketTopology bucket = requireMemcachedBucket(config);
    assertEquals(listOf(externalHostname), transform(config.nodes(), HostAndServicePorts::host));
    assertSame(config.nodes().get(0), bucket.nodeForKey("xyzzy".getBytes(UTF_8)));
  }

  @Test
  void magmaBucketIsNotEphemeral() {
    ClusterTopology config = read("config_magma_two_nodes.json");
    CouchbaseBucketTopology bucket = requireCouchbaseBucket(config);
    assertEquals("foo", bucket.name());
    assertFalse(bucket.ephemeral());
  }

  @Test
  void parsesRevEpoch() {
    ClusterTopology config = read("config_magma_two_nodes.json");
    assertEquals(new TopologyRevision(1, 1017), config.revision());
  }

  @Test
  void shouldReplaceHostPlaceholder() {
    ClusterTopology config = read("config_with_host_placeholder.json", "example.com");
    assertEquals("example.com", config.nodes().get(0).host());
  }

  @Test
  void shouldReplaceHostPlaceholderIpv6() {
    ClusterTopology config = read("config_with_host_placeholder.json", new HostAndPort("::1", 0).host());
    assertEquals("0:0:0:0:0:0:0:1", config.nodes().get(0).host());
  }

  @Test
  void shouldGracefullyHandleEmptyPartitions() {
    ClusterTopology config = read("config_with_no_partitions.json");
    CouchbaseBucketTopology bucket = requireCouchbaseBucket(config);
    assertEquals(-2, bucket.nodeIndexForActive(24, false));
    assertEquals(-2, bucket.nodeIndexForReplica(24, 1, false));
    assertFalse(bucket.ephemeral());
  }

  @Test
  void shouldLoadEphemeralBucketConfig() {
    ClusterTopology config = read("ephemeral_bucket_config.json");

    assertTrue(requireCouchbaseBucket(config).ephemeral());
    assertTrue(hasService(config, ServiceType.KV));
    assertTrue(hasService(config, ServiceType.VIEWS));
  }

  private static boolean hasService(ClusterTopology config, ServiceType service) {
    return config.nodes().stream().anyMatch(it -> it.has(service));
  }

  @Test
  void shouldLoadConfigWithSameNodesButDifferentPorts() {
    ClusterTopology config = read("cluster_run_two_nodes_same_host.json");
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
    ClusterTopology config = read("config_with_ipv6.json", new HostAndPort("::1", 0).host());
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
    ClusterTopology config = read("config_with_invalid_capability.json");
    assertEquals(1, config.nodes().size());
  }

  /**
   * This test makes sure that the external hosts are present if set.
   */
  @Test
  void shouldIncludeExternalIfPresent() {
    ClusterTopology config = read("config_with_external.json", "127.0.0.1", PortSelector.NON_TLS, NetworkSelector.EXTERNAL);

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
    config = read("config_with_external.json", "127.0.0.1", PortSelector.TLS, NetworkSelector.EXTERNAL);
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
    ClusterTopologyWithBucket config = read("config_with_external.json").requireBucket();
    CouchbaseBucketTopology bucket = (CouchbaseBucketTopology) config.bucket();
    assertEquals(
      "{0..170=[0,1], 171..341=[0,2], 342..512=[1,0], 513..682=[1,2], 683..853=[2,0], 854..1023=[2,1]}",
      bucket.partitions().toString()
    );
  }

  private static ClusterTopology read(String resourceName) {
    return read(resourceName, "127.0.0.1");
  }

  private static ClusterTopology read(String resourceName, String originHost) {
    return read(resourceName, originHost, PortSelector.NON_TLS, NetworkSelector.DEFAULT);
  }

  private static ClusterTopology read(
    String resourceName,
    String originHost,
    PortSelector portSelector,
    NetworkSelector networkSelector
  ) {
    String json = readResource(resourceName, CouchbaseBucketConfigTranslationTest.class);
    return ClusterTopologyParser.parse(
      (ObjectNode) Mapper.decodeIntoTree(json),
      originHost,
      portSelector,
      networkSelector,
      StandardMemcachedHashingStrategy.INSTANCE
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
