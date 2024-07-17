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
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.test.Util.readResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that parsing various bucket configs works as expected through the
 * {@link BucketConfigParser}.
 */
class CouchbaseBucketConfigTest {

    @Test
    void shouldHavePrimaryPartitionsOnNode() {
        CouchbaseBucketConfig config = readConfig("config_with_mixed_partitions.json");

        assertTrue(config.hasPrimaryPartitionsOnNode(new NodeIdentifier("1.2.3.4", 8091)));
        assertFalse(config.hasPrimaryPartitionsOnNode(new NodeIdentifier("2.3.4.5", 8091)));
        assertTrue(config.hasPrimaryPartitionsOnNode("1.2.3.4"));
        assertFalse(config.hasPrimaryPartitionsOnNode("2.3.4.5"));
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
        assertTrue(config.nodes().get(0).alternateAddresses().isEmpty());
        assertEquals(0, config.revEpoch());
    }

    @Test
    void shouldFallbackToNodeHostnameIfNotInNodesExt() {
        CouchbaseBucketConfig config = readConfig("nodes_ext_without_hostname.json");

        String expected = "1.2.3.4";
        assertEquals(1, config.nodes().size());
        assertEquals(expected, config.nodes().get(0).hostname());
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
    }

    @Test
    void shouldGracefullyHandleEmptyPartitions() {
        CouchbaseBucketConfig config = readConfig("config_with_no_partitions.json");

        assertEquals(CouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForActive(24, false));
        assertEquals(CouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForReplica(24, 1, false));
        assertFalse(config.ephemeral());
    }

    @Test
    void shouldLoadEphemeralBucketConfig() {
        CouchbaseBucketConfig config = readConfig("ephemeral_bucket_config.json");

        assertTrue(config.ephemeral());
        assertTrue(config.serviceEnabled(ServiceType.KV));
        assertFalse(config.serviceEnabled(ServiceType.VIEWS));
    }

    @Test
    void shouldLoadConfigWithoutBucketCapabilities() {
        CouchbaseBucketConfig config = readConfig("config_without_capabilities.json");

        assertFalse(config.ephemeral());
        assertEquals(0, config.numberOfReplicas());
        assertEquals(64, config.numberOfPartitions());
        assertEquals(2, config.nodes().size());
        assertTrue(config.serviceEnabled(ServiceType.KV));
        assertTrue(config.serviceEnabled(ServiceType.VIEWS));
    }

    @Test
    void shouldLoadConfigWithSameNodesButDifferentPorts() {
        CouchbaseBucketConfig config = readConfig("cluster_run_two_nodes_same_host.json");

        assertFalse(config.ephemeral());
        assertEquals(1, config.numberOfReplicas());
        assertEquals(1024, config.numberOfPartitions());
        assertEquals(2, config.nodes().size());
        assertEquals("192.168.1.194", config.nodes().get(0).hostname());
        assertEquals(9000, (int)config.nodes().get(0).services().get(ServiceType.MANAGER));
        assertEquals("192.168.1.194", config.nodes().get(1).hostname());
        assertEquals(9001, (int)config.nodes().get(1).services().get(ServiceType.MANAGER));

        assertEquals("192.168.1.194", config.nodeAtIndex(0).hostname());
        assertEquals(12000, config.nodeAtIndex(0).services().get(ServiceType.KV));
        assertEquals("192.168.1.194", config.nodeAtIndex(1).hostname());
        assertEquals(12002, config.nodeAtIndex(1).services().get(ServiceType.KV));
    }

    @Test
    void shouldLoadConfigWithMDS() {
        CouchbaseBucketConfig config = readConfig("cluster_run_three_nodes_mds_with_localhost.json");

        assertEquals(3, config.nodes().size());
        assertEquals("192.168.0.102", config.nodes().get(0).hostname());
        assertEquals("127.0.0.1", config.nodes().get(1).hostname());
        assertEquals("127.0.0.1", config.nodes().get(2).hostname());
        assertTrue(config.nodes().get(0).services().containsKey(ServiceType.KV));
        assertTrue(config.nodes().get(1).services().containsKey(ServiceType.KV));
        assertFalse(config.nodes().get(2).services().containsKey(ServiceType.KV));

        assertEquals("192.168.0.102", config.nodeAtIndex(0).hostname());
        assertEquals(12000, config.nodeAtIndex(0).services().get(ServiceType.KV));
        assertEquals("127.0.0.1", config.nodeAtIndex(1).hostname());
        assertEquals(12002, config.nodeAtIndex(1).services().get(ServiceType.KV));
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
        assertEquals(11210, config.nodeAtIndex(0).services().get(ServiceType.KV));
        assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", config.nodeAtIndex(1).hostname());
        assertEquals(11210, config.nodeAtIndex(1).services().get(ServiceType.KV));
    }

    @Test
    void shouldLoadConfigWithIPv6FromHost() {
        CouchbaseBucketConfig config = readConfig("single_node_wildcard.json", "[fd63:6f75:6368:20d4:423d:37c3:e6f7:3fac]");
        assertEquals("fd63:6f75:6368:20d4:423d:37c3:e6f7:3fac", config.nodeAtIndex(0).hostname());

        config = readConfig("single_node_wildcard.json", "fd63:6f75:6368:20d4:423d:37c3:e6f7:3fac");
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
    }

    @Test
    void shouldReadBucketUuid() {
        CouchbaseBucketConfig config = readConfig("config_with_mixed_partitions.json");
        assertEquals("aa4b515529fa706f1e5f09f21abb5c06", config.uuid());
    }

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

        assertEquals(1, cc.get(ServiceType.QUERY).size());
        assertTrue(cc.get(ServiceType.QUERY).contains(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS));

        assertTrue(cc.get(ServiceType.KV).isEmpty());
        assertTrue(cc.get(ServiceType.SEARCH).isEmpty());
        assertTrue(cc.get(ServiceType.ANALYTICS).isEmpty());
        assertTrue(cc.get(ServiceType.MANAGER).isEmpty());
        assertTrue(cc.get(ServiceType.VIEWS).isEmpty());
    }

    @Test
    void partitionMapHasCompactStringRepresentation() {
        CouchbaseBucketConfig config = readConfig("config_with_external.json");

        assertThat(config.toString())
            .contains("partitions={0..170=[0,1], 171..341=[0,2], 342..512=[1,0], 513..682=[1,2], 683..853=[2,0], 854..1023=[2,1]}");
    }

    /**
     * This test makes sure that the external hosts are present if set.
     */
    @Test
    void shouldIncludeExternalIfPresent() {
        CouchbaseBucketConfig config = readConfig("config_with_external.json");

        assertEquals(0, config.revEpoch());
        List<NodeInfo> nodes = config.nodes();
        assertEquals(3, nodes.size());
        for (NodeInfo node : nodes) {
            Map<String, AlternateAddress> addrs = node.alternateAddresses();
            assertEquals(1, addrs.size());
            AlternateAddress addr = addrs.get(NetworkResolution.EXTERNAL.name());
            assertNotNull(addr.hostname());
            assertFalse(addr.services().isEmpty());
            assertFalse(addr.sslServices().isEmpty());
            for (int port : addr.services().values()) {
                assertTrue(port > 0);
            }
            for (int port : addr.sslServices().values()) {
                assertTrue(port > 0);
            }
        }
    }

    @Test
    void shouldParseElixirConfig() {
        CouchbaseBucketConfig config = readConfig("cloud_tls_only.json");
        assertEquals("database_hSeAfu", config.name());
        assertEquals(64, config.numberOfPartitions());
        assertEquals(0, config.numberOfReplicas());

        assertEquals(12, config.nodes().size());

        Set<String> kvNodes = new HashSet<>();
        Set<String> queryNodes = new HashSet<>();
        for (NodeInfo ni : config.nodes()) {
            assertTrue(ni.services().isEmpty());

            Map<ServiceType, Integer> sslServices = ni.sslServices();
            assertTrue(sslServices.get(ServiceType.MANAGER) > 0);

            Integer kvPort = sslServices.get(ServiceType.KV);
            if (kvPort != null) {
                kvNodes.add(ni.hostname() + ":" + kvPort);
                assertTrue(kvPort > 0);
            }

            Integer queryPort = sslServices.get(ServiceType.QUERY);
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
        assertEquals(21001, config.nodeAtIndex(0).sslServices().get(ServiceType.KV));
        assertEquals("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com", config.nodeAtIndex(1).hostname());
        assertEquals(21002, config.nodeAtIndex(1).sslServices().get(ServiceType.KV));
        assertEquals("i-041c154dd0246354e.sdk.dev.cloud.couchbase.com", config.nodeAtIndex(2).hostname());
        assertEquals(11207, config.nodeAtIndex(2).sslServices().get(ServiceType.KV));
    }

    private static CouchbaseBucketConfig readConfig(final String path) {
        return readConfig(path, null);
    }

    /**
     * Helper method to load the config.
     */
    private static CouchbaseBucketConfig readConfig(final String path, final String origin) {
        String raw = readResource(path, CouchbaseBucketConfigTest.class);
        if (origin != null) {
            raw = raw.replace("$HOST", origin);
        }
        return (CouchbaseBucketConfig) BucketConfigParser.parse(raw, StandardMemcachedHashingStrategy.INSTANCE, origin);
    }
}
