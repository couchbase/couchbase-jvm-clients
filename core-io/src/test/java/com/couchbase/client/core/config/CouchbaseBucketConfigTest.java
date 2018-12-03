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
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.couchbase.client.util.Utils.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Verifies that parsing various bucket configs works as expected through the
 * {@link BucketConfigParser}.
 */
class CouchbaseBucketConfigTest {

    @Test
    void shouldHavePrimaryPartitionsOnNode() {
        CouchbaseBucketConfig config = readConfig("config_with_mixed_partitions.json");

        assertTrue(config.hasPrimaryPartitionsOnNode(NetworkAddress.create("1.2.3.4")));
        assertFalse(config.hasPrimaryPartitionsOnNode(NetworkAddress.create("2.3.4.5")));
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
        assertTrue(config.nodes().get(0).alternateAddresses().isEmpty());
    }

    @Test
    void shouldFallbackToNodeHostnameIfNotInNodesExt() {
        CouchbaseBucketConfig config = readConfig("nodes_ext_without_hostname.json");

        NetworkAddress expected = NetworkAddress.create("1.2.3.4");
        assertEquals(1, config.nodes().size());
        assertEquals(expected, config.nodes().get(0).hostname());
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
    }

    @Test
    void shouldGracefullyHandleEmptyPartitions() {
        CouchbaseBucketConfig config = readConfig("config_with_no_partitions.json");

        assertEquals(CouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForMaster(24, false));
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
        assertEquals("192.168.1.194", config.nodes().get(0).hostname().address());
        assertEquals(9000, (int)config.nodes().get(0).services().get(ServiceType.CONFIG));
        assertEquals("192.168.1.194", config.nodes().get(1).hostname().address());
        assertEquals(9001, (int)config.nodes().get(1).services().get(ServiceType.CONFIG));
    }

    @Test
    void shouldLoadConfigWithMDS() {
        CouchbaseBucketConfig config = readConfig("cluster_run_three_nodes_mds_with_localhost.json");

        assertEquals(3, config.nodes().size());
        assertEquals("192.168.0.102", config.nodes().get(0).hostname().address());
        assertEquals("127.0.0.1", config.nodes().get(1).hostname().address());
        assertEquals("127.0.0.1", config.nodes().get(2).hostname().address());
        assertTrue(config.nodes().get(0).services().containsKey(ServiceType.KV));
        assertTrue(config.nodes().get(1).services().containsKey(ServiceType.KV));
        assertFalse(config.nodes().get(2).services().containsKey(ServiceType.KV));
    }

    @Test
    void shouldLoadConfigWithIPv6() {
        assumeFalse(NetworkAddress.FORCE_IPV4);
        CouchbaseBucketConfig config = readConfig("config_with_ipv6.json");

        assertEquals(2, config.nodes().size());
        assertEquals("fd63:6f75:6368:2068:1471:75ff:fe25:a8be", config.nodes().get(0).hostname().address());
        assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", config.nodes().get(1).hostname().address());

        assertEquals(1, config.numberOfReplicas());
        assertEquals(1024, config.numberOfPartitions());
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

    /**
     * This test makes sure that the external hosts are present if set.
     */
    @Test
    void shouldIncludeExternalIfPresent() {
        CouchbaseBucketConfig config = readConfig("config_with_external.json");

        List<NodeInfo> nodes = config.nodes();
        assertEquals(3, nodes.size());
        for (NodeInfo node : nodes) {
            Map<String, AlternateAddress> addrs = node.alternateAddresses();
            assertEquals(1, addrs.size());
            AlternateAddress addr = addrs.get(NetworkResolution.EXTERNAL.name());
            assertNotNull(addr.hostname());
            assertNotNull(addr.rawHostname());
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

    /**
     * Helper method to load the config.
     */
    private static CouchbaseBucketConfig readConfig(final String path) {
        return (CouchbaseBucketConfig) BucketConfigParser.parse(
          readResource(path, CouchbaseBucketConfigTest.class),
          null,
          null
        );
    }
}
