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

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.util.Utils.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of {@link MemcachedBucketConfigTest} through the
 * {@link BucketConfigParser}.
 */
class MemcachedBucketConfigTest {

    private static final CoreEnvironment ENV = CoreEnvironment.create(mock(Credentials.class));

    @AfterAll
    static void cleanup() {
        ENV.shutdown(Duration.ofSeconds(1));
    }

    /**
     * The config loaded has 4 nodes, but only two are data nodes. This tests checks that the ketama
     * nodes are only populated for those two nodes which include the binary service type.
     */
    @Test
    void shouldOnlyUseDataNodesForKetama() {
        MemcachedBucketConfig config = readConfig("memcached_mixed_sherlock.json");

        assertEquals(4, config.nodes().size());
        for (Map.Entry<Long, NodeInfo> node : config.ketamaNodes().entrySet()) {
            String hostname = node.getValue().hostname().address();
            assertTrue(hostname.equals("192.168.56.101") || hostname.equals("192.168.56.102"));
            assertTrue(node.getValue().services().containsKey(ServiceType.KV));
        }
    }

    @Test
    void shouldLoadConfigWithIPv6() {
        assumeFalse(NetworkAddress.FORCE_IPV4);
        MemcachedBucketConfig config = readConfig("memcached_with_ipv6.json");

        assertEquals(2, config.nodes().size());
        for (Map.Entry<Long, NodeInfo> node : config.ketamaNodes().entrySet()) {
            String hostname = node.getValue().hostname().address();
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
        assertNull(config.uuid());
    }

    /**
     * This test makes sure that the external hosts are present if set.
     */
    @Test
    void shouldIncludeExternalIfPresent() {
        MemcachedBucketConfig config = readConfig("config_with_external_memcache.json");

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

    @Test
    void shouldOnlyTakeNodesArrayIntoAccount() {
        MemcachedBucketConfig config = readConfig("memcached_during_rebalance.json");

        List<String> mustContain = Arrays.asList(
            "10.0.0.1",
            "10.0.0.2",
            "10.0.0.3"
        );
        List<String> mustNotContain = Collections.singletonList("10.0.0.4");

        Collection<NodeInfo> actualRingNodes = config.ketamaNodes().values();
        for (NodeInfo nodeInfo : actualRingNodes) {
            String actual = nodeInfo.hostname().nameOrAddress();
            assertTrue(mustContain.contains(actual));
            assertFalse(mustNotContain.contains(actual));
        }
    }

    /**
     * Helper method to load the config.
     */
    private static MemcachedBucketConfig readConfig(final String path) {
        return (MemcachedBucketConfig) BucketConfigParser.parse(
            readResource(path, MemcachedBucketConfigTest.class),
            ENV,
            null
        );
    }

}
