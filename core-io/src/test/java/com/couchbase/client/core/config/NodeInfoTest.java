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

import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Verifies the functionality of the {@link NodeInfo}.
 *
 * @author Michael Nitschinger
 * @since 1.0.3
 */
class NodeInfoTest {

    @Test
    void shouldExposeViewServiceWhenAvailable() {
        Map<String, Integer> ports = new HashMap<>();
        String viewBase = "http://127.0.0.1:8092/default%2Baa4b515529fa706f1e5f09f21abb5c06";
        NodeInfo info = new NodeInfo(viewBase, "localhost:8091", ports, null);

        assertEquals(2, info.services().size());
        assertEquals(8091, (long) info.services().get(ServiceType.MANAGER));
        assertEquals(8092, (long) info.services().get(ServiceType.VIEWS));
    }

    @Test
    void shouldNotExposeViewServiceWhenNotAvailable() {
        Map<String, Integer> ports = new HashMap<>();
        NodeInfo info = new NodeInfo(null, "localhost:8091", ports, null);

        assertEquals(1, info.services().size());
        assertEquals(8091, (long) info.services().get(ServiceType.MANAGER));
    }

    @Test
    void shouldExposeRawHostnameFromConstruction() {
        assertEquals(
            "localhost",
            new NodeInfo(null, "localhost:8091", new HashMap<>(), null).rawHostname()
        );

        assertEquals(
            "127.0.0.1",
            new NodeInfo(null, "127.0.0.1:8091", new HashMap<>(), null).rawHostname()
        );
    }

    @Test
    void shouldHandleIPv6() {
        assumeFalse(NetworkAddress.FORCE_IPV4);

        Map<String, Integer> ports = new HashMap<String, Integer>();
        NodeInfo info = new NodeInfo(null, "[fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7]:8091", ports, null);

        assertEquals(1, info.services().size());
        assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", info.hostname().address());
        assertEquals(8091, (long) info.services().get(ServiceType.MANAGER));
    }

}
