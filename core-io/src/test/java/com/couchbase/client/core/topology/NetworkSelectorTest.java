/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.topology;

import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NetworkSelectorTest {

  private static HostAndServicePorts nodeWithHostAndKvPort(String host, int kvPort) {
    return new HostAndServicePorts(
      host,
      mapOf(
        ServiceType.KV, kvPort,
        ServiceType.MANAGER, 8091
      ),
      NodeIdentifier.forBootstrap(host, 8091),
      null,
      null,
      null,
      null
    );
  }

  @Test
  void autoSelectsDefaultIfExactMatch() {
    Set<SeedNode> seeds = setOf(SeedNode.create("example.com").withKvPort(11210));

    NetworkResolution selected = NetworkSelector.autoDetect(seeds)
      .selectNetwork(listOf(
        mapOf(NetworkResolution.DEFAULT, nodeWithHostAndKvPort("example.com", 11210)),
        mapOf(NetworkResolution.EXTERNAL, nodeWithHostAndKvPort("example.com", 11210))
      ));

    assertEquals(NetworkResolution.DEFAULT, selected);
  }

  @Test
  void autoSelectsExternalIfExactMatch() {
    Set<SeedNode> seeds = setOf(SeedNode.create("example.com").withKvPort(11210));

    NetworkResolution selected = NetworkSelector.autoDetect(seeds)
      .selectNetwork(listOf(
        mapOf(NetworkResolution.DEFAULT, nodeWithHostAndKvPort("example.com", 1)),
        mapOf(NetworkResolution.EXTERNAL, nodeWithHostAndKvPort("example.com", 11210))
      ));

    assertEquals(NetworkResolution.EXTERNAL, selected);
  }

  @Test
  void autoFallsBackToExternalIfPresent() {
    Set<SeedNode> seeds = setOf(SeedNode.create("example.com").withKvPort(11210));

    NetworkResolution selected = NetworkSelector.autoDetect(seeds)
      .selectNetwork(listOf(
        mapOf(NetworkResolution.DEFAULT, nodeWithHostAndKvPort("example.com", 1)),
        mapOf(NetworkResolution.EXTERNAL, nodeWithHostAndKvPort("example.com", 2))
      ));

    assertEquals(NetworkResolution.EXTERNAL, selected);
  }

  @Test
  void autoFallsBackToDefaultIfExternalIsAbsent() {
    Set<SeedNode> seeds = setOf(SeedNode.create("example.com").withKvPort(11210));

    NetworkResolution selected = NetworkSelector.autoDetect(seeds)
      .selectNetwork(listOf(
        mapOf(NetworkResolution.DEFAULT, nodeWithHostAndKvPort("127.0.0.1", 1))
      ));

    assertEquals(NetworkResolution.DEFAULT, selected);
  }
}
