/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyBuilder;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterConfigTest {

  @Test
  void returnsEmptyAllNodeAddresses() {
    ClusterConfig config = new ClusterConfig();
    assertEquals(emptySet(), config.allNodeAddresses());
  }

  @Test
  void allNodeAddressesWithOnlyGlobalConfig() {
    ClusterConfig config = new ClusterConfig();

    ClusterTopology gc = minimalTopology(
      "hostname1",
      "hostname2"
    ).build();
    config.setGlobalConfig(gc);

    Set<String> expected = setOf(
      "hostname1",
      "hostname2"
    );

    assertEquals(expected, config.allNodeAddresses());
  }

  @Test
  void allNodeAddressesWithOnlyBucketConfig() {
    ClusterConfig config = new ClusterConfig();

    ClusterTopologyWithBucket bc = minimalTopology(
      "hostname1",
      "hostname2",
      "hostname3"
    ).couchbaseBucket("bucket-name").build();
    config.setBucketConfig(bc);

    Set<String> expected = setOf(
      "hostname1",
      "hostname2",
      "hostname3"
    );

    assertEquals(expected, config.allNodeAddresses());
  }

  @Test
  void allNodeAddressesWithGlobalAndBucketConfigs() {
    ClusterConfig config = new ClusterConfig();

    ClusterTopology gc = minimalTopology(
      "hostname1",
      "hostname2"
    ).build();
    config.setGlobalConfig(gc);

    ClusterTopologyWithBucket bc = minimalTopology(
      "hostname1",
      "hostname2",
      "hostname3"
    ).couchbaseBucket("bucket-name").build();
    config.setBucketConfig(bc);

    Set<String> expected = setOf(
      "hostname1",
      "hostname2",
      "hostname3"
    );

    assertEquals(expected, config.allNodeAddresses());
  }

  static ClusterTopologyBuilder minimalTopology(String... hosts) {
    ClusterTopologyBuilder builder = new ClusterTopologyBuilder();
    for (String host : hosts) {
      builder.addNode(host, node -> node.ports(mapOf(
        ServiceType.MANAGER, 8091,
        ServiceType.KV, 11210
      )));
    }
    return builder;
  }
}
