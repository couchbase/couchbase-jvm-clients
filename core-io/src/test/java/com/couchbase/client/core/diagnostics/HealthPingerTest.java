/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.NodeIdentifier;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.diagnostics.HealthPinger.extractPingTargets;
import static com.couchbase.client.core.service.ServiceType.KV;
import static com.couchbase.client.core.service.ServiceType.VIEWS;
import static com.couchbase.client.core.topology.TopologyTestUtils.topologyParser;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HealthPingerTest {

  private static final String BUCKET = "travel-sample";
  private static final NodeIdentifier hasActives = new NodeIdentifier("1.2.3.4", 8091, "1.2.3.4");
  private static final NodeIdentifier noActives = new NodeIdentifier("2.3.4.5", 8091, "2.3.4.5");

  /**
   * Both nodes host the bucket, but only one hosts active partitions.
   * This happens during a rebalance.
   */
  private static ClusterConfig configWithNodeHostingNoActivePartitions() {
    ClusterConfig config = new ClusterConfig();
    config.setBucketConfig(
      (ClusterTopologyWithBucket) topologyParser()
        .withResourcesFrom(ClusterConfig.class)
        .parseResource("config_7.6.0_2kv_but_only_1_has_active_partitions.json")
    );
    return config;
  }

  private static Set<RequestTarget> extract(Set<ServiceType> serviceTypes) {
    return extractPingTargets(
      configWithNodeHostingNoActivePartitions(),
      serviceTypes,
      Optional.of(BUCKET),
      WaitUntilReadyHelper.WaitUntilReadyLogger.dummy
    );
  }

  @Test
  void viewsTargetsOnlyIncludeNodesWithActivePartitions() {
    assertEquals(
      setOf(new RequestTarget(VIEWS, hasActives, BUCKET)),
      extract(setOf(VIEWS))
    );
  }

  @Test
  void kvTargetsIncludeNodesWithoutActivePartitions() {
    assertEquals(
      setOf(
        new RequestTarget(KV, hasActives, BUCKET),
        new RequestTarget(KV, noActives, BUCKET)
      ),
      extract(setOf(KV))
    );
  }

  @Test
  void defaultServicesSkipOnlyViewsOnNodeWithoutActivePartitions() {
    Set<RequestTarget> targets = extract(emptySet());

    assertTrue(targets.contains(new RequestTarget(VIEWS, hasActives, BUCKET)));
    assertTrue(targets.stream().noneMatch(it -> it.serviceType() == VIEWS && it.nodeIdentifier().equals(noActives)));
    assertTrue(targets.contains(new RequestTarget(KV, noActives, BUCKET)));
  }
}
