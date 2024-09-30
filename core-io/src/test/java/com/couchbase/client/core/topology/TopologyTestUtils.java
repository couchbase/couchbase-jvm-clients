/*
 * Copyright 2024 Couchbase, Inc.
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

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.GlobalConfig;
import com.couchbase.client.core.config.LegacyConfigHelper;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class TopologyTestUtils {
  private TopologyTestUtils() {
  }

  public static NodeIdentifier nodeId(String host) {
    return nodeId(host, 8091);
  }

  public static NodeIdentifier nodeId(String host, int port) {
    return new NodeIdentifier(host, port, host);
  }

  public static HostAndServicePorts node(String host, Map<ServiceType, Integer> ports) {
    return new HostAndServicePorts(host, ports, nodeId(host, ports.getOrDefault(ServiceType.MANAGER, 8091)), null, null);
  }

  @Deprecated
  public static NodeInfo nodeInfo(String host, Map<ServiceType, Integer> ports) {
    return new NodeInfo(host, ports, emptyMap(), null, nodeId(host, ports.getOrDefault(ServiceType.MANAGER, 8091)).toLegacy());
  }

  public static ClusterTopology clusterTopology(List<HostAndServicePorts> nodes) {
    return ClusterTopology.of(
      new TopologyRevision(1, 1),
      null,
      nodes,
      emptySet(),
      NetworkResolution.DEFAULT,
      PortSelector.NON_TLS,
      null // no bucket
    );
  }

  public static TestTopologyParser topologyParser() {
    return new TestTopologyParser();
  }

  public static class TestTopologyParser {
    private String originHost = "127.0.0.1";
    private NetworkSelector networkSelector = NetworkSelector.DEFAULT;
    private PortSelector portSelector = PortSelector.NON_TLS;
    private MemcachedHashingStrategy memcachedHashingStrategy = StandardMemcachedHashingStrategy.INSTANCE;

    private TestTopologyParser() {
    }

    private TestTopologyParser(String originHost, NetworkSelector networkSelector, PortSelector portSelector, MemcachedHashingStrategy memcachedHashingStrategy) {
      this.originHost = requireNonNull(originHost);
      this.networkSelector = requireNonNull(networkSelector);
      this.portSelector = requireNonNull(portSelector);
      this.memcachedHashingStrategy = requireNonNull(memcachedHashingStrategy);
    }

    public TestTopologyParser originHost(String originHost) {
      return new TestTopologyParser(originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public TestTopologyParser networkSelector(NetworkSelector networkSelector) {
      return new TestTopologyParser(originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public TestTopologyParser portSelector(PortSelector portSelector) {
      return new TestTopologyParser(originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public TestTopologyParser memcachedHashingStrategy(MemcachedHashingStrategy memcachedHashingStrategy) {
      return new TestTopologyParser(originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public ClusterTopology parse(String json) {
      return new TopologyParser(networkSelector, portSelector, memcachedHashingStrategy).parse(json, originHost);
    }

    @Deprecated
    public BucketConfig parseBucketConfig(String json) {
      return LegacyConfigHelper.toLegacyBucketConfig(parse(json).requireBucket());
    }

    @Deprecated
    public GlobalConfig parseGlobalConfig(String json) {
      return new GlobalConfig(parse(json));
    }
  }

}

