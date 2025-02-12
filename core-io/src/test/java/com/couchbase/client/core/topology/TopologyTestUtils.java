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

import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.test.Resources;

import java.util.Map;

import static java.util.Collections.emptyMap;
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
    return new HostAndServicePorts(host, ports, nodeId(host, ports.getOrDefault(ServiceType.MANAGER, 8091)), null, null, null, null);
  }

  @Deprecated
  public static NodeInfo nodeInfo(String host, Map<ServiceType, Integer> ports) {
    return new NodeInfo(host, ports, emptyMap(), null, nodeId(host, ports.getOrDefault(ServiceType.MANAGER, 8091)).toLegacy());
  }

  public static TestTopologyParser topologyParser() {
    return new TestTopologyParser();
  }

  public static class TestTopologyParser {
    private Resources resources = Resources.from(TestTopologyParser.class);
    private String originHost = "127.0.0.1";
    private NetworkSelector networkSelector = NetworkSelector.DEFAULT;
    private PortSelector portSelector = PortSelector.NON_TLS;
    private MemcachedHashingStrategy memcachedHashingStrategy = StandardMemcachedHashingStrategy.INSTANCE;

    private TestTopologyParser() {
    }

    private TestTopologyParser(Resources resources, String originHost, NetworkSelector networkSelector, PortSelector portSelector, MemcachedHashingStrategy memcachedHashingStrategy) {
      this.resources = requireNonNull(resources);
      this.originHost = requireNonNull(originHost);
      this.networkSelector = requireNonNull(networkSelector);
      this.portSelector = requireNonNull(portSelector);
      this.memcachedHashingStrategy = requireNonNull(memcachedHashingStrategy);
    }

    public TestTopologyParser withResourcesFrom(Class<?> classLoader) {
      return new TestTopologyParser(Resources.from(classLoader), originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public TestTopologyParser withOriginHost(String originHost) {
      return new TestTopologyParser(resources, originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public TestTopologyParser withNetworkSelector(NetworkSelector networkSelector) {
      return new TestTopologyParser(resources, originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public TestTopologyParser withPortSelector(PortSelector portSelector) {
      return new TestTopologyParser(resources, originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public TestTopologyParser withMemcachedHashingStrategy(MemcachedHashingStrategy memcachedHashingStrategy) {
      return new TestTopologyParser(resources, originHost, networkSelector, portSelector, memcachedHashingStrategy);
    }

    public ClusterTopology parseResource(String jsonResourceName) {
      return new TopologyParser(networkSelector, portSelector, memcachedHashingStrategy).parse(resources.getString(jsonResourceName), originHost);
    }
  }

}

