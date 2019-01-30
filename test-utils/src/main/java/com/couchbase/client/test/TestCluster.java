/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.shaded.io.netty.util.CharsetUtil;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

abstract class TestCluster implements ExtensionContext.Store.CloseableResource {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * The topology spec defined by the child implementations.
   */
  private volatile TestClusterConfig config;

  /**
   * Creates the Test cluster (either managed ur unmanaged).
   */
  static TestCluster create() {
    Properties properties = loadProperties();
    String clusterType = properties.getProperty("cluster.type");

    if (clusterType.equals("containerized")) {
      return new ContainerizedTestCluster(properties);
    } else if (clusterType.equals("mocked")) {
      return new MockTestCluster(properties);
    } else if (clusterType.equals("unmanaged")) {
      return new UnmanagedTestCluster(properties);
    } else {
      throw new IllegalStateException("Unsupported test cluster type: " + clusterType);
    }
  }

  /**
   * Implemented by the child class to start the cluster if needed.
   */
  abstract TestClusterConfig _start() throws Exception;

  abstract ClusterType type();

  TestCluster() { }

  void start() {
    try {
      config = _start();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public TestClusterConfig config() {
    return config;
  }

  /**
   * Helper method to extract the node configs from a raw bucket config.
   *
   * @param config the config.
   * @return the extracted node configs.
   */
  @SuppressWarnings({"unchecked"})
  protected List<TestNodeConfig> nodesFromRaw(final String inputHost, final String config) {
    List<TestNodeConfig> result = new ArrayList<>();
    Map<String, Object> decoded;
    try {
      decoded = (Map<String, Object>)
        MAPPER.readValue(config.getBytes(CharsetUtil.UTF_8), Map.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<Map<String, Object>> ext = (List<Map<String, Object>>) decoded.get("nodesExt");
    for (Map<String, Object> node : ext) {
      Map<String, Integer> services = (Map<String, Integer>) node.get("services");
      String hostname = (String) node.get("hostname");
      if (hostname == null) {
        hostname = inputHost;
      }
      Map<Services, Integer> ports = new HashMap<>();
      ports.put(Services.KV, services.get("kv"));
      ports.put(Services.MANAGER, services.get("mgmt"));
      ports.put(Services.KV_TLS, services.get("kvSSL"));
      ports.put(Services.MANAGER_TLS, services.get("mgmtSSL"));
      result.add(new TestNodeConfig(hostname, ports));
    }
    return result;
  }

  protected int replicasFromRaw(final String config) {
    Map<String, Object> decoded;
    try {
      decoded = (Map<String, Object>)
        MAPPER.readValue(config.getBytes(CharsetUtil.UTF_8), Map.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Map<String, Object> serverMap = (Map<String, Object>) decoded.get("vBucketServerMap");
    return (int) serverMap.get("numReplicas");
  }

  /**
   * Load properties from the defaults file and then override with sys properties.
   */
  private static Properties loadProperties() {
    Properties defaults = new Properties();
    try {
      defaults.load(
        TestCluster.class.getResourceAsStream("/integration.properties")
      );
    } catch (Exception ex) {
      throw new RuntimeException("Could not load properties", ex);
    }

    Properties all = new Properties(System.getProperties());
    for (Map.Entry<Object, Object> property : defaults.entrySet()) {
      if (all.getProperty(property.getKey().toString()) == null) {
        all.put(property.getKey(), property.getValue());
      }
    }
    return all;
  }

}
