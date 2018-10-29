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

package com.couchbase.client.util;

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.shaded.io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

abstract class TestCluster implements ExtensionContext.Store.CloseableResource {

  /**
   * The topology spec defined by the child implementations.
   */
  private volatile TestClusterConfig config;

  /**
   * Creates the Test cluster (either managed ur unmanaged).
   */
  static TestCluster create() {
    Properties properties = loadProperties();
    boolean managed = Boolean.parseBoolean(properties.getProperty("cluster.managed"));
    return managed ? new ContainerizedTestCluster(properties) : new UnmanagedTestCluster(properties);
  }

  /**
   * Implemented by the child class to start the cluster if needed.
   */
  abstract TestClusterConfig _start() throws Exception;

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
    Map<String, Object> decoded = (Map<String, Object>)
      Mapper.decodeInto(config.getBytes(CharsetUtil.UTF_8), Map.class);
    List<Map<String, Object>> ext = (List<Map<String, Object>>) decoded.get("nodesExt");
    for (Map<String, Object> node : ext) {
      Map<String, Integer> services = (Map<String, Integer>) node.get("services");
      String hostname = (String) node.get("hostname");
      if (hostname == null) {
        hostname = inputHost;
      }
      Map<ServiceType, Integer> ports = new HashMap<>();
      ports.put(ServiceType.KV, services.get("kv"));
      ports.put(ServiceType.CONFIG, services.get("mgmt"));
      result.add(new TestNodeConfig(hostname, ports));
    }
    return result;
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
