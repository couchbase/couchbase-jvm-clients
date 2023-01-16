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

// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.Response;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

abstract class TestCluster implements ExtensionContext.Store.CloseableResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestCluster.class);
  private static final Duration START_TIMEOUT = Duration.ofMinutes(2);
  private static final TypeReference<HashMap<String, Object>> MAP_STRING_OBJECT =
    new TypeReference<HashMap<String, Object>>() {};

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Map<String, Function<Properties, ? extends TestCluster>> CLUSTER_BUILDERS = new HashMap<>();

  static {
    CLUSTER_BUILDERS.put("containerized", props -> new ContainerizedTestCluster(props));
    CLUSTER_BUILDERS.put("mocked", props -> new MockTestCluster(props));
    CLUSTER_BUILDERS.put("unmanaged", props -> new UnmanagedTestCluster(props));
    CLUSTER_BUILDERS.put("caves", props -> new CavesTestCluster(props));
  }
  /**
   * The topology spec defined by the child implementations.
   */
  private volatile TestClusterConfig config;

  /**
   * Creates the Test cluster (either managed ur unmanaged).
   */
  static TestCluster create() {
    Properties properties = loadProperties();
    loadFromEnv(properties);
    String clusterType = properties.getProperty("cluster.type");
    if (!CLUSTER_BUILDERS.containsKey(clusterType)) {
      throw new IllegalStateException("Unsupported test cluster type: " + clusterType);
    }
    return CLUSTER_BUILDERS.get(clusterType).apply(properties);
  }

  /**
   * Implemented by the child class to start the cluster if needed.
   */
  abstract TestClusterConfig _start() throws Exception;

  abstract ClusterType type();

  TestCluster() { }

  void start() {
    long startTime = System.nanoTime();
    // JCBC-1672: Seeing intermittent flakiness connecting to ns_server, try to resolve by looping
    while (System.nanoTime() - startTime < START_TIMEOUT.toNanos()) {
      try {
        config = _start();
        break;
      } catch (Exception ex) {
        LOGGER.warn("Starting failed with error", ex);
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          LOGGER.debug("Test cluster creating was interrupted", e);
        }
      }
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

  protected List<TestNodeConfig> nodesFromRaw(final String inputHost, final String config) {
    List<TestNodeConfig> result = new ArrayList<>();
    for (Map<String, Object> node : nodesExtFromConfig(decodeConfig(config))) {
      Map<String, Integer> services = getServicesFromNode(node);
      String hostname = (String) node.get("hostname");
      if (hostname == null) {
        hostname = inputHost;
      }
      Map<Services, Integer> ports = new HashMap<>();
      Arrays.stream(Services.values())
        .filter(service -> services.containsKey(service.getNodeName()))
        .forEach(service -> ports.put(service, services.get(service.getNodeName())));

      result.add(new TestNodeConfig(hostname, ports, false));
    }
    return result;
  }

  @SuppressWarnings({"unchecked"})
  private static List<Map<String, Object>> nodesExtFromConfig(Map<String, Object> decodedConfig) {
    return (List<Map<String, Object>>) decodedConfig.get("nodesExt");
  }

  protected static ClusterVersion parseClusterVersion(Response response) {
    try {
      Map<String, Object> decoded = MAPPER.readValue(response.body().bytes(), MAP_STRING_OBJECT);
      return ClusterVersion.parseString((String) decoded.get("implementationVersion"));
    } catch (IOException e) {
      throw new RuntimeException("Error decoding", e);
    }
  }

  @SuppressWarnings({"unchecked"})
  protected int replicasFromRaw(final String config) {
    Map<String, Object> decoded = decodeConfig(config);
    Map<String, Object> serverMap = (Map<String, Object>) decoded.get("vBucketServerMap");
    return (int) serverMap.get("numReplicas");
  }

  @SuppressWarnings({"unchecked"})
  protected Set<Capabilities> capabilitiesFromRaw(final String config, ClusterVersion clusterVersion) {
    Map<String, Object> decoded = decodeConfig(config);
    Set<Capabilities> capabilities = new HashSet<>(capabilitiesFromConfig(decoded));

    List<String> bucketCapabilities = (List<String>) decoded.get("bucketCapabilities");
    if (bucketCapabilities.contains("durableWrite")) {
      capabilities.add(Capabilities.SYNC_REPLICATION);
      /// GCCCP was also added in 6.5 when sync replication was added, so we can assume the same.
      capabilities.add(Capabilities.GLOBAL_CONFIG);
      // same for user groups
      capabilities.add(Capabilities.USER_GROUPS);
    }
    if (bucketCapabilities.contains("collections")) {
      capabilities.add(Capabilities.COLLECTIONS);
      capabilities.add(Capabilities.PRESERVE_EXPIRY); // also added in 7.0
    }
    if (bucketCapabilities.contains("tombstonedUserXAttrs")) {
      // note: 6.6 and later
      capabilities.add(Capabilities.CREATE_AS_DELETED);
      capabilities.add(Capabilities.BUCKET_MINIMUM_DURABILITY);
    }
    if (!clusterVersion.isCommunityEdition()) {
      capabilities.add(Capabilities.ENTERPRISE_EDITION);
    }
    if (bucketCapabilities.contains("subdoc.ReplaceBodyWithXattr")) {
      capabilities.add(Capabilities.SUBDOC_REPLACE_BODY_WITH_XATTR);
    }
    if (bucketCapabilities.contains("subdoc.ReviveDocument")) {
      capabilities.add(Capabilities.SUBDOC_REVIVE_DOCUMENT);
    }
    if (clusterVersion.majorVersion() == 7 && clusterVersion.minorVersion() == 1) {
      //Rate limiting only available on 7.1
      capabilities.add(Capabilities.RATE_LIMITING);
    }
    if (clusterVersion.majorVersion() > 7
      || (clusterVersion.majorVersion() == 7 && clusterVersion.minorVersion() >= 1)) {
      capabilities.add(Capabilities.QUERY_PRESERVE_EXPIRY);

      if (!clusterVersion.isCommunityEdition()) {
        capabilities.add(Capabilities.STORAGE_BACKEND);
      }
    }
    if (bucketCapabilities.contains("rangeScan")) {
      capabilities.add(Capabilities.RANGE_SCAN);
    }
    return capabilities;
  }

  private static Set<Capabilities> capabilitiesFromConfig(Map<String, Object> decoded) {
    return nodesExtFromConfig(decoded).stream()
      .flatMap(node -> getServicesFromNode(node).keySet().stream())
      .flatMap(name -> Arrays.stream(Capabilities.values())
        .filter(v -> v.getNames().contains(name))
        .distinct())
      .collect(Collectors.toSet());
  }

  @SuppressWarnings({"unchecked"})
  private static Map<String, Integer> getServicesFromNode(Map<String, Object> node) {
    return (Map<String, Integer>) node.get("services");
  }

  private static Map<String, Object> decodeConfig(String config) {
    try {
      return MAPPER.readValue(config.getBytes(UTF_8), MAP_STRING_OBJECT);
    } catch (IOException e) {
      throw new RuntimeException("Error decoding, raw: " + config, e);
    }
  }

  /**
   * Load properties from the defaults file and then override with sys properties.
   */
  private static Properties loadProperties() {
    Properties defaults = new Properties();
    try {
      // This file is unversioned.  Good practice is to copy integration.properties to this and make changes to this.
      URL url = TestCluster.class.getResource("/integration.local.properties");
      if (url == null) {
        url = TestCluster.class.getResource("/integration.properties");
      }
      LOGGER.info("Found test config file {}", url.getPath());
      defaults.load(url.openStream());

    } catch (Exception ex) {
      throw new RuntimeException("Could not load properties", ex);
    }

    Properties all = new Properties(System.getProperties());
    defaults.forEach((key, value) -> all.putIfAbsent(key.toString(), value));

    return all;
  }

  /**
   * Adds system properties as well.
   *
   * @param toOverride original properties coming from the config files.
   */
  static void loadFromEnv(final Properties toOverride) {
    Map<String, String> envParams = System.getenv();
    envParams.entrySet().stream()
      .filter(e -> e.getKey().toLowerCase().startsWith("cluster"))
      .forEach(e -> toOverride.setProperty(transformKey(e), e.getValue()));
  }

  @NotNull
  private static String transformKey(Map.Entry<String, String> e) {
    return e.getKey().toLowerCase().replace("_", ".");
  }

  /**
   * Whether the test config has asked to connect to the cluster over protostellar://
   *
   * (Not whether the cluster has Stellar Nebula enabled).
   */
  public boolean isProtostellar() {
    return false;
  }
}
