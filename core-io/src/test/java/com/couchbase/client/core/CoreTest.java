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

package com.couchbase.client.core;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.core.InitGlobalConfigFailedEvent;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.GlobalConfigNotFoundException;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.cnc.SimpleEventBus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.couchbase.client.test.Util.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Verifies the various functionality of the {@link Core}.
 */
@SuppressWarnings("UnassignedFluxMonoInstance")
class CoreTest {
  private static final Logger logger = LoggerFactory.getLogger(CoreTest.class);

  private static CoreEnvironment ENV;

  private static final String LOCALHOST = "127.0.0.1";

  private static SimpleEventBus EVENT_BUS;

  private static final Authenticator AUTHENTICATOR = PasswordAuthenticator.create("foo", "bar");
  
  private static final int TIMEOUT = 1000;

  @BeforeAll
  static void beforeAll() {
    EVENT_BUS = new SimpleEventBus(true);
    ENV = CoreEnvironment.builder().eventBus(EVENT_BUS).build();
  }

  @AfterAll
  static void afterAll() {
    ENV.shutdown();
  }

  /**
   * This test initializes with a first config and then pushes a second one, making sure that
   * the difference in services and nodes is enabled.
   */
  @Test
  @SuppressWarnings({"unchecked"})
  void addNodesAndServicesOnNewConfig() throws Exception {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    Sinks.Many<ClusterConfig> configs = Sinks.many().multicast().directBestEffort();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs.asFlux());
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket(eq("travel-sample"), anyBoolean())).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    Node mock102 = mock(Node.class);
    configureMock(mock101, "mock101", "10.143.190.101", 8091);
    configureMock(mock102, "mock102", "10.143.190.102", 8091);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    try ( Core core = new Core(ENV, AUTHENTICATOR, SeedNode.LOCALHOST, null) {
      @Override
      public ConfigurationProvider createConfigurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NodeIdentifier target, final Optional<String> alternate) {
        return mocks.get(target.address());
      }
    } ) {
      logger.info("Emitting config {}", clusterConfig.allNodeAddresses());
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());
      verify(mock102, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());

      BucketConfig oneNodeConfig = BucketConfigParser.parse(
        readResource("one_node_config.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(oneNodeConfig);
      logger.info("Emitting config");
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating 1");
      logger.info("Validating 2");
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      logger.info("Validating 3");
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));
      logger.info("Done validating");

      verify(mock102, never()).addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock102, never()).addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock102, never()).addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock102, never()).addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      BucketConfig twoNodeConfig = BucketConfigParser.parse(
        readResource("two_nodes_config.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodeConfig);
      logger.info("Emitting config");
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));
    }
  }


  void configureMock(Node mock, String id, String ip, int port) {
    when(mock.identifier()).thenReturn(new NodeIdentifier(ip, port));
    when(mock.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenAnswer((Answer) invocation -> {
        logger.info("{}.addService called with arguments: {}", id, Arrays.toString(invocation.getArguments()));
        return Mono.empty();
      });
    when(mock.removeService(any(ServiceType.class), any(Optional.class)))
      .thenAnswer((Answer) invocation -> {
        logger.info("{}.removeService called with arguments: {}", id, Arrays.toString(invocation.getArguments()));
        return Mono.empty();
      });
    when(mock.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock.disconnect()).thenReturn(Mono.empty());

    logger.info("Configured mock {} {} {}", id, ip, port);
  }

  @Test
  @SuppressWarnings("unchecked")
  void addServicesOnNewConfig() throws Exception {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    Sinks.Many<ClusterConfig> configs = Sinks.many().multicast().directBestEffort();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs.asFlux());
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket(eq("travel-sample"), anyBoolean())).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    Node mock102 = mock(Node.class);
    configureMock(mock101, "mock101", "10.143.190.101", 8091);
    configureMock(mock102, "mock102", "10.143.190.102", 8091);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    try ( Core core = new Core(ENV, AUTHENTICATOR, SeedNode.LOCALHOST, null) {
      @Override
      public ConfigurationProvider createConfigurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NodeIdentifier target, final Optional<String> alternate) {
        return mocks.get(target.address());
      }
    } ) {
      logger.info("Emitting config {}", clusterConfig.allNodeAddresses());
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());
      verify(mock102, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());

      BucketConfig twoNodesConfig = BucketConfigParser.parse(
        readResource("two_nodes_config.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesConfig);
      logger.info("Emitting config {}", clusterConfig.allNodeAddresses());
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      BucketConfig twoNodesConfigMore = BucketConfigParser.parse(
        readResource("two_nodes_config_more_services.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesConfigMore);
      logger.info("Emitting config");
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(2))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      verify(mock102, timeout(TIMEOUT).times(2))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(2))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(2))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(2))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.SEARCH, 8094, Optional.empty());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void removeNodesAndServicesOnNewConfig() throws Exception {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    Sinks.Many<ClusterConfig> configs = Sinks.many().multicast().directBestEffort();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs.asFlux());
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket(eq("travel-sample"), anyBoolean())).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    Node mock102 = mock(Node.class);
    configureMock(mock101, "mock101", "10.143.190.101", 8091);
    configureMock(mock102, "mock102", "10.143.190.102", 8091);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    try ( Core core = new Core(ENV, AUTHENTICATOR, SeedNode.LOCALHOST, null) {
      @Override
      public ConfigurationProvider createConfigurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NodeIdentifier target, final Optional<String> alternate) {
        logger.info("createNode {} {}", target, alternate);
        return mocks.get(target.address());
      }
    } ) {
      logger.info("Emitting config {}", clusterConfig.allNodeAddresses());
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());
      verify(mock102, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());

      BucketConfig twoNodesConfig = BucketConfigParser.parse(
        readResource("two_nodes_config_more_services.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesConfig);
      logger.info("Emitting config");
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.SEARCH, 8094, Optional.empty());

      BucketConfig twoNodesLessServices = BucketConfigParser.parse(
        readResource("two_nodes_config.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesLessServices);
      logger.info("Emitting config");
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock102, timeout(TIMEOUT).times(1))
        .removeService(ServiceType.SEARCH, Optional.empty());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void removesNodeIfNotPresentInConfigAnymore() throws Exception {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    Sinks.Many<ClusterConfig> configs = Sinks.many().multicast().directBestEffort();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs.asFlux());
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket(eq("travel-sample"), anyBoolean())).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    Node mock102 = mock(Node.class);
    configureMock(mock101, "mock101", "10.143.190.101", 8091);
    configureMock(mock102, "mock102", "10.143.190.102", 8091);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    try ( Core core = new Core(ENV, AUTHENTICATOR, SeedNode.LOCALHOST, null) {
      @Override
      public ConfigurationProvider createConfigurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NodeIdentifier target, final Optional<String> alternate) {
        return mocks.get(target.address());
      }
    } ){
      logger.info("Emitting config {}", clusterConfig.allNodeAddresses());
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());
      verify(mock102, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());

      BucketConfig twoNodesConfig = BucketConfigParser.parse(
        readResource("two_nodes_config_more_services.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesConfig);
      logger.info("Emitting config");
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.SEARCH, 8094, Optional.empty());

      BucketConfig twoNodesLessServices = BucketConfigParser.parse(
        readResource("one_node_config.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesLessServices);
      logger.info("Emitting config");
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");

      verify(mock102, timeout(TIMEOUT).times(1)).disconnect();
    }
  }

  /**
   * With cluster_run it is possible to run more than one node on the same hostname. So we need to make sure that
   * the node is identified by a tuple of hostname and manager port, and this should work.
   */
  @Test
  @SuppressWarnings("unchecked")
  void addsSecondNodeIfBothSameHostname() throws Exception {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    Sinks.Many<ClusterConfig> configs = Sinks.many().multicast().directBestEffort();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs.asFlux()
      .doOnNext(v -> logger.info("config emitted")));
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket(eq("default"), anyBoolean())).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    Node mock102 = mock(Node.class);
    configureMock(mock101, "mock101", LOCALHOST, 9000);
    configureMock(mock102, "mock102", LOCALHOST, 9001);

    final Map<String, Node> mocks = new HashMap<>();

    mocks.put("127.0.0.1:9000", mock101);
    mocks.put("127.0.0.1:9001", mock102);
    try ( Core core = new Core(ENV, AUTHENTICATOR, SeedNode.LOCALHOST, null) {
      @Override
      public ConfigurationProvider createConfigurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NodeIdentifier target, final Optional<String> alternate) {
        return mocks.get(target.address() + ":" + target.managerPort());
      }
    } ) {
      logger.info("Emitting config {}", clusterConfig.allNodeAddresses());
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());
      verify(mock102, timeout(TIMEOUT).times(0)).addService(any(), anyInt(), any());

      BucketConfig oneNodeConfig = BucketConfigParser.parse(
        readResource("cluster_run_two_nodes.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(oneNodeConfig);
      logger.info("Emitting config {}", oneNodeConfig.nodes().stream().map(v -> v.hostname()).collect(Collectors.joining(", ")));
      configs.tryEmitNext(clusterConfig).orThrow();
      addMagicSleep();

      logger.info("Validating");
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 9500, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 9000, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 12000, Optional.of("default"));

      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 9501, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 9001, Optional.empty());
      verify(mock102, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 12002, Optional.of("default"));
    }
  }

  @Test
  void ignoresFailedGlobalConfigInitAttempt() throws Exception {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    when(configProvider.configs()).thenReturn(Flux.empty());
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket(eq("travel-sample"), anyBoolean())).thenReturn(Mono.empty());

    try ( Core core = new Core(ENV, AUTHENTICATOR, SeedNode.LOCALHOST, null) {
      @Override
      public ConfigurationProvider createConfigurationProvider() {
        return configProvider;
      }
    } ) {

      when(configProvider.loadAndRefreshGlobalConfig()).thenReturn(Mono.error(new GlobalConfigNotFoundException()));
      core.initGlobalConfig();

      when(configProvider.loadAndRefreshGlobalConfig()).thenReturn(Mono.error(new UnsupportedConfigMechanismException()));
      core.initGlobalConfig();

      int numRaised = 0;
      for (Event event : EVENT_BUS.publishedEvents()) {
        if (event instanceof InitGlobalConfigFailedEvent) {
          numRaised++;
        }
      }
      assertEquals(2, numRaised);
    }
  }

  // A sleep after emitting the config prevents it intermittently emitting the config twice, for reasons that are unclear.
  static void addMagicSleep() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
