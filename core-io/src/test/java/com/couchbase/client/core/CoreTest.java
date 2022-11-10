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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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

  private static CoreEnvironment ENV;

  private static final String LOCALHOST = "127.0.0.1";

  private static SimpleEventBus EVENT_BUS;

  private static final Authenticator AUTHENTICATOR = PasswordAuthenticator.create("foo", "bar");
  
  private static final int TIMEOUT = 500;

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
    when(configProvider.closeBucket("travel-sample")).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    when(mock101.identifier()).thenReturn(new NodeIdentifier("10.143.190.101", 8091));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock101.disconnect()).thenReturn(Mono.empty());


    Node mock102 = mock(Node.class);
    when(mock102.identifier()).thenReturn(new NodeIdentifier("10.143.190.102", 8091));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock102.disconnect()).thenReturn(Mono.empty());

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
      configs.tryEmitNext(clusterConfig);

      verify(mock101, timeout(TIMEOUT).times(0))
              .addService(ServiceType.VIEWS, 8092, Optional.empty());

      BucketConfig oneNodeConfig = BucketConfigParser.parse(
        readResource("one_node_config.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(oneNodeConfig);
      configs.tryEmitNext(clusterConfig);

      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.VIEWS, 8092, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.MANAGER, 8091, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.QUERY, 8093, Optional.empty());
      verify(mock101, timeout(TIMEOUT).times(1))
        .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

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
      configs.tryEmitNext(clusterConfig);

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

  @Test
  @SuppressWarnings("unchecked")
  void addServicesOnNewConfig() throws Exception {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    Sinks.Many<ClusterConfig> configs = Sinks.many().multicast().directBestEffort();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs.asFlux());
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket("travel-sample")).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    when(mock101.identifier()).thenReturn(new NodeIdentifier("10.143.190.101", 8091));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock101.disconnect()).thenReturn(Mono.empty());


    Node mock102 = mock(Node.class);
    when(mock102.identifier()).thenReturn(new NodeIdentifier("10.143.190.102", 8091));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock102.disconnect()).thenReturn(Mono.empty());


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
      configs.tryEmitNext(clusterConfig);

      BucketConfig twoNodesConfig = BucketConfigParser.parse(
        readResource("two_nodes_config.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesConfig);
      configs.tryEmitNext(clusterConfig);

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
      configs.tryEmitNext(clusterConfig);

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
    when(configProvider.closeBucket("travel-sample")).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    when(mock101.identifier()).thenReturn(new NodeIdentifier("10.143.190.101", 8091));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock101.disconnect()).thenReturn(Mono.empty());

    Node mock102 = mock(Node.class);
    when(mock102.identifier()).thenReturn(new NodeIdentifier("10.143.190.102", 8091));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock102.disconnect()).thenReturn(Mono.empty());

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
      configs.tryEmitNext(clusterConfig);

      BucketConfig twoNodesConfig = BucketConfigParser.parse(
        readResource("two_nodes_config_more_services.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesConfig);
      configs.tryEmitNext(clusterConfig);

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
      configs.tryEmitNext(clusterConfig);

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
    when(configProvider.closeBucket("travel-sample")).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    when(mock101.identifier()).thenReturn(new NodeIdentifier("10.143.190.101", 8091));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock101.disconnect()).thenReturn(Mono.empty());

    Node mock102 = mock(Node.class);
    when(mock102.identifier()).thenReturn(new NodeIdentifier("10.143.190.102", 8091));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock102.disconnect()).thenReturn(Mono.empty());

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
      configs.tryEmitNext(clusterConfig);

      BucketConfig twoNodesConfig = BucketConfigParser.parse(
        readResource("two_nodes_config_more_services.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(twoNodesConfig);
      configs.tryEmitNext(clusterConfig);

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
      configs.tryEmitNext(clusterConfig);

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
    when(configProvider.configs()).thenReturn(configs.asFlux());
    when(configProvider.config()).thenReturn(clusterConfig);
    when(configProvider.shutdown()).thenReturn(Mono.empty());
    when(configProvider.closeBucket("default")).thenReturn(Mono.empty());

    Node mock101 = mock(Node.class);
    when(mock101.identifier()).thenReturn(new NodeIdentifier(LOCALHOST, 9000));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock101.disconnect()).thenReturn(Mono.empty());

    Node mock102 = mock(Node.class);
    when(mock102.identifier()).thenReturn(new NodeIdentifier(LOCALHOST, 9001));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);
    when(mock102.disconnect()).thenReturn(Mono.empty());

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
      configs.tryEmitNext(clusterConfig);

      BucketConfig oneNodeConfig = BucketConfigParser.parse(
        readResource("cluster_run_two_nodes.json", CoreTest.class),
        ENV,
        LOCALHOST
      );
      clusterConfig.setBucketConfig(oneNodeConfig);
      configs.tryEmitNext(clusterConfig);

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
    when(configProvider.closeBucket("travel-sample")).thenReturn(Mono.empty());

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

}
