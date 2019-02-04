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

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.util.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the various functionality of the {@link Core}.
 */
@SuppressWarnings("UnassignedFluxMonoInstance")
class CoreTest {

  private static CoreEnvironment ENV;

  @BeforeAll
  static void beforeAll() {
    ENV = CoreEnvironment.create(mock(Credentials.class));
  }

  @AfterAll
  static void afterAll() {
    ENV.shutdown(Duration.ofSeconds(1));
  }

  /**
   * This test initializes with a first config and then pushes a second one, making sure that
   * the difference in services and nodes is enabled.
   */
  @Test
  @SuppressWarnings({"unchecked"})
  void addNodesAndServicesOnNewConfig() {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    DirectProcessor<ClusterConfig> configs = DirectProcessor.create();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs);
    when(configProvider.config()).thenReturn(clusterConfig);

    Node mock101 = mock(Node.class);
    when(mock101.address()).thenReturn(NetworkAddress.create("10.143.190.101"));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    Node mock102 = mock(Node.class);
    when(mock102.address()).thenReturn(NetworkAddress.create("10.143.190.102"));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    new Core(ENV) {
      @Override
      public ConfigurationProvider configurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NetworkAddress target) {
        return mocks.get(target.address());
      }
    };
    configs.onNext(clusterConfig);

    BucketConfig oneNodeConfig = BucketConfigParser.parse(
      Utils.readResource("one_node_config.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(oneNodeConfig);
    configs.onNext(clusterConfig);

    verify(mock101, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));


    verify(mock102, never()).addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock102, never()).addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock102, never()).addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock102, never()).addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    BucketConfig twoNodeConfig = BucketConfigParser.parse(
      Utils.readResource("two_nodes_config.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(twoNodeConfig);
    configs.onNext(clusterConfig);


    verify(mock101, times(2))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock101, times(2))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock101, times(2))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock101, times(2))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    verify(mock102, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));
  }

  @Test
  void addServicesOnNewConfig() {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    DirectProcessor<ClusterConfig> configs = DirectProcessor.create();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs);
    when(configProvider.config()).thenReturn(clusterConfig);

    Node mock101 = mock(Node.class);
    when(mock101.address()).thenReturn(NetworkAddress.create("10.143.190.101"));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    Node mock102 = mock(Node.class);
    when(mock102.address()).thenReturn(NetworkAddress.create("10.143.190.102"));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    new Core(ENV) {
      @Override
      public ConfigurationProvider configurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NetworkAddress target) {
        return mocks.get(target.address());
      }
    };
    configs.onNext(clusterConfig);

    BucketConfig twoNodesConfig = BucketConfigParser.parse(
      Utils.readResource("two_nodes_config.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(twoNodesConfig);
    configs.onNext(clusterConfig);

    verify(mock101, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    verify(mock102, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    BucketConfig twoNodesConfigMore = BucketConfigParser.parse(
      Utils.readResource("two_nodes_config_more_services.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(twoNodesConfigMore);
    configs.onNext(clusterConfig);

    verify(mock101, times(2))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock101, times(2))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock101, times(2))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock101, times(2))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    verify(mock102, times(2))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock102, times(2))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock102, times(2))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock102, times(2))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    verify(mock102, times(1))
      .addService(ServiceType.SEARCH, 8094, Optional.empty());
  }

  @Test
  void removeNodesAndServicesOnNewConfig() {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    DirectProcessor<ClusterConfig> configs = DirectProcessor.create();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs);
    when(configProvider.config()).thenReturn(clusterConfig);

    Node mock101 = mock(Node.class);
    when(mock101.address()).thenReturn(NetworkAddress.create("10.143.190.101"));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    Node mock102 = mock(Node.class);
    when(mock102.address()).thenReturn(NetworkAddress.create("10.143.190.102"));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    new Core(ENV) {
      @Override
      public ConfigurationProvider configurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NetworkAddress target) {
        return mocks.get(target.address());
      }
    };
    configs.onNext(clusterConfig);

    BucketConfig twoNodesConfig = BucketConfigParser.parse(
      Utils.readResource("two_nodes_config_more_services.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(twoNodesConfig);
    configs.onNext(clusterConfig);

    verify(mock101, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    verify(mock102, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));
    verify(mock102, times(1))
      .addService(ServiceType.SEARCH, 8094, Optional.empty());

    BucketConfig twoNodesLessServices = BucketConfigParser.parse(
      Utils.readResource("two_nodes_config.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(twoNodesLessServices);
    configs.onNext(clusterConfig);

    verify(mock102, times(1))
      .removeService(ServiceType.SEARCH, Optional.empty());
  }

  @Test
  void removesNodeIfNotPresentInConfigAnymore() {
    final ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    DirectProcessor<ClusterConfig> configs = DirectProcessor.create();
    ClusterConfig clusterConfig = new ClusterConfig();
    when(configProvider.configs()).thenReturn(configs);
    when(configProvider.config()).thenReturn(clusterConfig);

    Node mock101 = mock(Node.class);
    when(mock101.address()).thenReturn(NetworkAddress.create("10.143.190.101"));
    when(mock101.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock101.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    Node mock102 = mock(Node.class);
    when(mock102.address()).thenReturn(NetworkAddress.create("10.143.190.102"));
    when(mock102.addService(any(ServiceType.class), anyInt(), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.removeService(any(ServiceType.class), any(Optional.class)))
      .thenReturn(Mono.empty());
    when(mock102.serviceEnabled(any(ServiceType.class))).thenReturn(true);

    final Map<String, Node> mocks = new HashMap<>();
    mocks.put("10.143.190.101", mock101);
    mocks.put("10.143.190.102", mock102);
    new Core(ENV) {
      @Override
      public ConfigurationProvider configurationProvider() {
        return configProvider;
      }

      @Override
      protected Node createNode(final NetworkAddress target) {
        return mocks.get(target.address());
      }
    };
    configs.onNext(clusterConfig);

    BucketConfig twoNodesConfig = BucketConfigParser.parse(
      Utils.readResource("two_nodes_config_more_services.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(twoNodesConfig);
    configs.onNext(clusterConfig);

    verify(mock101, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock101, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));

    verify(mock102, times(1))
      .addService(ServiceType.VIEWS, 8092, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.MANAGER, 8091, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.QUERY, 8093, Optional.empty());
    verify(mock102, times(1))
      .addService(ServiceType.KV, 11210, Optional.of("travel-sample"));
    verify(mock102, times(1))
      .addService(ServiceType.SEARCH, 8094, Optional.empty());

    BucketConfig twoNodesLessServices = BucketConfigParser.parse(
      Utils.readResource("one_node_config.json", CoreTest.class),
      ENV,
      NetworkAddress.localhost()
    );
    clusterConfig.setBucketConfig(twoNodesLessServices);
    configs.onNext(clusterConfig);

    verify(mock102, times(1)).disconnect();
  }

}