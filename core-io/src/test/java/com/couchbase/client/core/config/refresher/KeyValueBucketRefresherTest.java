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
package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigRefreshFailure;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ConfigurationProvider.TopologyPollingTrigger;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigRequest;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopologyBuilder;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.Reactor.safeInterval;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeyValueBucketRefresherTest {

  private static final Duration FAST_CONFIG_POLL_INTERVAL = Duration.ofMillis(300);

  private CoreEnvironment env;
  private Core core;

  @BeforeEach
  void beforeEach() {
    SimpleEventBus eventBus = new SimpleEventBus(true);
    env = CoreEnvironment.builder()
      .eventBus(eventBus)
      .ioConfig(io -> io.configPollInterval(FAST_CONFIG_POLL_INTERVAL))
      .build();

    CoreContext coreContext = mock(CoreContext.class);
    core = mockCore();
    when(core.context()).thenReturn(coreContext);
    when(coreContext.environment()).thenReturn(env);
  }

  @AfterEach
  void afterEach() {
    env.shutdown();
  }


  @Test
  @SuppressWarnings("unchecked")
  void triggersEventIfAllNodesFailedToRefresh() {
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    ClusterConfig clusterConfig = new ClusterConfig();
    when(provider.config()).thenReturn(clusterConfig);
    when(provider.topologyPollingTriggers(any()))
      .thenReturn(safeInterval(Duration.ofMillis(10), Schedulers.parallel()).map(it -> TopologyPollingTrigger.TIMER));

    ClusterTopologyWithBucket config = new ClusterTopologyBuilder()
      .addNode("foo", node -> node.ports(mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091)))
      .addNode("bar", node -> node.ports(mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091)))
      .couchbaseBucket("bucket")
      .replicas(2)
      .build();

    clusterConfig.setBucketConfig(config);

    final AtomicInteger invocationCounter = new AtomicInteger(0);

    doAnswer(invocation -> {
      invocationCounter.incrementAndGet();
      CarrierBucketConfigRequest request = invocation.getArgument(0);

      CarrierBucketConfigResponse response = mock(CarrierBucketConfigResponse.class);
      when(response.content()).thenReturn(Bytes.EMPTY_BYTE_ARRAY);
      when(response.status()).thenReturn(ResponseStatus.NOT_FOUND);
      request.fail(new RuntimeException("Request Failed"));

      return null;
    }).when(core).send(isA(Request.class));

    KeyValueBucketRefresher refresher = new KeyValueBucketRefresher(provider, core) {
      @Override
      protected Duration pollerInterval() {
        return Duration.ofMillis(10);
      }
    };

    refresher.register("bucket").block();

    verify(provider, timeout(1000).atLeast(1))
      .signalConfigRefreshFailed(any(ConfigRefreshFailure.class));
  }

}
