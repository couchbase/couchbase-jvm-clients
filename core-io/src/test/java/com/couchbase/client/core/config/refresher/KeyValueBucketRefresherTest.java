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
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigRefreshFailure;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigRequest;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.core.util.CbCollections.mapOf;
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
    env = CoreEnvironment.builder().eventBus(eventBus).ioConfig(IoConfig.configPollInterval(FAST_CONFIG_POLL_INTERVAL)).build();

    CoreContext coreContext = mock(CoreContext.class);
    core = mock(Core.class);
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
    BucketConfig config = mock(BucketConfig.class);
    when(config.name()).thenReturn("bucket");
    clusterConfig.setBucketConfig(config);
    when(config.nodes()).thenReturn(Arrays.asList(
      new NodeInfo("foo", mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091),
        Collections.emptyMap(), Collections.emptyMap()),
      new NodeInfo("bar", mapOf(ServiceType.KV, 11210, ServiceType.MANAGER, 8091),
        Collections.emptyMap(), Collections.emptyMap())
    ));

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
