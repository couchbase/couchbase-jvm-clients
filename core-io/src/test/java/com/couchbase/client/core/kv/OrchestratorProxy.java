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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueResponse;
import com.couchbase.client.core.msg.kv.RangeScanCreateRequest;
import com.couchbase.client.core.msg.kv.RangeScanCreateResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorProxy {

  private final Core core;

  private final RangeScanOrchestrator rangeScanOrchestrator;

  private final CouchbaseBucketConfig bucketConfig;

  public OrchestratorProxy(final CoreEnvironment environment, boolean capabilityEnabled, Map<Short, List<CoreRangeScanItem>> data) {
    // Set up Bucket Config
    ClusterConfig clusterConfig = new ClusterConfig();
    bucketConfig = mock(CouchbaseBucketConfig.class);
    CollectionIdentifier collectionIdentifier = CollectionIdentifier.fromDefault("testbucket");
    when(bucketConfig.name()).thenReturn(collectionIdentifier.bucket());
    when(bucketConfig.bucketCapabilities())
      .thenReturn(capabilityEnabled ? EnumSet.of(BucketCapabilities.RANGE_SCAN) : Collections.emptySet());
    clusterConfig.setBucketConfig(bucketConfig);

    // Set up config provider
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.configs()).thenReturn(Flux.just(clusterConfig));
    when(configurationProvider.config()).thenReturn(clusterConfig);

    // Set up core
    core = mockCore();
    CoreContext coreContext = new CoreContext(core, 1, environment, null);
    when(core.context()).thenReturn(coreContext);
    when(core.configurationProvider()).thenReturn(configurationProvider);

    rangeScanOrchestrator = new RangeScanOrchestrator(core, collectionIdentifier);

    prepare(data);
  }

  private void prepare(final Map<Short, List<CoreRangeScanItem>> data) {
    when(bucketConfig.numberOfPartitions()).thenReturn(data.size());

    Map<Short, String> uuids = new HashMap<>();
    Map<String, Short> reverseUuids = new HashMap<>();
    for (Short k : data.keySet()) {
      String uuid = UUID.randomUUID().toString().substring(0, 16);
      uuids.put(k, uuid);
      reverseUuids.put(uuid, k);
    }

    doAnswer(invocation -> {
      RangeScanCreateRequest req = invocation.getArgument(0);
      req.succeed(new RangeScanCreateResponse(
        ResponseStatus.SUCCESS,
        new CoreRangeScanId(Unpooled.copiedBuffer(uuids.get(req.partition()), StandardCharsets.UTF_8))
      ));
      return null;
    }).when(core).send(isA(RangeScanCreateRequest.class));

    doAnswer(invocation -> {
      RangeScanContinueRequest req = invocation.getArgument(0);

      short partition = reverseUuids.get(new String(req.rangeScanId().bytes(), StandardCharsets.UTF_8));
      if (!req.completed()) {
        req.succeed(new RangeScanContinueResponse(ResponseStatus.SUCCESS,
          Sinks.many().multicast().onBackpressureBuffer(), false)
        );
      }
      RangeScanContinueResponse res;
      try {
        res = req.response().get();
        res.feedItems(data.get(partition), true, true);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      return null;
    }).when(core).send(isA(RangeScanContinueRequest.class));
  }

  List<CoreRangeScanItem> runRangeScan(final CoreRangeScan rangeScan, final CoreScanOptions scanOptions) {
    return rangeScanOrchestrator.rangeScan(rangeScan, scanOptions)
      .collectList().block();
  }

  List<CoreRangeScanItem> runSamplingScan(final CoreSamplingScan sampleScan, final CoreScanOptions scanOptions) {
    return rangeScanOrchestrator.samplingScan(sampleScan, scanOptions).collectList().block();
  }

}
