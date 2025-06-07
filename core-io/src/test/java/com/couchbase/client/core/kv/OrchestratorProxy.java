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
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueResponse;
import com.couchbase.client.core.msg.kv.RangeScanCreateRequest;
import com.couchbase.client.core.msg.kv.RangeScanCreateResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.BucketCapability;
import com.couchbase.client.core.topology.ClusterTopologyBuilder;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorProxy {

  private final Core core;

  private final RangeScanOrchestrator rangeScanOrchestrator;

  private ClusterTopologyWithBucket newTopologyWithPartitionCount(int partitions, Set<BucketCapability> bucketCapabilities) {
    return new ClusterTopologyBuilder()
      .addNode("127.0.0.1", node -> node.ports(mapOf(ServiceType.MANAGER, 8091, ServiceType.KV, 11210)))
      .couchbaseBucket("test-bucket")
      .capabilities(bucketCapabilities)
      .numPartitions(partitions)
      .build();
  }

  public OrchestratorProxy(final CoreEnvironment environment, boolean capabilityEnabled, Map<Short, List<CoreRangeScanItem>> data) {
    // Set up Bucket Config
    Set<BucketCapability> bucketCapabilities = capabilityEnabled
      ? setOf(BucketCapability.RANGE_SCAN)
      : emptySet();
    ClusterTopologyWithBucket bucketConfig = newTopologyWithPartitionCount(data.size(), bucketCapabilities);
    CollectionIdentifier collectionIdentifier = CollectionIdentifier.fromDefault(bucketConfig.bucket().name());
    ClusterConfig clusterConfig = new ClusterConfig();
    clusterConfig.setBucketConfig(bucketConfig);

    // Set up core
    core = mockCore(environment);
    when(core.clusterConfig()).thenReturn(clusterConfig);

    rangeScanOrchestrator = new RangeScanOrchestrator(core, collectionIdentifier);

    prepare(data);
  }

  private void prepare(final Map<Short, List<CoreRangeScanItem>> data) {
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
