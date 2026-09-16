/*
 * Copyright (c) 2026 Couchbase, Inc.
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
package com.couchbase.client.core.service.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreGetReplicaStrategy;
import com.couchbase.client.core.api.kv.CoreGetResult;
import com.couchbase.client.core.api.kv.CoreReplicaIndex;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.ReplicaIndexCurrentlyUnavailableException;
import com.couchbase.client.core.error.ReplicaIndexOutOfBoundsException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import com.couchbase.client.core.topology.PartitionTopology;
import com.couchbase.client.core.util.BucketConfigUtil;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.node.KeyValueLocator.partitionForKey;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class CoreGetReplicaStrategyFromIndex implements CoreGetReplicaStrategy {

  private final CoreReplicaIndex index;
  private final boolean wrap;

  public CoreGetReplicaStrategyFromIndex(CoreReplicaIndex index, boolean wrap) {
    this.index = requireNonNull(index);
    this.wrap = wrap;
  }

  @Override
  public CompletableFuture<CoreGetResult> execute(
    Core core,
    CollectionIdentifier collectionIdentifier,
    String documentId,
    Duration timeout,
    RetryStrategy retryStrategy,
    Map<String, Object> clientContext,
    RequestSpan parentSpan
  ) {
    return BucketConfigUtil.waitForBucketTopologyAsync(core, collectionIdentifier.bucket(), timeout)
      .thenCompose(topologyRaw -> {
        if (!(topologyRaw.bucket() instanceof CouchbaseBucketTopology)) {
          throw new FeatureNotAvailableException("This feature can only be used with Couchbase buckets");
        }
        CouchbaseBucketTopology topology = (CouchbaseBucketTopology) topologyRaw.bucket();
        int numReplicas = topology.numberOfReplicas();
        int requestedPosition = index.replicaIndex();
        int partitionId = partitionForKey(documentId.getBytes(StandardCharsets.UTF_8), topology.numberOfPartitions());

        if (numReplicas == 0) {
          throw ReplicaIndexOutOfBoundsException.forIndex(requestedPosition, numReplicas);
        }

        int replicaNumber = wrap
          ? replicaNumberWrapping(topology, partitionId, numReplicas, requestedPosition)
          : replicaNumberNotWrapping(topology, partitionId, numReplicas, requestedPosition);

        return ReplicaHelper.getReplica(
          core, collectionIdentifier, documentId, replicaNumber + 1, timeout, retryStrategy, clientContext, parentSpan
        );
      });
  }

  private int replicaNumberNotWrapping(CouchbaseBucketTopology topology, int partitionId, int numReplicas, int requestedPosition) {
    if (requestedPosition >= numReplicas) {
      throw ReplicaIndexOutOfBoundsException.forIndex(requestedPosition, numReplicas);
    }

    PartitionTopology partition = topology.partitions().get(partitionId);

    // rawNodeIndexes includes the active at element 0, so the replica chain length is one less.
    int replicaChainLength = partition.rawNodeIndexes().size() - 1;
    if (requestedPosition >= replicaChainLength) {
      throw ReplicaIndexCurrentlyUnavailableException.forIndex(requestedPosition);
    }

    OptionalInt nodeIndexForReplica = partition.nodeIndexForReplica(requestedPosition);
    if (!nodeIndexForReplica.isPresent()) {
      throw ReplicaIndexCurrentlyUnavailableException.forIndex(requestedPosition);
    }

    return requestedPosition;
  }

  private int replicaNumberWrapping(CouchbaseBucketTopology topology, int partitionId, int numReplicas, int requestedPosition) {
    int startPosition = requestedPosition % numReplicas;

    int position = startPosition;
    while (topology.nodeIndexForReplica(partitionId, position, false) < 0) {
      position = (position + 1) % numReplicas;
      if (position == startPosition) {
        throw ReplicaIndexCurrentlyUnavailableException.forIndex(index.replicaIndex());
      }
    }

    return position;
  }
}
