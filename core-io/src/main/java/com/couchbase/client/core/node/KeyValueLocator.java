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

package com.couchbase.client.core.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.events.node.NodePartitionLengthNotEqualEvent;
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.ObserveViaSeqnoRequest;
import com.couchbase.client.core.msg.kv.ReplicaGetRequest;
import com.couchbase.client.core.msg.kv.SyncDurabilityRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;

import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * A {@link Locator} responsible for locating the right node based on the partition of the
 * key.
 *
 * <p>Coming from 1.0, this locator has not really changed - only minor details have been
 * modified in the refactoring process.</p>
 *
 * @since 1.0.0
 */
public class KeyValueLocator implements Locator {

  @Override
  public void dispatch(final Request<? extends Response> request, final List<Node> nodes,
                       final ClusterConfig config, final CoreContext ctx) {
    if (request instanceof TargetedRequest) {
      dispatchTargeted((TargetedRequest) request, nodes, ctx);
    } else {
      KeyValueRequest r = (KeyValueRequest) request;
      String bucket = r.bucket();
      BucketConfig bucketConfig = config.bucketConfig(bucket);

      if (bucketConfig == null) {
        // Since a bucket is opened lazily, it might not be available yet (or for some
        // other reason the config is gone) - send it into retry!
        RetryOrchestrator.maybeRetry(ctx, request, ctx.core().configurationProvider().bucketConfigLoadInProgress()
          ? RetryReason.BUCKET_OPEN_IN_PROGRESS
          : RetryReason.BUCKET_NOT_AVAILABLE);
        return;
      }

      if (bucketConfig instanceof CouchbaseBucketConfig) {
        couchbaseBucket(r, nodes, (CouchbaseBucketConfig) bucketConfig, ctx);
      } else if (bucketConfig instanceof MemcachedBucketConfig) {
        memcacheBucket(r, nodes, (MemcachedBucketConfig) bucketConfig, ctx);
      } else {
        throw new IllegalStateException("Unsupported Bucket Type: " + bucketConfig
          + " for request " + request);
      }
    }
  }

  @SuppressWarnings({ "unchecked" })
  private static void dispatchTargeted(final TargetedRequest request, final List<Node> nodes,
                                       final CoreContext ctx) {
    for (Node node : nodes) {
      if (node.state() == NodeState.CONNECTED || node.state() == NodeState.DEGRADED) {
        if (!request.target().equals(node.identifier())) {
          continue;
        }
        node.send((Request) request);
        return;
      }
    }

    RetryOrchestrator.maybeRetry(ctx, (Request) request, RetryReason.NODE_NOT_AVAILABLE);
  }

  private static void couchbaseBucket(final KeyValueRequest<?> request, final List<Node> nodes,
                                      final CouchbaseBucketConfig config, CoreContext ctx) {
    if(!precheckCouchbaseBucket(request, config)) {
      return;
    }

    int partitionId = partitionForKey(request.key(), config.numberOfPartitions());
    request.partition((short) partitionId);

    int nodeId = calculateNodeId(partitionId, request, config);
    if (nodeId < 0) {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      return;
    }

    NodeInfo nodeInfo = config.nodeAtIndex(nodeId);
    for (Node node : nodes) {
      if (node.identifier().equals(nodeInfo.identifier())) {
        node.send(request);
        return;
      }
    }

    if(handleNotEqualNodeSizes(config.nodes().size(), nodes.size(), ctx)) {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      return;
    }

    throw new IllegalStateException("Node not found for request" + request);
  }

  private static boolean precheckCouchbaseBucket(final KeyValueRequest<?> request, final CouchbaseBucketConfig config) {
    if (request instanceof SyncDurabilityRequest) {
      Optional<DurabilityLevel> level = ((SyncDurabilityRequest) request).durabilityLevel();
      if (level.isPresent()
        && level.get() != DurabilityLevel.NONE
        && !config.bucketCapabilities().contains(BucketCapabilities.DURABLE_WRITE)) {
        request.fail(new FeatureNotAvailableException("Synchronous Durability is currently not available on this bucket"));
        return false;
      }
    }
    return true;
  }

  /**
   * Helper method to calculate the node if for the given partition and request type.
   *
   * @param partitionId the partition id.
   * @param request the request used.
   * @param config the current bucket configuration.
   * @return the calculated node id.
   */
  private static int calculateNodeId(int partitionId, final KeyValueRequest<?> request,
                                     final CouchbaseBucketConfig config) {
    boolean useFastForward = request.context().retryAttempts() > 0 && config.hasFastForwardMap();
    if (request instanceof ReplicaGetRequest) {
      return config.nodeIndexForReplica(partitionId, ((ReplicaGetRequest) request).replica() - 1, useFastForward);
    } else if (request instanceof ObserveViaSeqnoRequest && ((ObserveViaSeqnoRequest) request).replica() > 0) {
      return config.nodeIndexForReplica(partitionId, ((ObserveViaSeqnoRequest) request).replica() - 1, useFastForward);
    } else {
      return config.nodeIndexForMaster(partitionId, useFastForward);
    }
  }


  /**
   * Locates the proper {@link Node}s for a Memcache bucket.
   *
   * @param request the request.
   * @param nodes the managed nodes.
   * @param config the bucket configuration.
   */
  private static void memcacheBucket(final KeyValueRequest<?> request, final List<Node> nodes,
                                     final MemcachedBucketConfig config, final CoreContext ctx) {
    if(!precheckMemcacheBucket(request, config)) {
      return;
    }

    NodeIdentifier identifier = config.nodeForId(request.key());
    request.partition((short) 0);

    for (Node node : nodes) {
      if (node.identifier().equals(identifier)) {
        node.send(request);
        return;
      }
    }

    if(handleNotEqualNodeSizes(config.nodes().size(), nodes.size(), ctx)) {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      return;
    }

    throw new IllegalStateException("Node not found for request" + request);
  }

  private static boolean precheckMemcacheBucket(final KeyValueRequest<?> request, final MemcachedBucketConfig config) {
    if (request instanceof SyncDurabilityRequest) {
      Optional<DurabilityLevel> level = ((SyncDurabilityRequest) request).durabilityLevel();
      if (level.isPresent()
        && level.get() != DurabilityLevel.NONE
        && !config.bucketCapabilities().contains(BucketCapabilities.DURABLE_WRITE)) {
        request.fail(new FeatureNotAvailableException("Synchronous Durability is not available for memcache buckets"));
        return false;
      }
    }
    return true;
  }


  /**
   * Helper method to handle potentially different node sizes in the actual list and in the config.
   *
   * @return true if they are not equal, false if they are.
   */
  private static boolean handleNotEqualNodeSizes(final int configNodeSize, final int actualNodeSize,
                                                 final CoreContext ctx) {
    if (configNodeSize != actualNodeSize) {
      ctx.environment().eventBus().publish(
        new NodePartitionLengthNotEqualEvent(ctx, actualNodeSize, configNodeSize)
      );
      return true;
    }
    return false;
  }

  /**
   * Calculate the partition offset for the given key.
   *
   * @param id the document id to calculate from.
   * @param numPartitions the number of partitions in the bucket.
   * @return the calculated partition.
   */
  private static int partitionForKey(final byte[] id, final int numPartitions) {
    CRC32 crc32 = new CRC32();
    crc32.update(id, 0, id.length);
    long rv = (crc32.getValue() >> 16) & 0x7fff;
    return (int) rv &numPartitions - 1;
  }

}
