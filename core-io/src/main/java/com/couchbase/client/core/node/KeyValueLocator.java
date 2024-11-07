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
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.kv.PredeterminedPartitionRequest;
import com.couchbase.client.core.msg.kv.SyncDurabilityRequest;
import com.couchbase.client.core.retry.AuthErrorDecider;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.topology.NodeIdentifier;

import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

import static java.util.Objects.requireNonNull;

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
    if (request.target() != null) {
      dispatchTargeted(request, nodes, ctx);
    } else {
      KeyValueRequest r = (KeyValueRequest) request;
      String bucket = r.bucket();
      BucketConfig bucketConfig = config.bucketConfig(bucket);

      if (bucketConfig == null) {
        boolean isAuthError = AuthErrorDecider.isAuthError(ctx.core().internalDiagnostics());

        RetryReason retryReason = isAuthError ? RetryReason.AUTHENTICATION_ERROR

          // Since a bucket is opened lazily, it might not be available yet (or for some
          // other reason the config is gone) - send it into retry!
          : ctx.core().configurationProvider().bucketConfigLoadInProgress()
          ? RetryReason.BUCKET_OPEN_IN_PROGRESS
          : RetryReason.BUCKET_NOT_AVAILABLE;

        RetryOrchestrator.maybeRetry(ctx, request, retryReason);
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

  private static void dispatchTargeted(final Request<?> request, final List<Node> nodes,
                                       final CoreContext ctx) {
    NodeIdentifier target = requireNonNull(request.target());
    for (Node node : nodes) {
      if (node.state() == NodeState.CONNECTED || node.state() == NodeState.DEGRADED) {
        if (!target.equals(node.identifier())) {
          continue;
        }
        node.send(request);
        return;
      }
    }

    handleTargetNotAvailable(request, nodes, ctx);
  }

  /**
   * When a targeted request cannot be dispatched, apply some more logic to figure out what to do with it.
   * <p>
   * In the specific case there are two situations that can happen: either the target is there, it's just not ready
   * to serve requests (yet) or it is not even part of the node list anymore. In the latter case there is no way
   * the request is going to make progress, so cancel it and give the caller a chance to fetch a new target and
   * send a new request.
   *
   * @param request the request to check.
   * @param nodes the nodes list to check against.
   * @param ctx the core context.
   */
  private static void handleTargetNotAvailable(final Request<?> request, final List<Node> nodes,
                                               final CoreContext ctx) {
    NodeIdentifier target = requireNonNull(request.target());
    for (Node node : nodes) {
      if (target.equals(node.identifier())) {
        RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
        return;
      }
    }

    request.cancel(CancellationReason.TARGET_NODE_REMOVED);
  }

  private static void couchbaseBucket(final KeyValueRequest<?> request, final List<Node> nodes,
                                      final CouchbaseBucketConfig config, CoreContext ctx) {
    if(!precheckCouchbaseBucket(request, config)) {
      return;
    }

    int partitionId;
    if (request instanceof PredeterminedPartitionRequest) {
      partitionId = request.partition();
    } else {
      partitionId = partitionForKey(request.key(), config.numberOfPartitions());
      request.partition((short) partitionId);
    }

    int nodeId = calculateNodeId(partitionId, request, config);
    if (nodeId < 0) {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      return;
    }

    NodeInfo nodeInfo = config.nodeAtIndex(nodeId);
    for (Node node : nodes) {
      if (node.identifier().equals(nodeInfo.id())) {
        node.send(request);
        return;
      }
    }

    if(handleNotEqualNodeSizes(config.nodes().size(), nodes.size(), ctx)) {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      return;
    }

    if (ctx.core().configurationProvider().bucketConfigLoadInProgress()) {
      RetryOrchestrator.maybeRetry(ctx, request, AuthErrorDecider.isAuthError(ctx.core().internalDiagnostics())
        ? RetryReason.AUTHENTICATION_ERROR : RetryReason.BUCKET_OPEN_IN_PROGRESS);
    } else {
      throw new IllegalStateException("Node not found for request " + request);
    }
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

    // Only use the Fast-Forward node if we have a map in the first place, and more importantly,
    // if the request at least got rejected once from a different node with a "not my vbucket"
    // response. This prevents the client going to the newer node prematurely and potentially
    // having the request being stuck on the server side during rebalance.
    boolean useFastForward = config.hasFastForwardMap() && request.rejectedWithNotMyVbucket() > 0;

    int replica = request.replica();
    return replica == 0
      ? config.nodeIndexForActive(partitionId, useFastForward)
      : config.nodeIndexForReplica(partitionId, replica - 1, useFastForward);
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

    NodeIdentifier identifier = config.nodeForKey(request.key()).id();
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
        request.fail(new FeatureNotAvailableException("Synchronous Durability is not available for memcached buckets"));
        return false;
      }
    } else if (!request.collectionIdentifier().isDefault()) {
      request.fail(FeatureNotAvailableException.collectionsForMemcached());
      return false;
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
  public static int partitionForKey(final byte[] id, final int numPartitions) {
    CRC32 crc32 = new CRC32();
    crc32.update(id, 0, id.length);
    long rv = (crc32.getValue() >> 16) & 0x7fff;
    return (int) rv &numPartitions - 1;
  }

}
