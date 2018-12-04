package com.couchbase.client.core.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.msg.kv.BucketConfigRequest;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;

import java.util.List;
import java.util.zip.CRC32;

public class KeyValueLocator implements Locator {

  @Override
  public void dispatch(final Request<? extends Response> request, final List<Node> nodes,
                       final ClusterConfig config, final CoreContext ctx) {
    if (request instanceof TargetedRequest) {
      for (Node n : nodes) {
        if (n.address().equals(((TargetedRequest) request).target())) {
          n.send(request);
        }
      }
      // toDO: not found (also check for connected?) .. retry?
    } else {
      KeyValueRequest r = (KeyValueRequest) request;
      String bucket = r.bucket();
      BucketConfig bucketConfig = config.bucketConfig(bucket);
      if (bucketConfig instanceof CouchbaseBucketConfig) {
        locateForCouchbaseBucket(r, nodes, (CouchbaseBucketConfig) bucketConfig, ctx);
      } else if (bucketConfig instanceof MemcachedBucketConfig) {
        throw new UnsupportedOperationException("Implement me");
      } else {
        throw new IllegalStateException("Unsupported Bucket Type: " + bucket + " for request " + request);
      }
    }
  }

  private static void locateForCouchbaseBucket(final KeyValueRequest<?> request, final List<Node> nodes,
                                               final CouchbaseBucketConfig config, CoreContext ctx) {
    int partitionId = partitionForKey(request.key(), config.numberOfPartitions());
    request.partition((short) partitionId);

    boolean useFastForward = request.context().retryAttempts() > 0 && config.hasFastForwardMap();
    int nodeId = config.nodeIndexForMaster(partitionId, useFastForward);
    if (nodeId < 0) {
      // TODO errorObservables(nodeId, request, config.name(), env, responseBuffer);
      return;
    }

    NodeInfo nodeInfo = config.nodeAtIndex(nodeId);

    for (Node node : nodes) {
      if (node.address().equals(nodeInfo.hostname())) {
        node.send(request);
        return;
      }
    }

    if(handleNotEqualNodeSizes(config.nodes().size(), nodes.size())) {
      RetryOrchestrator.maybeRetry(ctx, request);
      return;
    }

    throw new IllegalStateException("Node not found for request" + request);
  }

  /**
   * Helper method to handle potentially different node sizes in the actual list and in the config.
   *
   * @return true if they are not equal, false if they are.
   */
  private static boolean handleNotEqualNodeSizes(int configNodeSize, int actualNodeSize) {
    if (configNodeSize != actualNodeSize) {
      // TODO event bus
      /*if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Node list and configuration's partition hosts sizes : {} <> {}, rescheduling",
          actualNodeSize, configNodeSize);
      }*/
      return true;
    }
    return false;
  }

  /**
   * Calculate the vbucket for the given key.
   *
   * @param key the key to calculate from.
   * @param numPartitions the number of partitions in the bucket.
   * @return the calculated partition.
   */
  private static int partitionForKey(byte[] key, int numPartitions) {
    CRC32 crc32 = new CRC32();
    crc32.update(key);
    long rv = (crc32.getValue() >> 16) & 0x7fff;
    return (int) rv &numPartitions - 1;
  }




}
