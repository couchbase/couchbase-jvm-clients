/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.java.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Bucket;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Helper class to provide direct access on how document IDs are mapped onto nodes.
 */
@Stability.Uncommitted
public class NodeLocatorHelper {

  private final AtomicReference<BucketConfig> bucketConfig = new AtomicReference<>();

  private NodeLocatorHelper(final Bucket bucket, final Duration waitUntilReadyDuration) {
    if (waitUntilReadyDuration.getSeconds() > 0) {
      bucket.waitUntilReady(waitUntilReadyDuration);
    }

    ConfigurationProvider configurationProvider = bucket.core().configurationProvider();

    BucketConfig bc = configurationProvider.config().bucketConfig(bucket.name());
    if (bc == null) {
      throw new CouchbaseException("Bucket configuration not found, if waitUntilReadyDuration is set to 0 the call" +
        "must be executed before initializing the NodeLocatorHelper!");
    }
    bucketConfig.set(configurationProvider.config().bucketConfig(bucket.name()));

    configurationProvider
      .configs()
      .subscribe(cc -> {
        BucketConfig newConfig = cc.bucketConfig(bucket.name());
        if (newConfig != null) {
          bucketConfig.set(newConfig);
        }
      });
  }

  /**
   * Creates a new {@link NodeLocatorHelper}, mapped on to the given {@link Bucket}.
   *
   * To make sure that the helper has a bucket config to work with in the beginning, it will call
   * {@link Bucket#waitUntilReady(Duration)} with the duration provided as an argument. If you already
   * did call waitUntilReady before initializing the helper, you can pass a duration of 0 in which case
   * it will be omitted.
   *
   * @param bucket the scoped bucket.
   * @param waitUntilReadyDuration the duration used to call waitUntilReady (if 0 ignored).
   * @return the created locator.
   */
  public static NodeLocatorHelper create(final Bucket bucket, final Duration waitUntilReadyDuration) {
    return new NodeLocatorHelper(bucket, waitUntilReadyDuration);
  }

  /**
   * Returns the target active node address for a given document ID on the bucket.
   *
   * @param id the document id to convert.
   * @return the node for the given document id.
   */
  public String activeNodeForId(final String id) {
    BucketConfig config = bucketConfig.get();

    if (config instanceof CouchbaseBucketConfig) {
      return nodeForIdOnCouchbaseBucket(id, (CouchbaseBucketConfig) config);
    } else if (config instanceof MemcachedBucketConfig) {
      return nodeForIdOnMemcachedBucket(id, (MemcachedBucketConfig) config);
    } else {
      throw new UnsupportedOperationException("Bucket type not supported: " + config.getClass().getName());
    }
  }

  /**
   * Returns all target replica nodes which are currently available on the bucket.
   *
   * @param id the document ID to check.
   * @return the list of nodes for the given document ID.
   */
  public List<String> availableReplicaNodesForId(final String id) {
    BucketConfig config = bucketConfig.get();

    if (config instanceof CouchbaseBucketConfig) {
      CouchbaseBucketConfig cbc = (CouchbaseBucketConfig) config;
      List<String> replicas = new ArrayList<>();
      for (int i = 1; i <= cbc.numberOfReplicas(); i++) {
        String foundReplica = replicaNodeForId(id, i, false);
        if (foundReplica != null) {
          replicas.add(foundReplica);
        }
      }
      return replicas;
    } else {
      throw new UnsupportedOperationException("Bucket type not supported: " + config.getClass().getName());
    }
  }

  /**
   * Returns all target replica nodes addresses for a given document ID on the bucket.
   *
   * @param id the document id to convert.
   * @return the node for the given document id.
   */
  public List<String> replicaNodesForId(final String id) {
    BucketConfig config = bucketConfig.get();

    if (config instanceof CouchbaseBucketConfig) {
      CouchbaseBucketConfig cbc = (CouchbaseBucketConfig) config;
      List<String> replicas = new ArrayList<>();
      for (int i = 1; i <= cbc.numberOfReplicas(); i++) {
        replicas.add(replicaNodeForId(id, i));
      }
      return replicas;
    } else {
      throw new UnsupportedOperationException("Bucket type not supported: " + config.getClass().getName());
    }
  }

  /**
   * Returns the target replica node address for a given document ID and replica number on the bucket.
   *
   * @param id the document id to convert.
   * @param replicaNum the replica number.
   * @return the node for the given document id.
   */
  public String replicaNodeForId(final String id, final int replicaNum) {
    return replicaNodeForId(id, replicaNum, true);
  }

  /**
   * Returns the target replica node address for a given document ID and replica number on the bucket.
   *
   * @param id the document id to convert.
   * @param replicaNum the replica number.
   * @param throwOnNotAvailable if on -1 and -2 an exception should be thrown.
   * @return the node for the given document id.
   */
  private String replicaNodeForId(final String id, final int replicaNum, final boolean throwOnNotAvailable) {
    if (replicaNum < 1 || replicaNum > 3) {
      throw new IllegalArgumentException("Replica number must be between 1 and 3.");
    }

    BucketConfig config = bucketConfig.get();

    if (config instanceof CouchbaseBucketConfig) {
      CouchbaseBucketConfig cbc = (CouchbaseBucketConfig) config;
      int partitionId = (int) hashId(id) & cbc.numberOfPartitions() - 1;
      int nodeId = cbc.nodeIndexForReplica(partitionId, replicaNum - 1, false);
      if (nodeId == -1) {
        if (throwOnNotAvailable) {
          throw new IllegalStateException("No partition assigned to node for Document ID: " + id);
        } else {
          return null;
        }
      }
      if (nodeId == -2) {
        if (throwOnNotAvailable) {
          throw new IllegalStateException("Replica not configured for this bucket.");
        } else {
          return null;
        }
      }
      return cbc.nodeAtIndex(nodeId).hostname();
    }  else {
      throw new UnsupportedOperationException("Bucket type not supported: " + config.getClass().getName());
    }
  }

  /**
   * Returns all nodes known in the current config.
   *
   * @return all currently known nodes.
   */
  public List<String> nodes() {
    return bucketConfig.get().nodes().stream().map(NodeInfo::hostname).collect(Collectors.toList());
  }

  private static String nodeForIdOnCouchbaseBucket(final String id, final CouchbaseBucketConfig config) {
    int partitionId = (int) hashId(id) & config.numberOfPartitions() - 1;
    int nodeId = config.nodeIndexForActive(partitionId, false);
    if (nodeId == -1) {
      throw new IllegalStateException("No partition assigned to node for Document ID: " + id);
    }
    return config.nodeAtIndex(nodeId).hostname();
  }

  private static String nodeForIdOnMemcachedBucket(final String id, final MemcachedBucketConfig config) {
    return config.nodeForKey(id.getBytes(UTF_8)).hostname();
  }

  private static long hashId(final String id) {
    CRC32 crc32 = new CRC32();
    crc32.update(id.getBytes(UTF_8));
    return (crc32.getValue() >> 16) & 0x7fff;
  }

}
