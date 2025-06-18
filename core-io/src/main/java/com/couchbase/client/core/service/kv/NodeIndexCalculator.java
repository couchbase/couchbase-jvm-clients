/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.service.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreReadPreference;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.PortInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static com.couchbase.client.core.api.kv.CoreReadPreference.PREFERRED_SERVER_GROUP;
import static com.couchbase.client.core.api.kv.CoreReadPreference.PREFERRED_SERVER_GROUP_OR_ALL_AVAILABLE;
import static com.couchbase.client.core.node.KeyValueLocator.partitionForKey;

@Stability.Internal
public class NodeIndexCalculator {
  private final boolean[] allowedNodeIndexes;
  private final CouchbaseBucketConfig topology;
  private final static Logger logger = LoggerFactory.getLogger(NodeIndexCalculator.class);

  public NodeIndexCalculator(CoreReadPreference readPreference, CouchbaseBucketConfig topology, CoreContext coreContext) {
    boolean[] allowedNodeIndexesOut = new boolean[topology.nodes().size()];

    for (int nodeIndex = 0; nodeIndex < topology.portInfos().size(); nodeIndex++) {
      boolean canUseNode = true;
      PortInfo node = topology.portInfos().get(nodeIndex);

      if (readPreference == PREFERRED_SERVER_GROUP || readPreference == PREFERRED_SERVER_GROUP_OR_ALL_AVAILABLE) {
        canUseNode = node.serverGroup() != null && node.serverGroup().equals(coreContext.environment().preferredServerGroup());
      }

      allowedNodeIndexesOut[nodeIndex] = canUseNode;
    }

    this.allowedNodeIndexes = allowedNodeIndexesOut;
    this.topology = topology;
  }

  public boolean canUseNode(String documentId, int replicaIndex, boolean isActive) {
    boolean useFastForward = false;
    int partitionId = partitionForKey(documentId.getBytes(StandardCharsets.UTF_8), topology.numberOfPartitions());
    int nodeIndex;

    if (isActive) {
      nodeIndex = topology.nodeIndexForActive(partitionId, useFastForward);
    } else {
      nodeIndex = topology.nodeIndexForReplica(partitionId, replicaIndex, useFastForward);
    }

    boolean ret = check(nodeIndex);

    logger.trace("Checking whether doc can use node.  doc={} isActive={} replica={} pid={} ni={} allowed={} canUse={}", documentId, isActive, replicaIndex, partitionId, nodeIndex, allowedNodeIndexes, ret);

    return ret;
  }

  public boolean canUseNodeForActive(String documentId) {
    return canUseNode(documentId, 0, true);
  }

  public boolean canUseNodeForReplica(String documentId, int replicaIndex) {
    return canUseNode(documentId, replicaIndex, false);
  }

  public boolean check(int nodeIndex) {
    if (nodeIndex < 0 || nodeIndex > allowedNodeIndexes.length) {
      return false;
    }
    return allowedNodeIndexes[nodeIndex];
  }
}
