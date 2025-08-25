/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.topology;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.env.NetworkResolution;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * A cluster topology that also has topology info for one bucket.
 * <p>
 * This is what you get from a KV connection if a bucket is selected,
 * or from the Manager service's "/pools/default/b/[bucketName]" HTTP endpoint.
 */
@Stability.Internal
public class ClusterTopologyWithBucket extends ClusterTopology {
  private final BucketTopology bucket;

  ClusterTopologyWithBucket(
    ObjectNode json,
    TopologyRevision revision,
    List<HostAndServicePorts> nodes,
    Set<ClusterCapability> capabilities,
    NetworkResolution network,
    PortSelector portSelector,
    BucketTopology bucket,
    @Nullable ClusterIdentifier clusterIdent
  ) {
    super(json, revision, nodes, capabilities, network, portSelector, clusterIdent);
    this.bucket = requireNonNull(bucket);
  }

  public BucketTopology bucket() {
    return bucket;
  }
}
