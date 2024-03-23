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
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Set;

@Stability.Internal
public interface BucketTopology {
  /**
   * Returns the UUID of the bucket.
   * <p>
   * The UUID is an opaque value assigned when the bucket is created.
   * If the bucket is deleted and a new bucket is created with the same name,
   * the new bucket will have a different UUID.
   */
  String uuid();

  /**
   * Returns the name of the bucket.
   */
  String name();

  Set<BucketCapability> capabilities();

  default boolean hasCapability(BucketCapability capability) {
    return capabilities().contains(capability);
  }

  /**
   * Returns the subset of cluster nodes that are ready to service
   * requests for this bucket.
   * <p>
   * This method is a candidate for eventual removal, since it's used only by
   * unit tests and the code that converts a BucketTopology to a legacy {@link BucketConfig}.
   */
  @Stability.Internal
  List<HostAndServicePorts> nodes();

  /**
   * @param nodes The info from "nodesExt" for nodes that are *also* in the top-level "nodes" array.
   * The presence in both "nodes" and "nodesExt" indicates the node is ready to service requests for this bucket.
   */
  @Stability.Internal
  static @Nullable BucketTopology parse(
    ObjectNode json,
    List<HostAndServicePorts> nodes,
    MemcachedHashingStrategy memcachedHashingStrategy
  ) {
    switch (json.path("nodeLocator").asText()) {
      case "vbucket":
        return CouchbaseBucketTopologyParser.parse(json, nodes);
      case "ketama":
        return MemcachedBucketTopologyParser.parse(json, nodes, memcachedHashingStrategy);
      default:
        // this was a "global" config with no bucket information (or an exotic new bucket type!)
        return null;
    }
  }
}
