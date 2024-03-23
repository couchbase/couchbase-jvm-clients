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
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.topology.AbstractBucketTopology.parseBucketCapabilities;
import static com.couchbase.client.core.util.CbCollections.filter;

@Stability.Internal
class MemcachedBucketTopologyParser {
  private static final Logger log = LoggerFactory.getLogger(MemcachedBucketTopologyParser.class);

  public static MemcachedBucketTopology parse(
    ObjectNode configNode,
    List<HostAndServicePorts> nodes,
    MemcachedHashingStrategy hashingStrategy
  ) {
    String bucketName = configNode.path("name").asText();
    String uuid = configNode.path("uuid").asText();

    Set<BucketCapability> bucketCapabilities = parseBucketCapabilities(configNode);

    // Just a sanity check!
    List<HostAndServicePorts> nodesWithoutKetamaAuthority = filter(nodes, it -> it.ketamaAuthority() == null);
    if (!nodesWithoutKetamaAuthority.isEmpty()) {
      log.warn(
        "These nodes can't participate in the ketama ring for Memcached bucket '{}' because they didn't advertise a non-TLS KV port: {}",
        redactMeta(bucketName),
        redactSystem(nodesWithoutKetamaAuthority)
      );
    }

    List<HostAndServicePorts> ketamaNodes = filter(nodes, it -> it.ketamaAuthority() != null);
    KetamaRing<HostAndServicePorts> ketamaRing = KetamaRing.create(ketamaNodes, hashingStrategy);

    return new MemcachedBucketTopology(
      bucketName,
      uuid,
      bucketCapabilities,
      nodes,
      ketamaRing,
      hashingStrategy
    );
  }
}
