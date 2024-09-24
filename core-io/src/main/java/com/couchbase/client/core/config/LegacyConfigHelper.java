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

package com.couchbase.client.core.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.BucketCapability;
import com.couchbase.client.core.topology.BucketTopology;
import com.couchbase.client.core.topology.ClusterCapability;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import com.couchbase.client.core.topology.HostAndServicePorts;
import com.couchbase.client.core.topology.MemcachedBucketTopology;
import com.couchbase.client.core.topology.TopologyRevision;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.transform;
import static java.util.Collections.emptyMap;

@Stability.Internal
public class LegacyConfigHelper {
  private LegacyConfigHelper() {
  }

  static List<PortInfo> getPortInfos(ClusterTopology topology) {
    return transform(
      topology.nodes(),
      it -> new PortInfo(
        nonTlsPorts(topology, it),
        tlsPorts(topology, it),
        emptyMap(), // The host is always accurate -- there is never an alternate.
        it.host(),
        it.id().toLegacy(),
        it.serverGroup()
      )
    );
  }

  private static Map<ServiceType, Integer> tlsPorts(ClusterTopology topology, HostAndServicePorts node) {
    return topology.isTls() ? node.ports() : emptyMap();
  }

  private static Map<ServiceType, Integer> nonTlsPorts(ClusterTopology topology, HostAndServicePorts node) {
    return !topology.isTls() ? node.ports() : emptyMap();
  }

  /**
   * Returns the full list of nodes in the cluster, but without
   * any KV or VIEWS services on nodes that aren't servicing the bucket.
   *
   * @see AbstractBucketConfig#nodeInfoFromExtended
   */
  static List<NodeInfo> getNodeInfosForBucket(ClusterTopologyWithBucket topology) {
    BucketTopology bucket = topology.bucket();

    List<HostAndServicePorts> allNodes = topology.nodes();

    // This dates back to JVMCBC-395. Removing the Views service seems like a kludge.
    // I'm not sure of the reason. There is already a pre-flight check in
    // ViewLocator.checkServiceNotAvailable() that throws FeatureNotAvailable
    // if a user attempts a view request with an ephemeral bucket.
    if (!bucket.hasCapability(BucketCapability.COUCHAPI)) {
      allNodes = transform(allNodes, it -> it.without(ServiceType.VIEWS));
    }

    int numberOfBucketNodes = bucket.nodes().size();
    List<HostAndServicePorts> bucketNodes = allNodes.subList(0, numberOfBucketNodes);
    List<HostAndServicePorts> otherNodes = allNodes.subList(numberOfBucketNodes, allNodes.size());

    // Don't need to modify the bucket nodes, so add them to the result just as they are.
    List<HostAndServicePorts> result = new ArrayList<>(bucketNodes);

    // Add the other nodes, but strip out the KV and VIEWS service,
    // and mark as ineligible for a Memcached bucket's ketama ring.
    for (HostAndServicePorts node : otherNodes) {
      result.add(
        node
          .without(ServiceType.KV, ServiceType.VIEWS)
          .withKetamaAuthority(null)
      );
    }

    return transform(
      result,
      it -> new NodeInfo(
        it.host(),
        nonTlsPorts(topology, it),
        tlsPorts(topology, it),
        it.ketamaAuthority(),
        it.id().toLegacy()
      ));
  }

  static Map<ServiceType, Set<ClusterCapabilities>> getClusterCapabilities(ClusterTopology topology) {
    Map<ServiceType, Set<ClusterCapabilities>> caps = new EnumMap<>(ServiceType.class);
    for (ServiceType service : ServiceType.values()) {
      caps.put(service, new HashSet<>());
    }
    if (topology.hasCapability(ClusterCapability.N1QL_ENHANCED_PREPARED_STATEMENTS)) {
      caps.get(ServiceType.QUERY).add(ClusterCapabilities.ENHANCED_PREPARED_STATEMENTS);
    }
    if (topology.hasCapability(ClusterCapability.SEARCH_VECTOR)) {
      caps.get(ServiceType.SEARCH).add(ClusterCapabilities.VECTOR_SEARCH);
    }
    if (topology.hasCapability(ClusterCapability.SEARCH_SCOPED)) {
      caps.get(ServiceType.SEARCH).add(ClusterCapabilities.SCOPED_SEARCH_INDEX);
    }
    return caps;
  }

  public static String uri(BucketTopology bucket) {
    return CoreHttpPath.formatPath("/pools/default/buckets/{}/?bucket_uuid={}", bucket.name(), bucket.uuid());
  }

  public static String streamingUri(BucketTopology bucket) {
    return CoreHttpPath.formatPath("/pools/default/bucketsStreaming/{}/?bucket_uuid={}", bucket.name(), bucket.uuid());
  }

  public static BucketConfig toLegacyBucketConfig(ClusterTopologyWithBucket cluster) {
    BucketTopology bucket = cluster.bucket();
    if (bucket instanceof MemcachedBucketTopology) {
      return new MemcachedBucketConfig(cluster, ((MemcachedBucketTopology) bucket).hashingStrategy());
    }

    if (bucket instanceof CouchbaseBucketTopology) {
      return new CouchbaseBucketConfig(cluster);
    }

    throw new IllegalArgumentException("Unsupported bucket type: " + cluster);
  }

  static Set<BucketCapabilities> toLegacy(Set<BucketCapability> capabilities) {
    Set<BucketCapabilities> result = EnumSet.noneOf(BucketCapabilities.class);
    for (BucketCapability cap : capabilities) {
      try {
        result.add(BucketCapabilities.valueOf(cap.name()));
      } catch (IllegalArgumentException ignoreNewCapability) {
      }
    }
    return result;
  }

  public static ConfigVersion toLegacy(TopologyRevision revision) {
    return new ConfigVersion(
      revision.epoch(),
      revision.rev()
    );
  }
}
