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
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbCollections.transform;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Stability.Internal
class ClusterTopologyParser {
  private static final Logger log = LoggerFactory.getLogger(ClusterTopologyParser.class);

  private ClusterTopologyParser() {
    throw new AssertionError("not instantiable");
  }

  /**
   * @param originHost The IP address or hostname of the node this config was retrieved from.
   * MUST NOT include a port. IPv6 addresses MUST NOT be enclosed in square brackets.
   */
  public static ClusterTopology parse(
    ObjectNode clusterConfig,
    String originHost,
    PortSelector portSelector,
    NetworkSelector networkSelector,
    MemcachedHashingStrategy memcachedHashingStrategy
  ) {
    requireNonNull(originHost, "originHost must be non-null.");
    if (originHost.endsWith("]")) {
      throw new IllegalArgumentException("Invalid originHost. Expected a hostname, IPv4 address, or IPv6 address (without square brackets), but got: " + originHost);
    }

    if (clusterConfig.isEmpty()) {
      throw new CouchbaseException("Cluster topology JSON is an empty node. Server returned invalid topology, or maybe this was a doomed attempt to re-parse a synthetic topology.");
    }

    ArrayNode nodesExt = (ArrayNode) clusterConfig.get("nodesExt");
    if (nodesExt == null) {
      throw new CouchbaseException("Couchbase Server version is too old for this SDK; missing 'nodesExt' field.");
    }
    List<Map<NetworkResolution, HostAndServicePorts>> nodes = JacksonHelper.transform(nodesExt, node ->
      HostAndServicePortsParser.parse(addHostnameIfMissing(node, originHost), portSelector)
    );

    NetworkResolution resolvedNetwork = networkSelector.selectNetwork(nodes);

    if (nodes.stream().noneMatch(it -> it.containsKey(resolvedNetwork))) {
      // User explicitly specified a network that doesn't exist.
      Set<NetworkResolution> availableNetworks = new HashSet<>();
      nodes.forEach(it -> availableNetworks.addAll(it.keySet()));
      throw new CouchbaseException("Requested network '" + resolvedNetwork + "' is not available for this cluster. Available networks: " + availableNetworks);
    }

    // Discard node info from networks we don't care about.
    //
    // A network SHOULD have an entry for each node, but the server doesn't
    // currently enforce this. If a node doesn't have an alternate address
    // for the selected network, use an "inaccessible" placeholder to preserve
    // the node indexes required by the KV partition map.
    List<HostAndServicePorts> resolvedNodes = transform(nodes, it -> {
      HostAndServicePorts resolved = it.getOrDefault(resolvedNetwork, HostAndServicePorts.INACCESSIBLE);
      if (resolved.inaccessible()) {
        log.error(
          "Cluster topology has at least one node that is inaccessible on the selected network ({}) : {}",
          resolvedNetwork, redactSystem(it)
        );
      }
      return resolved;
    });

    sanityCheck(resolvedNodes);

    // RELATIONSHIP BETWEEN "nodes" and "nodesEXT":
    //
    // The "nodes" array is only present in topologies with bucket information.
    // It lists the subset of KV nodes that are ready to service requests
    // for the bucket. This is especially important for parsing
    // Memcached bucket topology, because it tells us which KV nodes should
    // participate in the ketama ring. See JVMCBC-564 and CBSE-5657.
    //
    // The nodes listed in "nodes" are guaranteed to appear first in "nodesExt",
    // and in the same order in both lists.
    //
    // The info in the "nodes" array is a subset of what's in "nodesExt".
    // We don't need to actually parse "nodes" -- we just need to know
    // how many elements it has, so we can truncate the `nodesExt` list
    // to the same length when building the bucket topology.
    //
    // None of this is strictly required for parsing Couchbase (as opposed to Memcached)
    // bucket topology, since a Couchbase bucket's partition map references nodes by index,
    // and those indexes should never be greater than the number of elements in "nodes".
    // However, it doesn't hurt to truncate the list for all bucket types,
    // and it ensures we don't accidentally reference an unready KV node.
    ArrayNode nodesArray = (ArrayNode) clusterConfig.get("nodes");
    int numberOfKvNodesServicingThisBucket = nodesArray == null ? 0 : nodesArray.size();

    // Nodes present in both "nodesExt" and "nodes"
    List<HostAndServicePorts> nodesReadyToServiceThisBucket = resolvedNodes.subList(0, numberOfKvNodesServicingThisBucket);

    BucketTopology bucket = BucketTopology.parse(clusterConfig, nodesReadyToServiceThisBucket, memcachedHashingStrategy);

    ClusterIdentifier clusterIdent = ClusterIdentifier.parse(clusterConfig);

    return ClusterTopology.of(
      clusterConfig,
      TopologyRevision.parse(clusterConfig),
      clusterIdent,
      resolvedNodes,
      parseCapabilities(clusterConfig),
      resolvedNetwork,
      portSelector,
      bucket
    );
  }

  private static void sanityCheck(List<HostAndServicePorts> resolvedNodes) {
    List<NodeIdentifier> idsOfAccessibleNodes = resolvedNodes.stream()
      .filter(it -> !it.inaccessible())
      .map(HostAndServicePorts::id)
      .collect(toList());

    Set<NodeIdentifier> distinct = new HashSet<>(idsOfAccessibleNodes);
    if (distinct.size() != idsOfAccessibleNodes.size()) {
      throw new CouchbaseException(
        "Cluster topology has nodes with non-unique IDs (host and manager port on default network: " + redactSystem(resolvedNodes)
      );
    }
  }

  /**
   * A single-node cluster can omit the hostname, so patch it in!
   * <p>
   * The server omits the hostname so development nodes initialized with a hostname
   * of "localhost" or "127.0.0.1" are accessible from other machines.
   * Otherwise, the node would say, "My address is localhost!" and clients on
   * other machines would look in the wrong place.
   */
  private static ObjectNode addHostnameIfMissing(JsonNode node, String originHost) {
    ObjectNode obj = (ObjectNode) node;
    if (!node.has("hostname") && node.path("thisNode").asBoolean()) {
      obj.set("hostname", new TextNode(originHost));
    }
    return obj;
  }

  private static final TypeReference<Map<String, Set<String>>> SET_MULTIMAP_TYPE = new TypeReference<Map<String, Set<String>>>() {
  };

  /**
   * Parses the cluster capabilities from a cluster config node. For example, when given this:
   * <pre>
   * {
   *   "clusterCapabilities": {
   *     "n1ql": [
   *       "enhancedPreparedStatements"
   *     ]
   *   }
   * }
   * </pre>
   * returns a set containing {@link ClusterCapability#N1QL_ENHANCED_PREPARED_STATEMENTS}.
   * <p>
   * The map from the server is compressed into a set, because looking up capabilities
   * by service is error-prone and not particularly useful.
   */
  private static Set<ClusterCapability> parseCapabilities(ObjectNode clusterConfig) {
    JsonNode capabilitiesNode = clusterConfig.get("clusterCapabilities");
    if (capabilitiesNode == null) {
      return emptySet();
    }

    Map<String, Set<String>> map = JacksonHelper.convertValue(capabilitiesNode, SET_MULTIMAP_TYPE);

    Set<ClusterCapability> result = EnumSet.noneOf(ClusterCapability.class);
    ClusterCapability.valueList().forEach(it -> {
      if (map.getOrDefault(it.namespace(), emptySet()).contains(it.wireName())) {
        result.add(it);
      }
    });

    return result;
  }
}
