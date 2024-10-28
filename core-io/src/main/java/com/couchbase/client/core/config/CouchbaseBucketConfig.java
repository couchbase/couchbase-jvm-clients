/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JacksonInject;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import com.couchbase.client.core.topology.PartitionMap;
import com.couchbase.client.core.topology.TopologyHelper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.couchbase.client.core.config.LegacyConfigHelper.getClusterCapabilities;
import static com.couchbase.client.core.config.LegacyConfigHelper.getNodeInfosForBucket;
import static com.couchbase.client.core.config.LegacyConfigHelper.getPortInfos;
import static com.couchbase.client.core.config.LegacyConfigHelper.toLegacy;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbCollections.filter;
import static com.couchbase.client.core.util.CbCollections.transform;

/**
 * @deprecated In favor of {@link com.couchbase.client.core.topology.CouchbaseBucketTopology}
 */
@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class CouchbaseBucketConfig extends AbstractBucketConfig {

    public static final int PARTITION_NOT_EXISTENT = -2;

    private final int numberOfReplicas;
    private final int numberOfPartitions;

    private final List<NodeInfo> partitionHosts;
    private final Set<NodeIdentifier> hostsWithPrimaryPartitions;

    private final List<Partition> partitions;
    private final List<Partition> forwardPartitions;

    private final boolean tainted;
    private final boolean ephemeral;

    /**
     * Creates a new {@link CouchbaseBucketConfig}.
     *
     * @param rev the revision of the config.
     * @param name the name of the bucket.
     * @param uri the URI for this bucket.
     * @param streamingUri the streaming URI for this bucket.
     * @param partitionInfo partition info for this bucket.
     * @param nodeInfos related node information.
     * @param portInfos port info for the nodes, including services.
     */
    @JsonCreator
    public CouchbaseBucketConfig(
      @JsonProperty("rev") long rev,
      @JsonProperty("revEpoch") long revEpoch,
      @JsonProperty("uuid") String uuid,
      @JsonProperty("name") String name,
      @JsonProperty("uri") String uri,
      @JsonProperty("streamingUri") String streamingUri,
      @JsonProperty("vBucketServerMap") PartitionInfo partitionInfo,
      @JsonProperty("nodes") List<NodeInfo> nodeInfos,
      @JsonProperty("nodesExt") List<PortInfo> portInfos,
      @JsonProperty("bucketCapabilities") List<BucketCapabilities> bucketCapabilities,
      @JsonProperty("clusterCapabilities") Map<String, Set<ClusterCapabilities>> clusterCapabilities,
      @JsonProperty("bucketType") BucketType bucketType,
      @JacksonInject("origin") String origin) {
        super(uuid, name, BucketNodeLocator.VBUCKET, uri, streamingUri, nodeInfos, portInfos, bucketCapabilities,
          origin, clusterCapabilities, rev, revEpoch);

        this.numberOfReplicas = partitionInfo.numberOfReplicas();
        this.numberOfPartitions = partitionInfo.partitions().size();
        this.partitions = partitionInfo.partitions();
        this.forwardPartitions = partitionInfo.forwardPartitions();
        this.tainted = partitionInfo.tainted();
        List<NodeInfo> extendedNodeInfos = this.nodes(); // includes ports for SSL services
        this.partitionHosts = buildPartitionHosts(extendedNodeInfos, partitionInfo);
        this.hostsWithPrimaryPartitions = buildNodesWithPrimaryPartitions(nodeInfos, partitionInfo.partitions());

        // When ephemeral buckets were introduced, a "bucketType" field was not part of the config. In recent
        // servers (added in 7.1.0, same time when magma got introduced) there is a new bucketType available
        // which allows us to more directly infer if it is an ephemeral bucket and no rely on the COUCHAPI
        // presence as a heuristic.
        if (bucketType != null) {
            this.ephemeral = BucketType.EPHEMERAL == bucketType;
        } else {
            // Use bucket capabilities to identify if couchapi is missing (then its ephemeral). If its null then
            // we are running an old version of couchbase which doesn't have ephemeral buckets at all.
            this.ephemeral = bucketCapabilities != null && !bucketCapabilities.contains(BucketCapabilities.COUCHAPI);
        }
    }

    @Stability.Internal
    public CouchbaseBucketConfig(ClusterTopologyWithBucket cluster) {
        super(
          cluster.bucket().uuid(),
          cluster.bucket().name(),
          BucketNodeLocator.VBUCKET,
          LegacyConfigHelper.uri(cluster.bucket()),
          LegacyConfigHelper.streamingUri(cluster.bucket()),
          getNodeInfosForBucket(cluster),
          toLegacy(cluster.bucket().capabilities()),
          getClusterCapabilities(cluster),
          "<origin-not-required>",
          getPortInfos(cluster),
          LegacyConfigHelper.toLegacy(cluster.revision()),
          cluster
        );

        CouchbaseBucketTopology bucket = (CouchbaseBucketTopology) cluster.bucket();

        this.ephemeral = bucket.ephemeral();
        this.numberOfPartitions = bucket.numberOfPartitions();
        this.numberOfReplicas = bucket.numberOfReplicas();
        this.tainted = bucket.partitionsForward().isPresent();

        this.partitions = toLegacyPartitions(bucket.partitions(), numberOfReplicas);
        this.forwardPartitions = bucket.partitionsForward()
          .map(it -> toLegacyPartitions(it, numberOfReplicas))
          .orElse(null);

        this.partitionHosts = filter(
          getNodeInfosForBucket(cluster),
          it -> it.sslServices().containsKey(ServiceType.KV) || it.services().containsKey(ServiceType.KV)
        );

        this.hostsWithPrimaryPartitions = new HashSet<>();
        bucket.partitions().values().forEach(partitionTopology ->
          partitionTopology.active().ifPresent(it -> this.hostsWithPrimaryPartitions.add(it.id().toLegacy()))
        );
    }

    private static List<Partition> toLegacyPartitions(
      PartitionMap map,
      int numberOfReplicas
    ) {
        return transform(map.values(), it -> {
            short[] replicas = new short[numberOfReplicas];
            for (int i = 0; i < numberOfReplicas; i++) {
                replicas[i] = (short) it.nodeIndexForReplica(i).orElse(PARTITION_NOT_EXISTENT);
            }
            return new Partition(
              (short) it.nodeIndexForActive().orElse(PARTITION_NOT_EXISTENT),
              replicas
            );
        });
    }

    /**
     * Pre-computes a set of nodes that have primary partitions active.
     *
     * @param nodeInfos the list of nodes.
     * @param partitions the partitions.
     * @return a set containing the addresses of nodes with primary partitions.
     */
    private static Set<NodeIdentifier> buildNodesWithPrimaryPartitions(
        final List<NodeInfo> nodeInfos,
        final List<Partition> partitions
    ) {
        Set<NodeIdentifier> nodes = new HashSet<>(nodeInfos.size());
        for (Partition partition : partitions) {
            int index = partition.active();
            if (index >= 0) {
                nodes.add(nodeInfos.get(index).identifier());
            }
        }
        return nodes;
    }

    /**
     * Builds a list of nodes that are used for KV partition lookups.
     *
     * @param nodeInfos all nodes participating in the cluster which may not have the KV service enabled.
     * @param partitionInfo the raw partition info to check against the nodes list.
     * @return an ordered reference list for the partition hosts.
     */
    private static List<NodeInfo> buildPartitionHosts(final List<NodeInfo> nodeInfos,
                                                      final PartitionInfo partitionInfo) {
        List<NodeInfo> partitionHosts = new ArrayList<>();
        for (String rawHost : partitionInfo.partitionHosts()) {
            String convertedHost;
            try {
                String[] parts = rawHost.split(":");
                String host = "";
                if (parts.length > 2) {
                    // Handle IPv6 syntax
                    for (int i = 0; i < parts.length - 1; i++) {
                        host += parts[i];
                        if (parts[i].endsWith("]")) {
                            break;
                        } else {
                            host += ":";
                        }
                    }
                    if (host.startsWith("[") && host.endsWith("]")) {
                        host = host.substring(1, host.length() - 1);
                    }
                    if (host.endsWith(":")) {
                        host = host.substring(0, host.length() - 1);
                    }
                } else {
                    // Simple IPv4 Handling
                    host = parts[0];
                }

                convertedHost = host;
            } catch (Exception e) {
                throw new ConfigException("Could not resolve " + rawHost + "on config building.", e);
            }
            for (NodeInfo nodeInfo : nodeInfos) {
                // Make sure we only take into account nodes which contain KV
                boolean directPortEnabled = nodeInfo.services().containsKey(ServiceType.KV);
                boolean sslPortEnabled = nodeInfo.sslServices().containsKey(ServiceType.KV);
                if (!directPortEnabled && !sslPortEnabled) {
                    continue;
                }

                boolean hostMatches = nodeInfo.hostname().equals(convertedHost);
                if (hostMatches && !partitionHosts.contains(nodeInfo)) {
                    partitionHosts.add(nodeInfo);
                }
            }
        }
        if (partitionHosts.size() != partitionInfo.partitionHosts().length) {
            throw new ConfigException("Partition size is not equal after conversion, this is a bug.");
        }
        return partitionHosts;
    }

    public int numberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public boolean tainted() {
        return tainted;
    }

    /**
     * @deprecated In favor of {@link #hasPrimaryPartitionsOnNode(NodeIdentifier)}
     * which handles the case where node hosts are not unique within the cluster.
     */
    @Deprecated
    public boolean hasPrimaryPartitionsOnNode(final String hostname) {
        return hostsWithPrimaryPartitions.stream().anyMatch(it -> it.address().equals(hostname));
    }

    public boolean hasPrimaryPartitionsOnNode(final NodeIdentifier nodeId) {
        return hostsWithPrimaryPartitions.contains(nodeId);
    }

    public short nodeIndexForActive(int partition, boolean useFastForward) {
        if (useFastForward && !hasFastForwardMap()) {
            throw new IllegalStateException("Could not get index from FF-Map, none found in this config.");
        }
        List<Partition> partitions = useFastForward ? forwardPartitions : this.partitions;

        try {
            return partitions.get(partition).active();
        } catch (IndexOutOfBoundsException ex) {
            return PARTITION_NOT_EXISTENT;
        }
    }

    public short nodeIndexForReplica(int partition, int replica, boolean useFastForward) {
        if (useFastForward && !hasFastForwardMap()) {
            throw new IllegalStateException("Could not get index from FF-Map, none found in this config.");
        }

        List<Partition> partitions = useFastForward ? forwardPartitions : this.partitions;

        try {
            return partitions.get(partition).replica(replica);
        } catch (IndexOutOfBoundsException ex) {
            // TODO: LOGGER.debug("Out of bounds on index for replica " + partition + ".", ex);
            return PARTITION_NOT_EXISTENT;
        }
    }

    public int numberOfPartitions() {
        return numberOfPartitions;
    }

    public NodeInfo nodeAtIndex(int nodeIndex) {
        return partitionHosts.get(nodeIndex);
    }

    @Override
    public BucketType type() {
        return BucketType.COUCHBASE;
    }

    @Override
    public boolean hasFastForwardMap() {
        return forwardPartitions != null;
    }

    public boolean ephemeral() {
        return ephemeral;
    }

    @Override
    public String toString() {
        return "CouchbaseBucketConfig{"
            + "name='" + redactMeta(name()) + '\''
            + ", locator=" + locator()
            + ", uri='" + redactMeta(uri()) + '\''
            + ", streamingUri='" + redactMeta(streamingUri()) + '\''
            + ", numberOfReplicas=" + numberOfReplicas
            + ", nodes=" + redactSystem(nodes())
            + ", partitions=" + prettyPartitions()
            + ", tainted=" + tainted
            + ", version=" + version()
            + '}';
    }

    private String prettyPartitions() {
      SortedMap<Integer, String> sortedPartitions = new TreeMap<>();
      for (int i =0; i < partitions.size(); i++) {
        sortedPartitions.put(i, partitions.get(i).toString());
      }
      return TopologyHelper.compressKeyRuns(sortedPartitions).toString();
    }
}
