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

import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JacksonInject;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CouchbaseBucketConfig extends AbstractBucketConfig {

    public static final int PARTITION_NOT_EXISTENT = -2;

    private final PartitionInfo partitionInfo;
    private final List<NodeInfo> partitionHosts;
    private final Set<String> nodesWithPrimaryPartitions;

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
        this.partitionInfo = partitionInfo;
        this.tainted = partitionInfo.tainted();
        List<NodeInfo> extendedNodeInfos = this.nodes(); // includes ports for SSL services
        this.partitionHosts = buildPartitionHosts(extendedNodeInfos, partitionInfo);
        this.nodesWithPrimaryPartitions = buildNodesWithPrimaryPartitions(nodeInfos, partitionInfo.partitions());

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

    /**
     * Pre-computes a set of nodes that have primary partitions active.
     *
     * @param nodeInfos the list of nodes.
     * @param partitions the partitions.
     * @return a set containing the addresses of nodes with primary partitions.
     */
    private static Set<String> buildNodesWithPrimaryPartitions(final List<NodeInfo> nodeInfos,
                                                               final List<Partition> partitions) {
        Set<String> nodes = new HashSet<>(nodeInfos.size());
        for (Partition partition : partitions) {
            int index = partition.active();
            if (index >= 0) {
                nodes.add(nodeInfos.get(index).hostname());
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
        return partitionInfo.numberOfReplicas();
    }

    @Override
    public boolean tainted() {
        return tainted;
    }

    public boolean hasPrimaryPartitionsOnNode(final String hostname) {
        return nodesWithPrimaryPartitions.contains(hostname);
    }

    public short nodeIndexForActive(int partition, boolean useFastForward) {
        if (useFastForward && !hasFastForwardMap()) {
            throw new IllegalStateException("Could not get index from FF-Map, none found in this config.");
        }

        List<Partition> partitions = useFastForward ? partitionInfo.forwardPartitions() : partitionInfo.partitions();
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

        List<Partition> partitions = useFastForward ? partitionInfo.forwardPartitions() : partitionInfo.partitions();

        try {
            return partitions.get(partition).replica(replica);
        } catch (IndexOutOfBoundsException ex) {
            // TODO: LOGGER.debug("Out of bounds on index for replica " + partition + ".", ex);
            return PARTITION_NOT_EXISTENT;
        }
    }

    public int numberOfPartitions() {
        return partitionInfo.partitions().size();
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
        return partitionInfo.hasFastForwardMap();
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
            + ", nodes=" + redactSystem(nodes())
            + ", partitionInfo=" + partitionInfo
            + ", tainted=" + tainted
            + ", version=" + version()
            + '}';
    }
}
