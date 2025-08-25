/*
 * Copyright 2025 Couchbase, Inc.
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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbCollections.mapCopyOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.setCopyOf;
import static java.util.Objects.requireNonNull;

/**
 * For building synthetic topologies in unit tests.
 */
@Stability.Internal
public final class ClusterTopologyBuilder {
  private TopologyRevision revision = new TopologyRevision(1, 1);
  private PortSelector portSelector = PortSelector.TLS;
  private NetworkResolution networkResolution = NetworkResolution.DEFAULT;
  private String clusterName = "fake-cluster";
  private String clusterUuid = "fake-cluster-uuid";
  private String product = "server";
  private Set<ClusterCapability> capabilities = EnumSet.allOf(ClusterCapability.class);
  private final List<HostAndServicePorts> nodes = new ArrayList<>();

  public ClusterTopology build() {
    return buildWithOrWithoutBucket(null); // no bucket!
  }

  private ClusterTopology buildWithOrWithoutBucket(@Nullable BucketTopology bucket) {
    return ClusterTopology.of(
      JsonNodeFactory.instance.objectNode(),
      revision,
      new ClusterIdentifier(clusterUuid, clusterName, product),
      nodes,
      capabilities,
      networkResolution,
      portSelector,
      bucket
    );
  }

  public ClusterTopologyBuilder network(NetworkResolution networkResolution) {
    this.networkResolution = requireNonNull(networkResolution);
    return this;
  }

  public ClusterTopologyBuilder capabilities(Set<ClusterCapability> capabilities) {
    this.capabilities = setCopyOf(capabilities);
    return this;
  }

  public ClusterTopologyBuilder addNode(String hostForExternalConnections, Consumer<NodeBuilder> customizer) {
    NodeBuilder nodeBuilder = new NodeBuilder(hostForExternalConnections);
    customizer.accept(nodeBuilder);
    this.nodes.add(nodeBuilder.build());
    return this;
  }

  public CouchbaseBucketTopologyBuilder couchbaseBucket(String name) {
    return new CouchbaseBucketTopologyBuilder(name);
  }

  @Stability.Internal
  public static class NodeBuilder {
    private final String hostForNetworkConnections;
    private String canonicalHost;
    private Map<ServiceType, Integer> ports = mapOf(ServiceType.MANAGER, 8091);
    private @Nullable NodeIdentifier id;
    private @Nullable String serverGroup = "Group 1";
    private @Nullable HostAndPort ketamaAuthority;

    public NodeBuilder(String hostForNetworkConnections) {
      this.hostForNetworkConnections = requireNonNull(hostForNetworkConnections);
      this.canonicalHost = requireNonNull(hostForNetworkConnections);
    }

    public NodeBuilder canonicalHost(String canonicalHost) {
      this.canonicalHost = requireNonNull(canonicalHost);
      return this;
    }

    public NodeBuilder ketamaAuthority(@Nullable HostAndPort ketamaAuthority) {
      this.ketamaAuthority = ketamaAuthority;
      return this;
    }

    public NodeBuilder ports(Map<ServiceType, Integer> ports) {
      this.ports = mapCopyOf(ports);
      return this;
    }

    public NodeBuilder serverGroup(@Nullable String serverGroup) {
      this.serverGroup = serverGroup;
      return this;
    }

    public HostAndServicePorts build() {
      NodeIdentifier id = this.id != null ? this.id : new NodeIdentifier(canonicalHost, 8091, hostForNetworkConnections);
      return new HostAndServicePorts(hostForNetworkConnections, ports, id, ketamaAuthority, serverGroup, null, null);
    }
  }

  @Stability.Internal
  public class CouchbaseBucketTopologyBuilder {
    private final String bucketName;
    private String bucketUuid = "test-bucket-uuid";
    private int numPartitions = 1024;
    private boolean ephemeral = false;
    private int replicas = 0;
    private Set<BucketCapability> bucketCapabilities = EnumSet.allOf(BucketCapability.class);

    public CouchbaseBucketTopologyBuilder(String bucketName) {
      this.bucketName = requireNonNull(bucketName);
    }

    public ClusterTopologyWithBucket build() {
      CouchbaseBucketTopology bucket = new CouchbaseBucketTopology(
        bucketName,
        bucketUuid,
        bucketCapabilities,
        nodes,
        ephemeral,
        replicas,
        buildPartitionMap(),
        buildForwardPartitionMap()
      );

      return buildWithOrWithoutBucket(bucket).requireBucket();
    }

    private @Nullable PartitionMap buildForwardPartitionMap() {
      return null;
    }

    public CouchbaseBucketTopologyBuilder numPartitions(int numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    public CouchbaseBucketTopologyBuilder uuid(String uuid) {
      this.bucketUuid = requireNonNull(uuid);
      return this;
    }

    public CouchbaseBucketTopologyBuilder replicas(int replicas) {
      this.replicas = replicas;
      return this;
    }

    public CouchbaseBucketTopologyBuilder ephemeral(boolean ephemeral) {
      this.ephemeral = ephemeral;
      return this;
    }

    public CouchbaseBucketTopologyBuilder capabilities(Set<BucketCapability> capabilities) {
      this.bucketCapabilities = setCopyOf(capabilities);
      return this;
    }

    /**
     * Returns a dumb map with the first node hosting all actives,
     * and any remaining nodes hosting replicas (if applicable).
     */
    private PartitionMap buildPartitionMap() {
      List<PartitionTopology> partitions = new ArrayList<>();
      List<HostAndServicePorts> availableReplicas = new ArrayList<>();

      List<Integer> rawNodeIndexes = new ArrayList<>();
      rawNodeIndexes.add(0); // active
      for (int i = 1; i < nodes.size() && i <= replicas; i++) {
        rawNodeIndexes.add(i);
        availableReplicas.add(nodes.get(i));
      }

      for (int i = 0; i < numPartitions; i++) {
        partitions.add(new PartitionTopology(nodes.get(0), availableReplicas, rawNodeIndexes));
      }

      return new PartitionMap(partitions);
    }
  }
}
