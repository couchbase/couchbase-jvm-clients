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

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.env.NetworkResolution;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.listCopyOf;
import static com.couchbase.client.core.util.CbCollections.newEnumSet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ClusterTopology {
  private final TopologyRevision revision;
  private final boolean tls;
  private final NetworkResolution network;
  private final Set<ClusterCapability> capabilities;
  private final List<HostAndServicePorts> nodes;
  private final @Nullable ClusterIdentifier clusterIdent;
  private final ObjectNode json;

  public static ClusterTopology of(
    ObjectNode json,
    TopologyRevision revision,
    @Nullable ClusterIdentifier clusterIdent,
    List<HostAndServicePorts> nodes,
    Set<ClusterCapability> capabilities,
    NetworkResolution network,
    PortSelector portSelector,
    @Nullable BucketTopology bucket
  ) {
    if (bucket != null) {
      return new ClusterTopologyWithBucket(
        json,
        revision,
        nodes,
        capabilities,
        network,
        portSelector,
        bucket,
        clusterIdent
      );
    }

    return new ClusterTopology(
      json,
      revision,
      nodes,
      capabilities,
      network,
      portSelector,
      clusterIdent
    );
  }

  protected ClusterTopology(
    ObjectNode json,
    TopologyRevision revision,
    List<HostAndServicePorts> nodes,
    Set<ClusterCapability> capabilities,
    NetworkResolution network,
    PortSelector portSelector,
    @Nullable ClusterIdentifier clusterIdent
  ) {
    if (network.equals(NetworkResolution.AUTO)) {
      throw new IllegalArgumentException("Must resolve 'auto' network before creating config.");
    }

    this.json = requireNonNull(json);
    this.revision = requireNonNull(revision);
    this.nodes = listCopyOf(nodes);
    this.capabilities = unmodifiableSet(newEnumSet(ClusterCapability.class, capabilities));
    this.network = requireNonNull(network);
    this.tls = requireNonNull(portSelector) == PortSelector.TLS;
    this.clusterIdent = clusterIdent;
  }

  public ObjectNode json() {
    return json;
  }

  public TopologyRevision revision() {
    return revision;
  }

  public List<HostAndServicePorts> nodes() {
    return nodes;
  }

  public NetworkResolution network() {
    return network;
  }

  public boolean hasCapability(ClusterCapability capability) {
    return capabilities.contains(capability);
  }

  public Set<ClusterCapability> capabilities() {
    return capabilities;
  }

  public boolean isTls() {
    return tls;
  }

  public ClusterTopologyWithBucket requireBucket() {
    if (this instanceof ClusterTopologyWithBucket) {
      return (ClusterTopologyWithBucket) this;
    }
    throw new NoSuchElementException("Bucket topology is absent.");
  }

  @SinceCouchbase("7.6.4")
  @Nullable public ClusterIdentifier id() {
    return clusterIdent;
  }

  @Override
  public String toString() {
    String bucket = this instanceof ClusterTopologyWithBucket ? this.requireBucket().bucket().toString() : "<N/A>";

    return getClass().getSimpleName() + "{" +
      "revision=" + revision +
      ", clusterIdent=" + clusterIdent +
      ", tls=" + tls +
      ", network=" + network +
      ", capabilities=" + capabilities +
      ", nodes=" + nodes +
      ", bucket=" + bucket +
      '}';
  }
}
