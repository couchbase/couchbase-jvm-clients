/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterIdentifier;
import com.couchbase.client.core.topology.ClusterTopology;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@link GlobalConfig} represents a configuration which is not tied to a bucket.
 *
 * <p>This type of config has been introduced in couchbase server 6.5 (and forward) and allows cluster-level
 * operations without having a bucket open (and as a result fetch a bucket config). It only contains a subset
 * of what can be found in a bucket config, since it contains only what necessary to locate cluster-level
 * features and capabilities.</p>
 *
 * @deprecated In favor of {@link com.couchbase.client.core.topology.ClusterTopology}
 */
@Deprecated
@JsonIgnoreProperties(ignoreUnknown = true)
public class GlobalConfig {

  private final ConfigVersion version;
  private final List<PortInfo> portInfos;
  private final Map<ServiceType, Set<ClusterCapabilities>> clusterCapabilities;
  private final @Nullable ClusterIdentifier clusterIdent;

  // Null only if the GlobalConfig was created by a legacy config parser
  @Nullable private final ClusterTopology clusterTopology;

  public GlobalConfig(ClusterTopology topology) {
    this.version = LegacyConfigHelper.toLegacy(topology.revision());
    this.portInfos = LegacyConfigHelper.getPortInfos(topology);
    this.clusterCapabilities = LegacyConfigHelper.getClusterCapabilities(topology);
    this.clusterTopology = topology;
    this.clusterIdent = topology.id();
  }

  @JsonCreator
  public GlobalConfig(
    @JsonProperty("rev") long rev,
    @JsonProperty("revEpoch") long revEpoch,
    @JsonProperty("nodesExt") List<PortInfo> portInfos,
    @JsonProperty("clusterCapabilities") Map<String, Set<ClusterCapabilities>> clusterCapabilities,
    @JacksonInject("origin") String origin
  ) {
    this.version = new ConfigVersion(revEpoch, rev);
    this.portInfos = enrichPortInfos(portInfos, origin);
    this.clusterCapabilities = AbstractBucketConfig.convertClusterCapabilities(clusterCapabilities);
    this.clusterTopology = null;
    this.clusterIdent = null;
  }

  /**
   * Helper method to enrich the port infos with a synthetic origin host if not present.
   *
   * <p>In a single node cluster or if the node does not include the hostname, this method enriches the port config
   * with the hostname it got the config from. This will make sure we can still bootstrap and assemble it as a valid
   * configuration.</p>
   *
   * @param portInfos the original port infos.
   * @param origin the origin hostname.
   * @return the modified port infos to store and use.
   */
  private List<PortInfo> enrichPortInfos(final List<PortInfo> portInfos, final String origin) {
    List<PortInfo> enriched = new ArrayList<>(portInfos.size());
    for (PortInfo portInfo : portInfos) {
      if (portInfo.hostname() == null) {
        enriched.add(new PortInfo(portInfo.ports(), portInfo.sslPorts(), portInfo.alternateAddresses(), origin, portInfo.serverGroup()));
      } else {
        enriched.add(portInfo);
      }
    }
    return enriched;
  }

  /**
   * The revision id of this global config.
   *
   * @deprecated Please use {@link #version()} instead.
   */
  @Deprecated
  public long rev() {
    return version().rev();
  }

  /**
   * The epoch of the revision, 0 if not set on the config.
   *
   * @deprecated Please use {@link #version()} instead.
   */
  @Deprecated
  public long revEpoch() {
    return version.epoch();
  }

  public ConfigVersion version() {
    return version;
  }

  /**
   * All global cluster capabilities.
   */
  public Map<ServiceType, Set<ClusterCapabilities>> clusterCapabilities() {
    return clusterCapabilities;
  }

  @Nullable public ClusterIdentifier clusterIdent() {
    return clusterIdent;
  }

  /**
   * The node/port infos for each node in the list.
   */
  public List<PortInfo> portInfos() {
    return portInfos;
  }

  /**
   * @throws IllegalStateException if this GlobalConfig was not created from a ClusterTopology.
   */
  @Stability.Internal
  ClusterTopology asClusterTopology() {
    if (clusterTopology == null) {
      throw new IllegalStateException("This GlobalConfig instance was not created from a ClusterTopology.");
    }
    return clusterTopology;
  }

  @Override
  public String toString() {
    return "GlobalConfig{" +
      "version=" + version +
      ", portInfos=" + portInfos +
      ", clusterCapabilities=" + clusterCapabilities +
      '}';
  }
}
