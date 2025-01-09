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
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import reactor.util.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.util.CbCollections.transform;

/**
 * The {@link ClusterConfig} holds bucket and global configurations in a central place.
 */
public class ClusterConfig {

  /**
   * Holds all current bucket configurations.
   */
  private final Map<String, BucketConfig> bucketConfigs;

  /**
   * Holds a global configuration if present.
   */
  private final AtomicReference<GlobalConfig> globalConfig;

  /**
   * Creates a new {@link ClusterConfig}.
   */
  public ClusterConfig() {
    bucketConfigs = new ConcurrentHashMap<>();
    globalConfig = new AtomicReference<>();
  }

  @Stability.Internal
  public Set<String> bucketNames() {
    return bucketConfigs.keySet();
  }

  @Stability.Internal
  public @Nullable ClusterTopologyWithBucket bucketTopology(final String bucketName) {
    BucketConfig bucketConfig = bucketConfigs.get(bucketName);
    return bucketConfig == null ? null : bucketConfig.asClusterTopology();
  }

  @Stability.Internal
  public Collection<ClusterTopologyWithBucket> bucketTopologies() {
    return transform(bucketConfigs.values(), BucketConfig::asClusterTopology);
  }

  @Stability.Internal
  public @Nullable ClusterTopology globalTopology() {
    GlobalConfig g = globalConfig();
    return g == null ? null : g.asClusterTopology();
  }

  public BucketConfig bucketConfig(final String bucketName) {
    return bucketConfigs.get(bucketName);
  }

  @Stability.Internal
  public void setBucketConfig(final ClusterTopologyWithBucket topology) {
    bucketConfigs.put(topology.bucket().name(), LegacyConfigHelper.toLegacyBucketConfig(topology));
  }

  @Stability.Internal
  public void deleteBucketConfig(String bucketName) {
    bucketConfigs.remove(bucketName);
  }

  public Map<String, BucketConfig> bucketConfigs() {
    return bucketConfigs;
  }

  public GlobalConfig globalConfig() {
    return globalConfig.get();
  }

  @Stability.Internal
  public void setGlobalConfig(final ClusterTopology config) {
    globalConfig.set(new GlobalConfig(config));
  }

  @Stability.Internal
  public void deleteGlobalConfig() {
    globalConfig.set(null);
  }

  public boolean hasClusterOrBucketConfig() {
    return !bucketConfigs.isEmpty() || globalConfig() != null;
  }

  /**
   * Dynamically aggregates all node addresses from global and bucket configs into a set (no duplicates).
   *
   * @return all node addresses found in global and bucket configs without duplicates.
   */
  public Set<String> allNodeAddresses() {
    Set<String> nodes = new HashSet<>();

    GlobalConfig gc = globalConfig.get();
    if (gc != null) {
      for (PortInfo ni : gc.portInfos()) {
        nodes.add(ni.hostname());
      }
    }

    for (BucketConfig bc : bucketConfigs().values()) {
      for (NodeInfo ni : bc.nodes()) {
        nodes.add(ni.hostname());
      }
    }

    return nodes;
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
      "bucketConfigs=" + bucketConfigs +
      ", globalConfig=" + globalConfig +
      '}';
  }

  /**
   * Returns the current cluster capabilities.
   *
   * <p>Right now this needs at least one bucket open, and it grabs the capabilities from the first bucket
   * config available. If needed, in the future this can be made more intelligent (caching?).</p>
   */
  public Map<ServiceType, Set<ClusterCapabilities>> clusterCapabilities() {
    for (BucketConfig bc : bucketConfigs().values()) {
      return bc.clusterCapabilities();
    }
    return Collections.emptyMap();
  }

}
