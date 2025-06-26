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
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.setCopyOf;
import static java.util.Collections.emptySet;

/**
 * Helps the config parser select the correct alternate addresses.
 */
@Stability.Internal
public interface NetworkSelector {

  NetworkResolution selectNetwork(List<Map<NetworkResolution, HostAndServicePorts>> nodes);

  /**
   * @param network The config parser's final output will include only addresses for the specified network.
   * Pass {@link NetworkResolution#AUTO} to apply a heuristic that selects the network
   * based on the bootstrap addresses provided by the user.
   * @param seedNodes Addresses that were used to bootstrap the connector.
   * May be empty if network is not {@link NetworkResolution#AUTO}.
   * IMPORTANT: In this context, an absent port means that port should not be used
   * for address matching. Caller is responsible for supplying default ports, if applicable.
   */
  static NetworkSelector create(NetworkResolution network, Set<SeedNode> seedNodes) {
    if (network.equals(NetworkResolution.AUTO)) {
      return new AutoNetworkSelector(seedNodes);
    }

    return new NetworkSelector() {
      @Override
      public NetworkResolution selectNetwork(List<Map<NetworkResolution, HostAndServicePorts>> nodes) {
        return network;
      }

      @Override
      public String toString() {
        return network.name();
      }
    };

  }

  class AutoNetworkSelector implements NetworkSelector {
    private static final Logger log = LoggerFactory.getLogger(AutoNetworkSelector.class);

    private final Set<SeedNode> seedNodes;
    private @Nullable NetworkResolution cachedResult; // @GuardedBy(this)
    private boolean usedFallback; // @GuardedBy(this)

    public AutoNetworkSelector(Set<SeedNode> seedNodes) {
      this.seedNodes = setCopyOf(seedNodes);
    }

    public synchronized NetworkResolution selectNetwork(List<Map<NetworkResolution, HostAndServicePorts>> nodes) {
      if (cachedResult == null) {
        cachedResult = doSelectNetwork(nodes);
      }
      return cachedResult;
    }

    @Override
    public synchronized String toString() {
      String network = cachedResult == null
        ? "<TBD>"
        : (usedFallback ? "no match -> " : "") + cachedResult.name();
      return "auto(" + network + "; seedNodes=" + seedNodes + ")";
    }

    private NetworkResolution doSelectNetwork(List<Map<NetworkResolution, HostAndServicePorts>> nodes) {
      // Search the given map for nodes whose host and KV or Manager port
      // match one of the addresses used to bootstrap the connection to the cluster.
      for (Map<NetworkResolution, HostAndServicePorts> node : nodes) {
        for (Map.Entry<NetworkResolution, HostAndServicePorts> entry : node.entrySet()) {
          for (SeedNode seedNode : seedNodes) {
            if (entry.getValue().matches(seedNode)) {
              // We bootstrapped using an address associated with this network+node,
              // so this is very likely the correct network.
              NetworkResolution exactMatch = entry.getKey();
              log.debug("Found exact match for {} in network '{}'", seedNode, exactMatch);
              return exactMatch;
            }
          }
        }
      }

      // Didn't find a match.
      NetworkResolution fallback = nodes.stream().anyMatch(it -> it.containsKey(NetworkResolution.EXTERNAL))
        ? NetworkResolution.EXTERNAL
        : NetworkResolution.DEFAULT;

      log.info("Automatic network selection was requested, but no bootstrap address exactly matches an address in the cluster topology. Falling back to network: {}", fallback);

      usedFallback = true;
      return fallback;
    }
  }

  // Visible for testing
  NetworkSelector DEFAULT = create(NetworkResolution.DEFAULT, emptySet());

  // Visible for testing
  NetworkSelector EXTERNAL = create(NetworkResolution.EXTERNAL, emptySet());

  // Visible for testing
  static NetworkSelector autoDetect(Set<SeedNode> seedNodes) {
    return create(NetworkResolution.AUTO, seedNodes);
  }
}
