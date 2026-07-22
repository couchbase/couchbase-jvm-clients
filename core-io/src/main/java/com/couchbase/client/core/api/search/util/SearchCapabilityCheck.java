/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.client.core.api.search.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.topology.ClusterCapability;
import com.couchbase.client.core.topology.ClusterTopology;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Stability.Internal
public class SearchCapabilityCheck {
  private SearchCapabilityCheck() {
  }

  public static CompletableFuture<Void> requireCapabilities(
      Core core,
      Duration timeout,
      Set<ClusterCapability> capabilities
  ) {
    if (capabilities.isEmpty()) return CompletableFuture.completedFuture(null);

    return core.waitForClusterTopology(timeout)
        .doOnNext(topology -> capabilities.forEach(capability -> require(topology, capability)))
        .then()
        .toFuture();
  }

  private static void require(ClusterTopology topology, ClusterCapability capability) {
    if (!topology.hasCapability(capability)) {
      throw new FeatureNotAvailableException(
          "This cluster does not support " + capability.description() + "." +
              " Please use a cluster fully upgraded to Couchbase Server " + capability.firstVersion() + " or above."
      );
    }
  }
}
