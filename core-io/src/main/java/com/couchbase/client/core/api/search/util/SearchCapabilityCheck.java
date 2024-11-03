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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Stability.Internal
public class SearchCapabilityCheck {
  private SearchCapabilityCheck() {
  }

  public static CompletableFuture<Void> scopedSearchIndexCapabilityCheck(Core core, Duration timeout) {
    return requireCapability(core, timeout, ClusterCapability.SEARCH_SCOPED,
        "This method cannot be used with this cluster, as it does not support scoped search indexes." +
            " Please use a cluster fully upgraded to Couchbase Server 7.6 or above.");
  }

  public static CompletableFuture<Void> vectorSearchCapabilityCheck(Core core, Duration timeout) {
    return requireCapability(core, timeout, ClusterCapability.SEARCH_VECTOR,
        "This method cannot be used with this cluster, as it does not support vector search." +
            " Please use a cluster fully upgraded to Couchbase Server 7.6 or above.");
  }

  private static CompletableFuture<Void> requireCapability(Core core, Duration timeout, ClusterCapability capability, String message) {
    return core.waitForClusterTopology(timeout)
        .doOnNext(topology -> {
          if (!topology.hasCapability(capability)) {
            throw new FeatureNotAvailableException(message);
          }
        })
        .then()
        .toFuture();
  }

}
