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

package com.couchbase.client.core.classic.analytics;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.topology.ClusterIdentifier;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Stability.Internal
public class AnalyticsHelper {
  private AnalyticsHelper() {
  }

  private static volatile boolean skipClusterTypeCheck = false;

  /**
   * Call this method once when your app starts up to disable the check that
   * prevents the operational SDK from being used with an Enterprise Analytics cluster.
   * <p>
   * This is super-extra-unsupported internal API.
   */
  public static void skipClusterTypeCheck() {
    skipClusterTypeCheck = true;
  }

  public static Mono<Void> requireCouchbaseServer(Core core, Duration timeout) {
    if (skipClusterTypeCheck) {
      return Mono.empty();
    }

    return core.waitForClusterTopology(timeout)
        .mapNotNull(clusterTopology -> {
          if (isEnterpriseAnalytics(clusterTopology.id())) {
            throw new CouchbaseException(
                "This SDK is for Couchbase Server (operational) clusters, but the remote cluster is an Enterprise Analytics cluster." +
                    " Please use the Enterprise Analytics SDK to access this cluster."
            );
          }
          return null; // success! complete the empty mono.
        });
  }

  private static boolean isEnterpriseAnalytics(@Nullable ClusterIdentifier clusterId) {
    return clusterId != null && "analytics".equals(clusterId.product());
  }
}
