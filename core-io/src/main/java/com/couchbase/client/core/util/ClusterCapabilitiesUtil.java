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
package com.couchbase.client.core.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterCapabilities;
import com.couchbase.client.core.config.GlobalConfig;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.retry.reactor.Backoff;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * Defines helpful routines for working with cluster capabilities.
 */
@Stability.Internal
public class ClusterCapabilitiesUtil {
  private ClusterCapabilitiesUtil() {
  }

  private static final Duration retryDelay = Duration.ofMillis(100);

  public static Mono<Map<ServiceType, Set<ClusterCapabilities>>> waitForClusterCapabilities(final Core core,
                                                                                            final Duration timeout) {
    return Mono.fromCallable(() -> {
              // Cluster capabilities should be the same across global and bucket configs, so just use whatever is available.
              GlobalConfig globalConfig = core.clusterConfig().globalConfig();
              if (globalConfig != null) {
                return globalConfig.clusterCapabilities();
              }
              Map<String, BucketConfig> bucketConfigs = core.clusterConfig().bucketConfigs();
              if (bucketConfigs != null && !bucketConfigs.isEmpty()) {
                return bucketConfigs.values().iterator().next().clusterCapabilities();
              }
              throw new NullPointerException();
            }).retryWhen(Retry.anyOf(NullPointerException.class)
                    .timeout(timeout)
                    .backoff(Backoff.fixed(retryDelay))
                    .toReactorRetry())
            .onErrorResume(err -> {
              if (err instanceof RetryExhaustedException) {
                return Mono.error(new UnambiguousTimeoutException("Timed out while waiting for global config", null));
              } else {
                return Mono.error(err);
              }
            });
  }
}
