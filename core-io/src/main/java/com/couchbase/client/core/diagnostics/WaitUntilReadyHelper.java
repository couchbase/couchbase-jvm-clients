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

package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Helper class to perform the "wait until ready" logic.
 */
@Stability.Internal
public class WaitUntilReadyHelper {

  @Stability.Internal
  public static CompletableFuture<Void> waitUntilReady(final Core core, final Set<ServiceType> serviceTypes,
                                                       final Duration timeout, final ClusterState desiredState,
                                                       final Optional<String> bucketName) {
    boolean hasChance = core.clusterConfig().hasClusterOrBucketConfig()
      || core.configurationProvider().globalConfigLoadInProgress()
      || core.configurationProvider().bucketConfigLoadInProgress();
    if (!hasChance) {
      CompletableFuture<Void> f = new CompletableFuture<>();
      f.completeExceptionally(
        new IllegalStateException("Against pre 6.5 clusters at least a bucket needs to be opened!")
      );
      return f;
    }

    return Flux
      .interval(Duration.ofMillis(10), core.context().environment().scheduler())
      .filter(i -> !(core.configurationProvider().bucketConfigLoadInProgress()
        || core.configurationProvider().globalConfigLoadInProgress()
        || (bucketName.isPresent() && core.configurationProvider().collectionMapRefreshInProgress())
        || (bucketName.isPresent() && core.clusterConfig().bucketConfig(bucketName.get()) == null))
      )
      .take(1)
      .flatMap(aLong -> {
        final Set<ServiceType> servicesToCheck = serviceTypes != null && !serviceTypes.isEmpty()
          ? serviceTypes
          : HealthPinger
          .extractPingTargets(core.clusterConfig(), bucketName)
          .stream()
          .map(HealthPinger.PingTarget::serviceType)
          .collect(Collectors.toSet());

        final Flux<ClusterState> diagnostics = Flux
          .interval(Duration.ofMillis(10), core.context().environment().scheduler())
          .map(i -> diagnosticsCurrentState(core))
          .takeUntil(s -> s == desiredState);

        return Flux.concat(ping(core, servicesToCheck, timeout), diagnostics);
      })
      .then()
      .timeout(
        timeout,
        Mono.defer(() -> Mono.error(new UnambiguousTimeoutException("WaitUntilReady timed out", null))),
        core.context().environment().scheduler()
      )
      .toFuture();
  }

  /**
   * Checks diagnostics and returns the current aggregated cluster state.
   */
  private static ClusterState diagnosticsCurrentState(final Core core) {
    return DiagnosticsResult.aggregateClusterState(core
      .diagnostics()
      .collect(Collectors.groupingBy(EndpointDiagnostics::type))
      .values());
  }

  /**
   * Performs the ping part of the wait until ready (calling into the health pinger).
   */
  private static Flux<PingResult> ping(final Core core, final Set<ServiceType> serviceTypes, final Duration timeout) {
    return HealthPinger
      .ping(core, Optional.of(timeout), core.context().environment().retryStrategy(), serviceTypes, Optional.empty(), Optional.empty())
      .flux();
  }

}
