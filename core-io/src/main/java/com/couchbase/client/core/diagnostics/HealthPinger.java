/*
 * Copyright (c) 2017 Couchbase, Inc.
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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.kv.KvPingRequest;
import com.couchbase.client.core.msg.kv.KvPingResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.BucketCapability;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.HostAndServicePorts;
import com.couchbase.client.core.util.CbThrowables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.service.ServiceType.ANALYTICS;
import static com.couchbase.client.core.service.ServiceType.KV;
import static com.couchbase.client.core.service.ServiceType.QUERY;
import static com.couchbase.client.core.service.ServiceType.SEARCH;
import static com.couchbase.client.core.service.ServiceType.VIEWS;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.core.util.CbCollections.transformValues;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.groupingBy;

/**
 * The {@link HealthPinger} allows to "ping" individual services with real operations for their health.
 * <p>
 * This can be used by up the stack code to assert the given state of a connected cluster and/or bucket.
 */
@Stability.Internal
public class HealthPinger {

  /**
   * @see #pingTarget
   */
  private static final Set<ServiceType> pingableServices = unmodifiableSet(EnumSet.of(
    QUERY,
    KV,
    VIEWS,
    SEARCH,
    ANALYTICS
  ));

  private static final Set<ServiceType> servicesThatRequireBucket = unmodifiableSet(EnumSet.of(
    KV,
    VIEWS
  ));

  @Stability.Internal
  public static Mono<PingResult> ping(
    final Core core,
    final Optional<Duration> timeout,
    final RetryStrategy retryStrategy,
    final Set<ServiceType> serviceTypes,
    final Optional<String> reportId,
    final Optional<String> bucketName
  ) {
    return ping(
      core,
      timeout,
      retryStrategy,
      serviceTypes,
      reportId,
      bucketName,
      WaitUntilReadyHelper.WaitUntilReadyLogger.dummy
    );
  }

  /**
   * Performs a service ping against all or (if given) the services provided.
   *
   * @param core the core instance against to check.
   * @param timeout the timeout for each individual and total ping report.
   * @param retryStrategy the retry strategy to use for each ping.
   * @param serviceTypes if present, limits the queried services for the given types.
   * @return a mono that completes once all pings have been completed as well.
   */
  @Stability.Internal
  public static Mono<PingResult> ping(
    final Core core,
    final Optional<Duration> timeout,
    final RetryStrategy retryStrategy,
    final Set<ServiceType> serviceTypes,
    final Optional<String> reportId,
    final Optional<String> bucketName,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    return Mono.defer(() -> {
      Set<RequestTarget> targets = extractPingTargets(core.clusterConfig(), serviceTypes, bucketName, log);

      return pingTargets(core, targets, timeout, retryStrategy, log).collectList().map(reports -> new PingResult(
        reports.stream().collect(groupingBy(EndpointPingReport::type)),
        core.context().environment().userAgent().formattedShort(),
        reportId.orElse(UUID.randomUUID().toString())
      ));
    });
  }

  @Stability.Internal
  static Set<RequestTarget> extractPingTargets(
    final ClusterConfig clusterConfig,
    @Nullable final Set<ServiceType> serviceTypesOrEmpty,
    final Optional<String> bucketName,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    final String bucket = bucketName.orElse(null);
    final Set<ServiceType> serviceTypeFilter = isNullOrEmpty(serviceTypesOrEmpty)
      ? EnumSet.allOf(ServiceType.class)
      : EnumSet.copyOf(serviceTypesOrEmpty);
    serviceTypeFilter.retainAll(pingableServices); // narrow to the ones we can actually ping

    log.message("extractPingTargets: starting ping target extraction with candidate services: " + serviceTypeFilter + " and bucket: " + bucket);

    final List<ClusterTopology> topologiesToScan = new ArrayList<>();
    if (bucket != null) {
      ClusterTopologyWithBucket topology = clusterConfig.bucketTopology(bucket);
      if (topology != null && !topology.bucket().hasCapability(BucketCapability.COUCHAPI)) {
        serviceTypeFilter.remove(VIEWS);
      }
      topologiesToScan.add(topology);
    } else {
      serviceTypeFilter.removeAll(servicesThatRequireBucket); // narrow to the ones that can be pinged without a bucket
      topologiesToScan.add(clusterConfig.globalTopology());
      topologiesToScan.addAll(clusterConfig.bucketTopologies());
    }

    Set<RequestTarget> result = new HashSet<>();
    topologiesToScan.stream()
      .filter(Objects::nonNull) // global or specific bucket topology might be absent
      .forEach(topology -> {
        log.message("extractPingTargets: scanning " + describe(topology));
        for (HostAndServicePorts node : topology.nodes()) {
          for (ServiceType advertisedService : advertisedServices(node)) {
            if (serviceTypeFilter.contains(advertisedService)) {
              boolean serviceRequiresBucket = servicesThatRequireBucket.contains(advertisedService);
              String targetBucketOrNull = serviceRequiresBucket ? bucket : null;
              RequestTarget target = new RequestTarget(advertisedService, node.id(), targetBucketOrNull);
              if (result.add(target)) {
                log.message("extractPingTargets: found new target " + target);
              }
            }
          }
        }
      });

    log.message("extractPingTargets: Finished. Returning filtered targets (grouped by node): " + formatGroupedByNode(result));
    return result;
  }

  private static String describe(ClusterTopology topology) {
    String bucketOrGlobal = (topology instanceof ClusterTopologyWithBucket)
      ? "bucket '" + topology.requireBucket().bucket().name() + "'"
      : "global";
    return "topology from " + bucketOrGlobal + " ; nodes=" + topology.nodes();
  }

  private static Set<ServiceType> advertisedServices(HostAndServicePorts node) {
    return node.ports().keySet();
  }

  /**
   * Returns a map where the key is a redacted node identifier (stringified),
   * and the value is the list of service types (stringified) associated with that node.
   */
  static Map<String, List<String>> formatGroupedByNode(Collection<RequestTarget> targets) {
    Map<String, List<RequestTarget>> grouped = targets.stream()
      .collect(Collectors.groupingBy(requestTarget -> redactSystem(requestTarget.nodeIdentifier()).toString()));
    return transformValues(grouped, it -> transform(it, target -> target.serviceType().toString()));
  }

  private static Flux<EndpointPingReport> pingTargets(
    final Core core,
    final Set<RequestTarget> targets,
    final Optional<Duration> timeout,
    final RetryStrategy retryStrategy,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    final CoreCommonOptions options = CoreCommonOptions.of(timeout.orElse(null), retryStrategy, null);
    return Flux.fromIterable(targets).flatMap(target -> pingTarget(core, target, options, log));
  }

  /**
   * @see #pingableServices
   */
  static Mono<EndpointPingReport> pingTarget(
    final Core core,
    final RequestTarget target,
    final CoreCommonOptions options,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    switch (target.serviceType()) {
      case QUERY:
      case ANALYTICS:
        return pingHttpEndpoint(core, target, options, "/admin/ping", log);
      case KV:
        return pingKv(core, target, options, log);
      case VIEWS:
        return pingHttpEndpoint(core, target, options, "/", log);
      case SEARCH:
        return pingHttpEndpoint(core, target, options, "/api/ping", log);
      default:
        return Mono.error(new RuntimeException("Don't know how to ping the " + target.serviceType() + " service."));
    }
  }

  private static EndpointPingReport assembleSuccessReport(final RequestContext context, final String channelId,
                                                          final Optional<String> namespace) {
    String dispatchTo = null;
    String dispatchFrom = null;
    if (context.lastDispatchedTo() != null) {
      dispatchTo = context.lastDispatchedTo().toString();
    }
    if (context.lastDispatchedFrom() != null) {
      dispatchFrom = context.lastDispatchedFrom().toString();
    }
    return new EndpointPingReport(
      context.request().serviceType(),
      "0x" + channelId,
      dispatchFrom,
      dispatchTo,
      PingState.OK,
      namespace,
      Duration.ofNanos(context.logicalRequestLatency()),
      Optional.empty()
    );
  }

  private static EndpointPingReport assembleFailureReport(final Throwable throwable, final RequestContext context,
                                                          final Optional<String> namespace) {
    String dispatchTo = null;
    String dispatchFrom = null;
    if (context.lastDispatchedTo() != null) {
      dispatchTo = context.lastDispatchedTo().toString();
    }
    if (context.lastDispatchedFrom() != null) {
      dispatchFrom = context.lastDispatchedFrom().toString();
    }
    PingState state = throwable instanceof TimeoutException ? PingState.TIMEOUT : PingState.ERROR;
    return new EndpointPingReport(
      context.request().serviceType(),
      null,
      dispatchFrom,
      dispatchTo,
      state,
      namespace,
      state == PingState.TIMEOUT ? context.request().timeout() : Duration.ofNanos(context.logicalRequestLatency()),
      Optional.empty()
    );
  }

  private static Mono<EndpointPingReport> pingKv(
    final Core core,
    final RequestTarget target,
    final CoreCommonOptions options,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    return Mono.defer(() -> {
      Duration timeout = options.timeout().orElse(core.context().environment().timeoutConfig().kvTimeout());
      CollectionIdentifier collectionIdentifier = CollectionIdentifier.fromDefault(target.bucketName());
      KvPingRequest request = new KvPingRequest(timeout, core.context(), options.retryStrategy().orElse(null), collectionIdentifier, target.nodeIdentifier());
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .map(response -> {
          request.context().logicallyComplete();
          EndpointPingReport report = assembleSuccessReport(
            request.context(),
            ((KvPingResponse) response).channelId(),
            Optional.ofNullable(target.bucketName())
          );
          log.message("ping: Ping succeeded for " + target + " ; " + report);
          return report;
        }).onErrorResume(throwable -> {
          request.context().logicallyComplete(throwable);
          EndpointPingReport report = assembleFailureReport(throwable, request.context(), Optional.ofNullable(target.bucketName()));
          log.message("ping: Ping failed for " + target + " ; " + report + " ; " + CbThrowables.getStackTraceAsString(throwable));
          return Mono.just(report);
        });
    });
  }

  private static Mono<EndpointPingReport> pingHttpEndpoint(
    final Core core,
    final RequestTarget target,
    final CoreCommonOptions options,
    final String path,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    return Mono.defer(() -> {
      log.message("ping: Pinging " + target);

      CoreHttpRequest request = core.httpClient(target).get(path(path), options).build();
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .map(response -> {
          request.context().logicallyComplete();
          EndpointPingReport report = assembleSuccessReport(
            request.context(),
            response.channelId(),
            Optional.empty()
          );
          log.message("ping: Ping succeeded for " + target + " ; " + report);
          return report;
        }).onErrorResume(throwable -> {
          request.context().logicallyComplete(throwable);
          EndpointPingReport report = assembleFailureReport(throwable, request.context(), Optional.empty());
          log.message("ping: Ping failed for " + target + " ; " + report + " ; " + CbThrowables.getStackTraceAsString(throwable));
          return Mono.just(report);
        });
    });
  }
}
