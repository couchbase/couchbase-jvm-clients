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
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.GlobalConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.config.PortInfo;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpRequest;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.kv.KvPingRequest;
import com.couchbase.client.core.msg.kv.KvPingResponse;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CbThrowables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;

/**
 * The {@link HealthPinger} allows to "ping" individual services with real operations for their health.
 * <p>
 * This can be used by up the stack code to assert the given state of a connected cluster and/or bucket.
 */
@Stability.Internal
public class HealthPinger {

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
      if (core.isProtostellar()) {
        return Mono.error(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar());
      }

      Set<RequestTarget> targets = extractPingTargets(core.clusterConfig(), bucketName, log);
      if (!isNullOrEmpty(serviceTypes)) {
        targets = targets.stream().filter(t -> serviceTypes.contains(t.serviceType())).collect(Collectors.toSet());
      }

      log.message("ping: narrowed targets: " + targets);

      return pingTargets(core, targets, timeout, retryStrategy, log).collectList().map(reports -> new PingResult(
        reports.stream().collect(Collectors.groupingBy(EndpointPingReport::type)),
        core.context().environment().userAgent().formattedShort(),
        reportId.orElse(UUID.randomUUID().toString())
      ));
    });
  }

  @Stability.Internal
  static Set<RequestTarget> extractPingTargets(
    final ClusterConfig clusterConfig,
    final Optional<String> bucketName,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    final Set<RequestTarget> targets = new HashSet<>();
    log.message("extractPingTargets: starting ping target extraction");

    if (!bucketName.isPresent()) {
      if (clusterConfig.globalConfig() != null) {
        GlobalConfig globalConfig = clusterConfig.globalConfig();

        log.message("extractPingTargets: getting ping targets from global config portInfos: " + globalConfig.portInfos());
        for (PortInfo portInfo : globalConfig.portInfos()) {
          for (ServiceType serviceType : portInfo.ports().keySet()) {
            if (serviceType == ServiceType.KV || serviceType == ServiceType.VIEWS) {
              // do not check bucket-level resources from a global level (null bucket name will not work)
              continue;
            }
            RequestTarget target = new RequestTarget(serviceType, portInfo.identifier(), null);
            log.message("extractPingTargets: adding target from global config: " + target);
            targets.add(target);
          }
        }
        log.message("extractPingTargets: ping targets after scanning global config: " + targets);
      } else {
        log.message("extractPingTargets: globalConfig is absent");
      }
      for (Map.Entry<String, BucketConfig> bucketConfig : clusterConfig.bucketConfigs().entrySet()) {
        log.message("extractPingTargets: getting targets from bucket config via global config for bucket " + bucketConfig.getKey() + " : " + bucketConfig.getValue());

        for (NodeInfo nodeInfo : bucketConfig.getValue().nodes()) {
          for (ServiceType serviceType : nodeInfo.services().keySet()) {
            if (serviceType == ServiceType.KV || serviceType == ServiceType.VIEWS) {
              // do not check bucket-level resources from a global level (null bucket name will not work)
              continue;
            }
            RequestTarget target = new RequestTarget(serviceType, nodeInfo.identifier(), null);
            log.message("extractPingTargets: adding target from bucket config via global config: " + target);
            targets.add(new RequestTarget(serviceType, nodeInfo.identifier(), null));
          }
        }
      }
    } else {
      BucketConfig bucketConfig = clusterConfig.bucketConfig(bucketName.get());
      if (bucketConfig !=  null) {
        log.message("extractPingTargets: Getting targets from bucket config: " + bucketConfig);
        for (NodeInfo nodeInfo : bucketConfig.nodes()) {
          for (ServiceType serviceType : nodeInfo.services().keySet()) {
            RequestTarget target;
            if (serviceType != ServiceType.VIEWS && serviceType != ServiceType.KV) {
              target = new RequestTarget(serviceType, nodeInfo.identifier(), null);
            } else {
              target = new RequestTarget(serviceType, nodeInfo.identifier(), bucketName.get());
            }

            log.message("extractPingTargets: adding target from bucket config: " + target);
            targets.add(target);
          }
        }
      } else {
        log.message("extractPingTargets: Bucket name was present, but clusterConfig has no config for bucket " + bucketName);
      }
    }

    log.message("extractPingTargets: Finished. Returning targets (grouped by node): " + targets.stream().collect(Collectors.groupingBy(it -> it.nodeIdentifier().address() + ":" + it.nodeIdentifier().managerPort())));
    return targets;
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

  private static Mono<EndpointPingReport> pingTarget(
    final Core core,
    final RequestTarget target,
    final CoreCommonOptions options,
    final WaitUntilReadyHelper.WaitUntilReadyLogger log
  ) {
    switch (target.serviceType()) {
      case QUERY:
        return pingHttpEndpoint(core, target, options, "/admin/ping", log);
      case KV:
        return pingKv(core, target, options, log);
      case VIEWS:
        return pingHttpEndpoint(core, target, options, "/", log);
      case SEARCH:
        return pingHttpEndpoint(core, target, options, "/api/ping", log);
      case MANAGER:
      case EVENTING:
      case BACKUP:
        // right now we are not pinging the cluster manager
        // right now we are not pinging the eventing service
        // right now we are not pinging the backup service
        return Mono.empty();
      case ANALYTICS:
        return pingHttpEndpoint(core, target, options, "/admin/ping", log);
      default:
        return Mono.error(new IllegalStateException("Unknown service to ping, this is a bug!"));
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
