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
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.config.PortInfo;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.analytics.AnalyticsPingRequest;
import com.couchbase.client.core.msg.kv.KvPingRequest;
import com.couchbase.client.core.msg.kv.KvPingResponse;
import com.couchbase.client.core.msg.query.QueryPingRequest;
import com.couchbase.client.core.msg.search.SearchPingRequest;
import com.couchbase.client.core.msg.view.ViewPingRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The {@link HealthPinger} allows to "ping" individual services with real operations for their health.
 * <p>
 * This can be used by up the stack code to assert the given state of a connected cluster and/or bucket.
 */
@Stability.Internal
public class HealthPinger {

  /**
   * Performs a service ping against all or (if given) the services provided.
   *
   * @param core     the core instance against to check.
   * @param timeout  the timeout for each individual and total ping report.
   * @param retryStrategy the retry strategy to use for each ping.
   * @param serviceTypes    if present, limits the queried services for the given types.
   * @return a mono that completes once all pings have been completed as well.
   */
  @Stability.Internal
  public static Mono<PingResult> ping(final Core core, final Optional<Duration> timeout, final RetryStrategy retryStrategy,
                                final Set<ServiceType> serviceTypes, final Optional<String> reportId, final Optional<String> bucketName) {
    return Mono.defer(() -> {
      Set<PingTarget> targets = extractPingTargets(core.clusterConfig(), bucketName);
      if (serviceTypes != null && !serviceTypes.isEmpty()) {
        targets = targets.stream().filter(t -> serviceTypes.contains(t.serviceType)).collect(Collectors.toSet());
      }

      return pingTargets(core, targets, timeout, retryStrategy).collectList().map(reports -> new PingResult(
        reports.stream().collect(Collectors.groupingBy(EndpointPingReport::type)),
        core.context().environment().userAgent().formattedShort(),
        reportId.orElse(UUID.randomUUID().toString())
      ));
    });
  }

  @Stability.Internal
  static Set<PingTarget> extractPingTargets(final ClusterConfig clusterConfig, final Optional<String> bucketName) {
    final Set<PingTarget> targets = new HashSet<>();

    if (!bucketName.isPresent()) {
      if (clusterConfig.globalConfig() != null) {
        for (PortInfo portInfo : clusterConfig.globalConfig().portInfos()) {
          for (ServiceType serviceType : portInfo.ports().keySet()) {
            if (serviceType == ServiceType.KV || serviceType == ServiceType.VIEWS) {
              // do not check bucket-level resources from a global level (null bucket name will not work)
              continue;
            }
            targets.add(new PingTarget(serviceType, portInfo.identifier(), null));
          }
        }
      }
      for (Map.Entry<String, BucketConfig> bucketConfig : clusterConfig.bucketConfigs().entrySet()) {
        for (NodeInfo nodeInfo : bucketConfig.getValue().nodes()) {
          for (ServiceType serviceType: nodeInfo.services().keySet()) {
            if (serviceType == ServiceType.KV || serviceType == ServiceType.VIEWS) {
              // do not check bucket-level resources from a global level (null bucket name will not work)
              continue;
            }
            targets.add(new PingTarget(serviceType, nodeInfo.identifier(), null));
          }
        }
      }
    } else {
      BucketConfig bucketConfig = clusterConfig.bucketConfig(bucketName.get());
      if (bucketConfig !=  null) {
        for (NodeInfo nodeInfo : bucketConfig.nodes()) {
          for (ServiceType serviceType : nodeInfo.services().keySet()) {
            if (serviceType != ServiceType.VIEWS && serviceType != ServiceType.KV) {
              targets.add(new PingTarget(serviceType, nodeInfo.identifier(), null));
            } else {
              targets.add(new PingTarget(serviceType, nodeInfo.identifier(), bucketName.get()));
            }
          }
        }
      }
    }

    return targets;
  }

  private static Flux<EndpointPingReport> pingTargets(final Core core, final Set<PingTarget> targets,
                                                      final Optional<Duration> timeout, final RetryStrategy retryStrategy) {
    return Flux.fromIterable(targets).flatMap(target -> pingTarget(core, target, timeout, retryStrategy));
  }

  private static Mono<EndpointPingReport> pingTarget(final Core core, final PingTarget target,
                                                     final Optional<Duration> timeout, final RetryStrategy retryStrategy) {
    final RetryStrategy retry = retryStrategy == null ? core.context().environment().retryStrategy() : retryStrategy;
    switch (target.serviceType) {
      case QUERY: return pingQuery(core, target, timeout, retry);
      case KV: return pingKv(core, target, timeout, retry);
      case VIEWS: return pingViews(core, target, timeout, retry);
      case SEARCH: return pingSearch(core, target, timeout, retry);
      case MANAGER: return Mono.empty(); // right now we are not pinging the cluster manager
      case ANALYTICS: return pingAnalytics(core, target, timeout, retry);
      default: return Mono.error(new IllegalStateException("Unknown service to ping, this is a bug!"));
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

  private static Mono<EndpointPingReport> pingKv(final Core core, final PingTarget target,
                                                 final Optional<Duration> userTimeout,
                                                 final RetryStrategy retryStrategy) {
    return Mono.defer(() -> {
      Duration timeout = userTimeout.orElse(core.context().environment().timeoutConfig().kvTimeout());
      CollectionIdentifier collectionIdentifier = CollectionIdentifier.fromDefault(target.bucketName);
      KvPingRequest request = new KvPingRequest(timeout, core.context(), retryStrategy, collectionIdentifier, target.nodeIdentifier);
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .map(response -> {
          request.context().logicallyComplete();
          return assembleSuccessReport(
            request.context(),
            ((KvPingResponse) response).channelId(),
            Optional.ofNullable(target.bucketName)
          );
        }).onErrorResume(throwable -> {
          request.context().logicallyComplete();
          return Mono.just(assembleFailureReport(throwable, request.context(), Optional.ofNullable(target.bucketName)));
        });
    });
  }

  private static Mono<EndpointPingReport> pingQuery(final Core core, final PingTarget target, final Optional<Duration> userTimeout,
                                      final RetryStrategy retryStrategy) {
    return Mono.defer(() -> {
      Duration timeout = userTimeout.orElse(core.context().environment().timeoutConfig().queryTimeout());
      QueryPingRequest request = new QueryPingRequest(timeout, core.context(), retryStrategy, target.nodeIdentifier);
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .map(response -> {
          request.context().logicallyComplete();
          return assembleSuccessReport(
            request.context(),
            response.channelId(),
            Optional.empty()
          );
        }).onErrorResume(throwable -> {
          request.context().logicallyComplete();
          return Mono.just(assembleFailureReport(throwable, request.context(), Optional.empty()));
        });
    });
  }

  private static Mono<EndpointPingReport> pingAnalytics(final Core core, final PingTarget target, final Optional<Duration> userTimeout,
                                          final RetryStrategy retryStrategy) {
    return Mono.defer(() -> {
      Duration timeout = userTimeout.orElse(core.context().environment().timeoutConfig().analyticsTimeout());
      AnalyticsPingRequest request = new AnalyticsPingRequest(timeout, core.context(), retryStrategy, target.nodeIdentifier);
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .map(response -> {
          request.context().logicallyComplete();
          return assembleSuccessReport(
            request.context(),
            response.channelId(),
            Optional.empty()
          );
        }).onErrorResume(throwable -> {
          request.context().logicallyComplete();
          return Mono.just(assembleFailureReport(throwable, request.context(), Optional.empty()));
        });
    });
  }

  private static Mono<EndpointPingReport> pingViews(final Core core, final PingTarget target, final Optional<Duration> userTimeout,
                                          final RetryStrategy retryStrategy) {
    return Mono.defer(() -> {
      Duration timeout = userTimeout.orElse(core.context().environment().timeoutConfig().viewTimeout());
      ViewPingRequest request = new ViewPingRequest(timeout, core.context(), retryStrategy, target.bucketName, target.nodeIdentifier);
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .map(response -> {
          request.context().logicallyComplete();
          return assembleSuccessReport(
            request.context(),
            response.channelId(),
            Optional.ofNullable(target.bucketName)
          );
        }).onErrorResume(throwable -> {
          request.context().logicallyComplete();
          return Mono.just(assembleFailureReport(throwable, request.context(), Optional.ofNullable(target.bucketName)));
        });
    });
  }

  private static Mono<EndpointPingReport> pingSearch(final Core core, final PingTarget target, final Optional<Duration> userTimeout,
                                       final RetryStrategy retryStrategy) {
    return Mono.defer(() -> {
      Duration timeout = userTimeout.orElse(core.context().environment().timeoutConfig().searchTimeout());
      SearchPingRequest request = new SearchPingRequest(timeout, core.context(), retryStrategy, target.nodeIdentifier);
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .map(response -> {
          request.context().logicallyComplete();
          return assembleSuccessReport(
            request.context(),
            response.channelId(),
            Optional.empty()
          );
        }).onErrorResume(throwable -> {
          request.context().logicallyComplete();
          return Mono.just(assembleFailureReport(throwable, request.context(), Optional.empty()));
        });
    });
  }

  @Stability.Internal
  public static class PingTarget {

    private final ServiceType serviceType;
    private final NodeIdentifier nodeIdentifier;
    private final String bucketName;

    PingTarget(final ServiceType serviceType, final NodeIdentifier nodeIdentifier, final String bucketName) {
      this.serviceType = serviceType;
      this.nodeIdentifier = nodeIdentifier;
      this.bucketName = bucketName;
    }

    public ServiceType serviceType() {
      return serviceType;
    }

    public NodeIdentifier nodeIdentifier() {
      return nodeIdentifier;
    }

    public String bucketName() {
      return bucketName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PingTarget that = (PingTarget) o;
      return serviceType == that.serviceType &&
        Objects.equals(nodeIdentifier, that.nodeIdentifier) &&
        Objects.equals(bucketName, that.bucketName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(serviceType, nodeIdentifier, bucketName);
    }

    @Override
    public String toString() {
      return "PingTarget{" +
        "serviceType=" + serviceType +
        ", nodeIdentifier=" + nodeIdentifier +
        ", bucketName='" + bucketName + '\'' +
        '}';
    }
  }

}
