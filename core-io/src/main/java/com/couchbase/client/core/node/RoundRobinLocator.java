/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.node;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.events.node.NodeLocatorBugIdentifiedEvent;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.PortInfo;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.ServiceNotAvailableException;
import com.couchbase.client.core.error.context.GenericRequestErrorContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.AuthErrorDecider;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.NodeIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * A {@link Locator} which implements node selection on a round-robin basis.
 *
 * <p>This locator simply tracks a counter that gets incremented and applied to the list of
 * nodes provided. The list of nodes is filtered to make sure that only nodes with the specific
 * service are taken into account.</p>
 *
 * @since 1.0.0
 */
public class RoundRobinLocator implements Locator {

  /**
   * Holds the counter which increments for proper round-robin.
   */
  private final AtomicLong counter;

  /**
   * Holds the configured service type for this locator.
   */
  private final ServiceType serviceType;

  public RoundRobinLocator(final ServiceType serviceType) {
    this(serviceType, new Random().nextInt(1024));
  }

  RoundRobinLocator(final ServiceType serviceType, final long initialValue) {
    counter = new AtomicLong(initialValue);
    this.serviceType = serviceType;
  }

  @Override
  public void dispatch(final Request<? extends Response> request, final List<Node> nodes,
                       final ClusterConfig config, final CoreContext ctx) {
    if (!checkServiceNotAvailable(request, config)) {
      return;
    }

    boolean isTargeted = request.target() != null;

    if (!isTargeted && !config.hasClusterOrBucketConfig()) {
      boolean globalLoadInProgress =  ctx.core().configurationProvider().globalConfigLoadInProgress();
      boolean bucketLoadInProgress =ctx.core().configurationProvider().bucketConfigLoadInProgress();
      boolean loadInProgress = globalLoadInProgress || bucketLoadInProgress;

      boolean isAuthError = AuthErrorDecider.isAuthError(ctx.core().internalDiagnostics());

      RetryReason retryReason = isAuthError ? RetryReason.AUTHENTICATION_ERROR
        : bucketLoadInProgress ? RetryReason.BUCKET_OPEN_IN_PROGRESS : RetryReason.GLOBAL_CONFIG_LOAD_IN_PROGRESS;

      if (isAuthError || loadInProgress) {
        RetryOrchestrator.maybeRetry(
          ctx,
          request,
          retryReason
        );
      } else {
        request.fail(FeatureNotAvailableException.clusterLevelQuery(serviceType));
      }
      return;
    }

    List<Node> filteredNodes = filterNodes(nodes, request, config);
    if (filteredNodes.isEmpty()) {
      if (serviceShowsUpInConfig(config) || !config.hasClusterOrBucketConfig()) {
        // No node has the service enabled, but it shows up in the config (or there is no config at all yet). This is
        // a race condition, likely the node/service is currently in progress of being configured, so let's send it into
        // retry and wait until it can be dispatched.
        RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      } else {
        // No node in the cluster has the service enabled, and it does not show up in the bucket config,
        // so we need to cancel the request with a service not available exception. Either the cluster is
        // not configured with the service in the first place or a failover happened and suddenly there
        // is no node in the cluster anymore which can serve the request. In any case sending it into
        // retry will not help resolve the situation so let's make it clear in the exception what's
        //going on.
        request.fail(new ServiceNotAvailableException(
          "The " + request.serviceType().id()
            + " service is not available in the cluster.",
          new GenericRequestErrorContext(request)
        ));
      }
      return;
    }

    if (isTargeted) {
      dispatchTargeted(request, filteredNodes, ctx);
    } else {
      dispatchUntargeted(request, filteredNodes, ctx);
    }
  }

  /**
   * Helper method to check if a given services shows up in a config on the cluster.
   *
   * @param clusterConfig the config to check against.
   * @return true if it shows up at least once, false otherwise.
   */
  private boolean serviceShowsUpInConfig(final ClusterConfig clusterConfig) {
    if (clusterConfig.globalConfig() != null) {
      for (PortInfo portInfo : clusterConfig.globalConfig().portInfos()) {
        if (portInfo.ports().containsKey(serviceType) || portInfo.sslPorts().containsKey(serviceType)) {
          return true;
        }
      }
    }

    return clusterConfig.bucketConfigs().values().stream()
      .anyMatch(it -> it.serviceEnabled(serviceType));
  }

  /**
   * Can be overridden to check if a request should be cancelled immediately that the service is not
   * supported.
   * <p>
   * If this method returns false, something MUST be done with the request, or it will time out!
   */
  protected boolean checkServiceNotAvailable(final Request<? extends Response> request, final ClusterConfig config) {
    return true;
  }

  private void dispatchTargeted(final Request<? extends Response> request, final List<Node> nodes,
                                final CoreContext ctx) {
    for (Node n : nodes) {
      if (n.identifier().equals(request.target())) {
        n.send(request);
        return;
      }
    }

    handleTargetNotAvailable(request, nodes, ctx);
  }

  /**
   * When a targeted request cannot be dispatched, apply some more logic to figure out what to do with it.
   * <p>
   * In the specific case there are two situations that can happen: either the target is there, it's just not ready
   * to serve requests (yet) or it is not even part of the node list anymore. In the latter case there is no way
   * the request is going to make progress, so cancel it and give the caller a chance to fetch a new target and
   * send a new request.
   *
   * @param request the request to check.
   * @param nodes the nodes list to check against.
   * @param ctx the core context.
   */
  private static void handleTargetNotAvailable(final Request<?> request, final List<Node> nodes,
                                               final CoreContext ctx) {
    NodeIdentifier target = requireNonNull(request.target());
    for (Node node : nodes) {
      if (target.equals(node.identifier())) {
        RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
        return;
      }
    }

    request.cancel(CancellationReason.TARGET_NODE_REMOVED);
  }

  private void dispatchUntargeted(final Request<? extends Response> request, final List<Node> nodes,
                                  final CoreContext ctx) {
    int nodeSize = nodes.size();
    int offset = (int) Math.floorMod(counter.getAndIncrement(), (long) nodeSize);
    Node node = nodes.get(offset);
    if (node != null) {
      node.send(request);
    } else {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      ctx.environment().eventBus().publish(new NodeLocatorBugIdentifiedEvent(ctx));
    }
  }

  /**
   * Filters the list of nodes by the {@link ServiceType}.
   *
   * @param allNodes all nodes as a source.
   * @param request the request in scope.
   * @param config the cluster-level config.
   * @return the list of filtered nodes.
   */
  private List<Node> filterNodes(final List<Node> allNodes, final Request<? extends Response> request,
                                 final ClusterConfig config) {
    List<Node> result = new ArrayList<>(allNodes.size());
    for (Node n : allNodes) {
      if (n.serviceEnabled(serviceType) && nodeCanBeUsed(n, request, config)) {
        result.add(n);
      }
    }
    return result;
  }

  /**
   * This method can be overridden for additional per-node checks in addition to the service-type
   * based check already performed in {@link #filterNodes(List, Request, ClusterConfig)}.
   *
   * <p>This method will be called for each node in the list to find out if it can be used in principle
   * for dispatching the request.</p>
   *
   * @param node the node to check against.
   * @param request the request in scope.
   * @param config the cluster-level config.
   * @return true if it can be used.
   */
  protected boolean nodeCanBeUsed(final Node node, final Request<? extends Response> request,
                                  final ClusterConfig config) {
    return true;
  }

}
