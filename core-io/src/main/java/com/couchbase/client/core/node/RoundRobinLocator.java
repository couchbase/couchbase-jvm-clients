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
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.retry.RetryOrchestrator;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.service.ServiceType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

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
    boolean isTargeted = request instanceof TargetedRequest;

    if (!isTargeted && !config.hasClusterOrBucketConfig()) {
      boolean globalLoadInProgress =  ctx.core().configurationProvider().globalConfigLoadInProgress();
      boolean bucketLoadInProgress =ctx.core().configurationProvider().bucketConfigLoadInProgress();
      boolean loadInProgress = globalLoadInProgress || bucketLoadInProgress;

      if (loadInProgress) {
        RetryOrchestrator.maybeRetry(
          ctx,
          request,
          bucketLoadInProgress ? RetryReason.BUCKET_OPEN_IN_PROGRESS : RetryReason.GLOBAL_CONFIG_LOAD_IN_PROGRESS
        );
      } else {
        request.fail(FeatureNotAvailableException.clusterLevelQuery(serviceType));
      }
      return;
    }

    List<Node> filteredNodes = filterNodes(nodes, request, config);
    if (filteredNodes.isEmpty()) {
      RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
      return;
    }

    if (isTargeted) {
      dispatchTargeted(request, filteredNodes, ctx);
    } else {
      dispatchUntargeted(request, filteredNodes, ctx);
    }
  }

  private void dispatchTargeted(final Request<? extends Response> request, final List<Node> nodes,
                                final CoreContext ctx) {
    for (Node n : nodes) {
      if (n.identifier().equals(((TargetedRequest) request).target())) {
        n.send(request);
        return;
      }
    }

    RetryOrchestrator.maybeRetry(ctx, request, RetryReason.NODE_NOT_AVAILABLE);
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
