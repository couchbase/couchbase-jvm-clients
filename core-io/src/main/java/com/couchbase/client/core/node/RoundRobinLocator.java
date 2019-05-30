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
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.retry.RetryOrchestrator;
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
    List<Node> filteredNodes = filterNodes(nodes);
    if (filteredNodes.isEmpty()) {
      RetryOrchestrator.maybeRetry(ctx, request);
      return;
    }

    int nodeSize = filteredNodes.size();
    int offset = (int) Math.floorMod(counter.getAndIncrement(), nodeSize);
    Node node = filteredNodes.get(offset);
    if (node != null) {
      node.send(request);
    } else {
      RetryOrchestrator.maybeRetry(ctx, request);
      ctx.environment().eventBus().publish(new NodeLocatorBugIdentifiedEvent(ctx));
    }
  }

  /**
   * Filters the list of nodes by the {@link ServiceType}.
   *
   * @param allNodes all nodes as a source.
   * @return the list of filtered nodes.
   */
  private List<Node> filterNodes(final List<Node> allNodes) {
    List<Node> result = new ArrayList<>(allNodes.size());
    for (Node n : allNodes) {
      if (n.serviceEnabled(serviceType)) {
        result.add(n);
      }
    }
    return result;
  }

}
