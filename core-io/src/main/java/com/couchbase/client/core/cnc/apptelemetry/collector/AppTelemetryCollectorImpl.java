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

package com.couchbase.client.core.cnc.apptelemetry.collector;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.env.UserAgent;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.HostAndServicePorts;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryRequestClassifier.classify;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;

@Stability.Internal
public final class AppTelemetryCollectorImpl implements AppTelemetryCollector {
  private static final Logger log = LoggerFactory.getLogger(AppTelemetryCollectorImpl.class);

  private final String agent;
  private final ConcurrentMap<NodeAndBucket, AppTelemetryMetricSet> metricSets = new ConcurrentHashMap<>();
  private volatile boolean paused;

  private volatile @Nullable ClusterTopology latestTopology;

  public AppTelemetryCollectorImpl(
    Flux<ClusterConfig> configs,
    UserAgent userAgent
  ) {
    this.agent = userAgent.name() + "/" + userAgent.version();

    configs.doOnNext(config -> {
        try {
          ClusterTopology topology = config.globalTopology();
          this.latestTopology = topology;
          prune(topology == null ? emptyList() : topology.nodes());
        } catch (Exception e) {
          log.warn("PRUNE: Failed to prune App Telemetry metrics", e);
        }
      })
      .subscribe(); // flux terminates when core shuts down.
  }

  private void prune(List<HostAndServicePorts> existingNodes) {
    Set<HostAndPort> canonicalAddressesOfExistingNodes = existingNodes.stream()
      .map(it -> it.id().canonical())
      .collect(toSet());

    log.debug("PRUNE: Canonical addresses of nodes in cluster: {}", redactSystem(canonicalAddressesOfExistingNodes));

    metricSets.keySet().removeIf(it -> {
      boolean remove = !canonicalAddressesOfExistingNodes.contains(it.nodeId.canonical());
      if (remove) {
        log.info("PRUNE: Discarding App Telemetry metrics for node that is no longer in cluster: {}", redactSystem(it.nodeId));
      }
      return remove;
    });
  }

  @Override
  public synchronized void setPaused(boolean paused) {
    if (this.paused == paused) {
      return;
    }

    log.info("{} app telemetry collection.",
      paused ? "Pausing" : "Resuming"
    );

    this.paused = paused;

    if (paused) {
      prune(emptyList());
    }
  }

  @Override
  public void recordLatency(Request<?> request) {
    if (paused) {
      return;
    }

    AppTelemetryRequestType type = classify(request);
    if (type == null) return;

    RequestContext ctx = request.context();
    recordLatency(
      ctx.lastDispatchedToNode(),
      request.bucket(),
      type,
      ctx.dispatchLatency()
    );
  }

  private void recordLatency(
    NodeIdentifier nodeId,
    @Nullable String bucket,
    AppTelemetryRequestType type,
    long latencyNanos
  ) {
    NodeAndBucket nodeAndBucket = new NodeAndBucket(nodeId, bucket);
    AppTelemetryMetricSet histogramGroup = metricSets.computeIfAbsent(nodeAndBucket, key -> new AppTelemetryMetricSet());
    AppTelemetryHistogram histogram = histogramGroup.histograms.get(type);
    histogram.record(latencyNanos);

    increment(nodeId, bucket, type.service, AppTelemetryCounterType.TOTAL);
  }

  @Override
  public void increment(
    Request<?> request,
    AppTelemetryCounterType counterType
  ) {
    if (paused) {
      return;
    }

    AppTelemetryRequestType type = classify(request);
    if (type == null) return;

    RequestContext ctx = request.context();
    NodeIdentifier nodeId = ctx.lastDispatchedToNode();
    if (nodeId == null) return;

    increment(
      nodeId,
      request.bucket(),
      request.serviceType(),
      counterType
    );
  }

  private void increment(
    NodeIdentifier nodeId,
    @Nullable String bucket,
    ServiceType serviceType,
    AppTelemetryCounterType counterType
  ) {
    NodeAndBucket nodeAndBucket = new NodeAndBucket(nodeId, bucket);
    AppTelemetryMetricSet metricSet = metricSets.computeIfAbsent(nodeAndBucket, key -> new AppTelemetryMetricSet());

    Map<AppTelemetryCounterType, AppTelemetryCounter> countersByType = metricSet.counters.get(serviceType);
    if (countersByType == null) return; // not tracking this service

    countersByType.get(counterType).increment();

    if (counterType != AppTelemetryCounterType.TOTAL) {
      countersByType.get(AppTelemetryCounterType.TOTAL).increment();
    }
  }

  /**
   * @implNote synchronized in case somehow multiple reporter connections are active concurrently.
   */
  @Override
  public synchronized void reportTo(Consumer<? super CharSequence> charSink) {
    long currentTimeMillis = System.currentTimeMillis();

    metricSets.forEach((nodeAndBucket, metricSet) -> {
      String nodeUuid = getNodeUuid(nodeAndBucket);
      if (nodeUuid == null) {
        // node no longer in cluster; can't report it, since we don't know its UUID.
        return;
      }

      Map<String, String> commonTags = new LinkedHashMap<>();
      commonTags.put("agent", agent);
      commonTags.put("node_uuid", nodeUuid);
      nodeAndBucket.writeTo(commonTags);
      metricSet.forEachMetric(reportable -> reportable.reportTo(charSink, commonTags, currentTimeMillis));
    });
  }

  /**
   * Returns the given node's `nodeUuid` (possibly empty string if server is older than 8.0)
   * or null if no matching node is present in the current cluster topology.
   */
  private @Nullable String getNodeUuid(NodeAndBucket nodeAndBucket) {
    ClusterTopology topology = latestTopology;
    if (topology == null) return null; // Shutting down, or not yet bootstrapped.

    return topology.nodes().stream()
      .filter(node -> node.id().equals(nodeAndBucket.nodeId))
      .findFirst()
      .map(node -> nullToEmpty(node.uuid()))
      .orElse(null);
  }
}
