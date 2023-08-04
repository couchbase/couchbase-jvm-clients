/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.config.BucketConfigRefreshFailedEvent;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ConfigRefreshFailure;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigRequest;
import com.couchbase.client.core.msg.kv.ConfigRequest;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.NanoTimestamp;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link KeyValueBucketRefresher} keeps configs up-to-date through the KV service.
 *
 * <p>The KV refresher works by proactively polling for new configurations against all open, registered
 * buckets. It tries to iterate through all available KV nodes so that even if one or more are not available
 * we'll eventually are able to get a proper, good config to work with.</p>
 *
 * <p>Once a config is retrieved it is sent to the config manager which then decides if it is going to apply
 * or discard the config.</p>
 *
 * @since 1.0.0
 */
@Stability.Internal
public class KeyValueBucketRefresher implements BucketRefresher {

  /**
   * The interval at which the poller fires.
   *
   * <p>Note that this does not mean that a new config is request at this interval, since
   * those are checked individually based on the configuration applied (but it specifies the
   * minimum interval possible).</p>
   */
  static final Duration POLLER_INTERVAL = Duration.ofSeconds(1);

  /**
   * The number of config fetches in parallel at maximum per interval.
   */
  static final int MAX_PARALLEL_FETCH = 1;

  /**
   * Holds the core as a reference.
   */
  private final Core core;

  /**
   * The registration which is used to track the interval polls and needs to be disposed
   * on shutdown.
   */
  private final Disposable pollRegistration;

  /**
   * Holds the config provider as a reference.
   */
  private final ConfigurationProvider provider;

  /**
   * The global offset is used to make sure each KV node eventually gets used
   * by shifting on each invocation.
   */
  private final AtomicLong nodeOffset = new AtomicLong(0);

  /**
   * Holds all current registrations and their last timestamp when the bucket has been
   * updated.
   */
  private final Map<String, NanoTimestamp> registrations = new ConcurrentHashMap<>();

  /**
   * Holds the number of failed refreshes per bucket.
   */
  private final Map<String, AtomicInteger> numFailedRefreshes = new ConcurrentHashMap<>();

  /**
   * Holds all tainted bucket configs (those undergoing rebalance at the moment).
   */
  private final Set<String> tainted = ConcurrentHashMap.newKeySet();

  /**
   * Holds the allowable config poll interval.
   */
  private final Duration configPollInterval;

  /**
   * Stores the timeout used for config refresh requests, keeping it in reasonable bounds (between 1 and 5s).
   */
  private final Duration configRequestTimeout;

  /**
   * Holds the event bus to send events to.
   */
  private final EventBus eventBus;


  public KeyValueBucketRefresher(final ConfigurationProvider provider, final Core core) {
    this.core = core;
    this.eventBus = core.context().environment().eventBus();
    this.provider = provider;
    this.configPollInterval = core.context().environment().ioConfig().configPollInterval();
    this.configRequestTimeout = clampConfigRequestTimeout(configPollInterval);

    pollRegistration = Flux
      .interval(pollerInterval(), core.context().environment().scheduler())
      // Proposing a new config should be quick, but just in case it gets held up we do
      // not want to terminate the refresher and just drop the ticks and keep going.
      .onBackpressureDrop()
      .filter(v -> !registrations.isEmpty())
      .flatMap(ignored -> Flux
        .fromIterable(registrations.keySet())
        .flatMap(KeyValueBucketRefresher.this::maybeUpdateBucket)
      )
      .subscribe(provider::proposeBucketConfig);
  }

  /**
   * Allows to override the default poller interval in tests to speed them up.
   *
   * @return the poller interval as a duration.
   */
  protected Duration pollerInterval() {
    return POLLER_INTERVAL;
  }

  /**
   * Helper method to make sure the config poll interval is set to a fixed bound every time.
   *
   * <p>The config poll interval is used, but the lower limit is set to 1 second and the upper limit
   * to 5 seconds to make sure it stays in "sane" bounds.</p>
   *
   * @param configPollInterval the config poll interval is used to determine the bounds.
   * @return the duration for the config request timeout.
   */
  static Duration clampConfigRequestTimeout(final Duration configPollInterval) {
    return constrainToRange(configPollInterval, Duration.ofSeconds(1), Duration.ofSeconds(5));
  }

  private static Duration constrainToRange(Duration d, Duration min, Duration max) {
    if (min.compareTo(max) > 0) {
      throw new IllegalArgumentException("min duration " + min + " must be <= max duration " + max);
    }
    if (d.compareTo(min) < 0) {
      return min;
    }
    if (d.compareTo(max) > 0) {
      return max;
    }
    return d;
  }

  /**
   * If needed, fetches a configuration (and otherwise ignores this attempt).
   *
   * <p>This method makes sure that individual errors are logged but swallowed so they do
   * not interrupt anything else in progress.</p>
   *
   * @param name the name of the bucket.
   * @return a {@link Mono} either with a new config or nothing to ignore.
   */
  private Mono<ProposedBucketConfigContext> maybeUpdateBucket(final String name) {
    NanoTimestamp last = registrations.get(name);
    boolean overInterval = last != null && last.hasElapsed(configPollInterval);
    boolean allowed = tainted.contains(name) || overInterval;

    if (allowed) {
      List<NodeInfo> nodes = filterEligibleNodes(name);
      if (numFailedRefreshes.get(name).get() >= nodes.size()) {
        provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
        numFailedRefreshes.get(name).set(0);
      }
      return fetchConfigPerNode(name, Flux.fromIterable(nodes).take(MAX_PARALLEL_FETCH))
        .next()
        .doOnSuccess(ctx -> registrations.replace(name, NanoTimestamp.now()));
    } else {
      return Mono.empty();
    }
  }

  /**
   * Helper method to generate a stream of KV nodes that can be used to fetch a config.
   *
   * <p>To be more resilient to failures, this method (similar to 1.x) shifts the node
   * list on each invocation by one to make sure we hit all of them eventually.</p>
   *
   * @param name the bucket name.
   * @return a flux of nodes that have the KV service enabled.
   */
  private List<NodeInfo> filterEligibleNodes(final String name) {
    BucketConfig config = provider.config().bucketConfig(name);
    if (config == null) {
      eventBus.publish(new BucketConfigRefreshFailedEvent(
        core.context(),
        BucketConfigRefreshFailedEvent.RefresherType.KV,
        BucketConfigRefreshFailedEvent.Reason.NO_BUCKET_FOUND,
        Optional.empty()
      ));
      return Collections.emptyList();
    }

    List<NodeInfo> nodes = new ArrayList<>(config.nodes());
    shiftNodeList(nodes);

    return nodes
      .stream()
      .filter(n -> n.services().containsKey(ServiceType.KV) || n.sslServices().containsKey(ServiceType.KV))
      .collect(Collectors.toList());
  }

  /**
   * Helper method to fetch a config per node provided.
   *
   * <p>Note that the bucket config request sent here has a fail fast strategy, so that if nodes are offline they
   * do not circle the system forever (given they have a specific node target). Since the refresher polls every
   * fixed interval anyways, fresh requests will flood the system eventually and there is no point in keeping
   * the old ones around.</p>
   *
   * <p>Also, the timeout is set to the poll interval since it does not make sense to keep them around any
   * longer.</p>
   *
   * @param name the bucket name.
   * @param nodes the flux of nodes that can be used to fetch a config.
   * @return returns configs for each node if found.
   */
  private Flux<ProposedBucketConfigContext> fetchConfigPerNode(final String name, final Flux<NodeInfo> nodes) {
    return nodes.flatMap(nodeInfo -> {
      CoreContext ctx = core.context();
      CarrierBucketConfigRequest request = new CarrierBucketConfigRequest(
        configRequestTimeout,
        ctx,
        new CollectionIdentifier(name, Optional.empty(), Optional.empty()),
        FailFastRetryStrategy.INSTANCE,
        nodeInfo.identifier(),
        ConfigRequest.Purpose.REFRESH
      );
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .filter(response -> {
          if (!response.status().success()) {
            numFailedRefreshes.get(name).incrementAndGet();
            eventBus.publish(new BucketConfigRefreshFailedEvent(
              core.context(),
              BucketConfigRefreshFailedEvent.RefresherType.KV,
              BucketConfigRefreshFailedEvent.Reason.INDIVIDUAL_REQUEST_FAILED,
              Optional.of(response)
            ));
          }
          return response.status().success();
        })
        .filter(response -> {
          // Ignore responses from channels where clustermap change notifications are enabled.
          return response.content().length > 0;
        })
        .map(response ->
          new ProposedBucketConfigContext(name, new String(response.content(), UTF_8), nodeInfo.hostname())
        )
        .doOnSuccess(r -> numFailedRefreshes.get(name).set(0))
        .onErrorResume(t -> {
          numFailedRefreshes.get(name).incrementAndGet();
          eventBus.publish(new BucketConfigRefreshFailedEvent(
            core.context(),
            BucketConfigRefreshFailedEvent.RefresherType.KV,
            BucketConfigRefreshFailedEvent.Reason.INDIVIDUAL_REQUEST_FAILED,
            Optional.of(t)
          ));
          return Mono.empty();
        });
    });
  }

  /**
   * Helper method to transparently rearrange the node list based on the current global offset.
   *
   * @param nodeList the list to shift.
   */
  private <T> void shiftNodeList(final List<T> nodeList) {
    int shiftBy = (int) (nodeOffset.getAndIncrement() % nodeList.size());
    Collections.rotate(nodeList, -shiftBy);
  }

  @Override
  public Mono<Void> register(final String name) {
    return Mono.defer(() -> {
      registrations.put(name, NanoTimestamp.never());
      numFailedRefreshes.put(name, new AtomicInteger(0));
      return Mono.empty();
    });
  }

  @Override
  public Mono<Void> deregister(final String name) {
    return Mono.defer(() -> {
      registrations.remove(name);
      numFailedRefreshes.remove(name);
      return Mono.empty();
    });
  }

  @Override
  public void markTainted(final String name) {
    tainted.add(name);
  }

  @Override
  public void markUntainted(final String name) {
    tainted.remove(name);
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(() -> {
      if (!pollRegistration.isDisposed()) {
        pollRegistration.dispose();
      }
      numFailedRefreshes.clear();
      registrations.clear();
      tainted.clear();
      return Mono.empty();
    });
  }

  @Override
  public Set<String> registered() {
    return registrations.keySet();
  }
}
