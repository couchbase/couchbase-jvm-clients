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

package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.cnc.events.config.IndividualGlobalConfigRefreshFailedEvent;
import com.couchbase.client.core.config.ConfigRefreshFailure;
import com.couchbase.client.core.config.ConfigVersion;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.GlobalConfig;
import com.couchbase.client.core.config.PortInfo;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.msg.kv.CarrierGlobalConfigRequest;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.couchbase.client.core.config.DefaultConfigurationProvider.TRIGGERED_BY_CONFIG_CHANGE_NOTIFICATION;
import static com.couchbase.client.core.config.refresher.KeyValueBucketRefresher.MAX_PARALLEL_FETCH;
import static com.couchbase.client.core.config.refresher.KeyValueBucketRefresher.POLLER_INTERVAL;
import static com.couchbase.client.core.config.refresher.KeyValueBucketRefresher.clampConfigRequestTimeout;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link GlobalRefresher} keeps the cluster-level global config up-to-date.
 * <p>
 * It works very similar to the {@link KeyValueBucketRefresher}, but explicitly has no bucket
 * level scope. It can be started and stopped, since there might be situations where global config polling is not
 * needed.
 */
public class GlobalRefresher {

  /**
   * Holds the parent configuration provider.
   */
  private final ConfigurationProvider provider;

  /**
   * Holds the parent core.
   */
  private final Core core;

  /**
   * Holds the allowable config poll interval.
   */
  private final Duration configPollInterval;

  /**
   * Stores the timeout used for config refresh requests, keeping it in reasonable bounds (between 1 and 5s).
   */
  private final Duration configRequestTimeout;

  /**
   * The registration which is used to track the interval polls and needs to be disposed
   * on shutdown.
   */
  private final Disposable pollRegistration;

  /**
   * The global offset is used to make sure each KV node eventually gets used
   * by shifting on each invocation.
   */
  private final AtomicLong nodeOffset = new AtomicLong(0);

  /**
   * Indicates if the refresher is started/stopped.
   */
  private volatile boolean started = false;

  /**
   * Stores the last poll attempt to calculate if enough time has passed for a new poll.
   */
  private volatile NanoTimestamp lastPoll = NanoTimestamp.never();

  /**
   * Holds the number of consecutively failed refreshes to trigger side effects if needed.
   */
  private final AtomicInteger numFailedRefreshes = new AtomicInteger(0);

  /**
   * Creates a new global refresher.
   *
   * @param provider the config provider which should get the config updates proposed.
   * @param core the core to send the config commands to.
   */
  public GlobalRefresher(final ConfigurationProvider provider, final Core core) {
    this.provider = provider;
    this.core = core;
    configPollInterval = core.context().environment().ioConfig().configPollInterval();
    configRequestTimeout = clampConfigRequestTimeout(configPollInterval);

    pollRegistration = Flux.merge(
        Flux.interval(pollerInterval(), core.context().environment().scheduler()),
        provider.configChangeNotifications()
      )
      // Proposing a new config should be quick, but just in case it gets held up we do
      // not want to terminate the refresher and just drop the ticks and keep going.
      .onBackpressureDrop()
      // If the refresher is not started yet, drop all intervals
      .filter(v -> started)
      // Since the POLLER_INTERVAL is smaller than the config poll interval, make sure
      // we only emit poll events if enough time has elapsed -- or if the server told us
      // there's a new config.
      .filter(v -> v == TRIGGERED_BY_CONFIG_CHANGE_NOTIFICATION || lastPoll.hasElapsed(configPollInterval))
      .flatMap(ign -> {
        List<PortInfo> nodes = filterEligibleNodes();
        if (numFailedRefreshes.get() >= nodes.size()) {
          provider.signalConfigRefreshFailed(ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS);
          numFailedRefreshes.set(0);
        }
        return attemptUpdateGlobalConfig(Flux.fromIterable(nodes).take(MAX_PARALLEL_FETCH));
      })
      .subscribe(provider::proposeGlobalConfig);
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
   * Attempt to update global configurations for each of the provided (and eligible) nodes.
   *
   * @param nodes the nodes eligible for global config loading.
   * @return a {@link Flux} with all the successfully loaded global config contexts.
   */
  private Flux<ProposedGlobalConfigContext> attemptUpdateGlobalConfig(final Flux<PortInfo> nodes) {
    return nodes.flatMap(nodeInfo -> {
      NanoTimestamp start = NanoTimestamp.now();
      CoreContext ctx = core.context();
      CarrierGlobalConfigRequest request = new CarrierGlobalConfigRequest(
        configRequestTimeout,
        ctx,
        FailFastRetryStrategy.INSTANCE,
        nodeInfo.identifier(),
        currentVersion()
      );
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .filter(response -> {
          if (response.status().success()) {
            return true;
          }
          numFailedRefreshes.incrementAndGet();
          core.context().environment().eventBus().publish(new IndividualGlobalConfigRefreshFailedEvent(
            start.elapsed(),
            core.context(),
            null,
            nodeInfo.hostname(),
            response
          ));
          return false;
        })
        .map(response ->
          new ProposedGlobalConfigContext(new String(response.content(), UTF_8), nodeInfo.hostname())
        )
        // If we got a good proposed config, make sure to set the lastPoll timestamp.
        .doOnSuccess(r -> {
          numFailedRefreshes.set(0);
          lastPoll = NanoTimestamp.now();
        })
        .onErrorResume(t -> {
          numFailedRefreshes.incrementAndGet();
          core.context().environment().eventBus().publish(new IndividualGlobalConfigRefreshFailedEvent(
            start.elapsed(),
            core.context(),
            t,
            nodeInfo.hostname(),
            null
          ));
          return Mono.empty();
        });
    });
  }

  private ConfigVersion currentVersion() {
    GlobalConfig config = provider.config().globalConfig();
    return config == null ? ConfigVersion.ZERO : config.version();
  }

  private List<PortInfo> filterEligibleNodes() {
    GlobalConfig config = provider.config().globalConfig();
    if (config == null) {
      return Collections.emptyList();
    }

    List<PortInfo> nodes = new ArrayList<>(config.portInfos());
    shiftNodeList(nodes);

    return nodes
      .stream()
      .filter(n -> n.ports().containsKey(ServiceType.KV) || n.sslPorts().containsKey(ServiceType.KV))
      .collect(Collectors.toList());
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

  /**
   * Starts the {@link GlobalRefresher}.
   * <p>
   * Refreshing can be started and stopped multiple times until the non-reversible {@link #shutdown()} is called.
   *
   * @return a {@link Mono} completing when started.
   */
  public Mono<Void> start() {
    return Mono.defer(() -> {
      started = true;
      numFailedRefreshes.set(0);
      return Mono.empty();
    });
  }

  /**
   * Stops the {@link GlobalRefresher}.
   * <p>
   * Refreshing can be started and stopped multiple times until the non-reversible {@link #shutdown()} is called.
   *
   * @return a {@link Mono} completing when stopped.
   */
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      started = false;
      numFailedRefreshes.set(0);
      return Mono.empty();
    });
  }

  /**
   * Permanently shuts down this {@link GlobalRefresher}.
   *
   * @return a {@link Mono} completing when shutdown completed.
   */
  public Mono<Void> shutdown() {
    return stop().then(Mono.defer(() -> {
      if (!pollRegistration.isDisposed()) {
        pollRegistration.dispose();
      }
      return Mono.empty();
    }));
  }

}
