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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.core.config.refresher.KeyValueBucketRefresher.MAX_PARALLEL_FETCH;
import static com.couchbase.client.core.config.refresher.KeyValueBucketRefresher.POLLER_INTERVAL;
import static com.couchbase.client.core.config.refresher.KeyValueBucketRefresher.clampConfigRequestTimeout;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link GlobalRefresher} keeps the cluster-level global config up-to-date.
 *
 * <p>It works very similar to the key value refresher on the bucket level, but explicitly has no bucket
 * level scope. It can be started and stopped, since there might be situations where polling is not needed.</p>
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
   * Indicates if the refresher is started/stopped.
   */
  private volatile boolean started;

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
   * Stores the last poll attempt to calculate if enough time has passed for a new poll.
   */
  private volatile NanoTimestamp lastPoll;

  /**
   * Creates a new global refresher.
   *
   * @param provider the config provider which should get the config updates proposed.
   * @param core the core to send the config commands to.
   */
  public GlobalRefresher(final ConfigurationProvider provider, final Core core) {
    this.provider = provider;
    this.core = core;
    this.configPollInterval = core.context().environment().ioConfig().configPollInterval();
    this.configRequestTimeout = clampConfigRequestTimeout(configPollInterval);
    this.started = false;

    pollRegistration = Flux
      .interval(pollerInterval(), core.context().environment().scheduler())
      // Proposing a new config should be quick, but just in case it gets held up we do
      // not want to terminate the refresher and just drop the ticks and keep going.
      .onBackpressureDrop()
      // If the refresher is not started yet, drop all intervals
      .filter(v -> started)
      // Since the POLLER_INTERVAL is smaller than the config poll interval, make sure
      // we only emit poll events if enough time has elapsed.
      .filter(v -> lastPoll == null || lastPoll.hasElapsed(configPollInterval))
      .flatMap(ign -> attemptUpdateGlobalConfig(filterEligibleNodes()))
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

  private Flux<ProposedGlobalConfigContext> attemptUpdateGlobalConfig(final Flux<PortInfo> nodes) {
    return nodes.flatMap(nodeInfo -> {
      CoreContext ctx = core.context();
      CarrierGlobalConfigRequest request = new CarrierGlobalConfigRequest(
        configRequestTimeout,
        ctx,
        FailFastRetryStrategy.INSTANCE,
        nodeInfo.identifier()
      );
      core.send(request);
      return Reactor
        .wrap(request, request.response(), true)
        .filter(response -> {
          // TODO: debug event that it got ignored.
          return response.status().success();
        })
        .map(response ->
          new ProposedGlobalConfigContext(new String(response.content(), UTF_8), nodeInfo.hostname())
        )
        // If we got a good proposed config, make sure to set the lastPoll timestamp.
        .doOnSuccess(r -> lastPoll = NanoTimestamp.now())
        .onErrorResume(t -> {
          // TODO: raise a warning that fetching a config individual failed.
          return Mono.empty();
        });
    });
  }

  private Flux<PortInfo> filterEligibleNodes() {
    return Flux.defer(() -> {
      GlobalConfig config = provider.config().globalConfig();
      if (config == null) {
        // todo: log debug that no node found to refresh a config from
        return Flux.empty();
      }

      List<PortInfo> nodes = new ArrayList<>(config.portInfos());
      shiftNodeList(nodes);

      return Flux
        .fromIterable(nodes)
        .filter(n -> n.ports().containsKey(ServiceType.KV)
          || n.sslPorts().containsKey(ServiceType.KV))
        .take(MAX_PARALLEL_FETCH);
    });
  }

  /**
   * Helper method to transparently rearrange the node list based on the current global offset.
   *
   * @param nodeList the list to shift.
   */
  private <T> void shiftNodeList(final List<T> nodeList) {
    int shiftBy = (int) (nodeOffset.getAndIncrement() % nodeList.size());
    for(int i = 0; i < shiftBy; i++) {
      T element = nodeList.remove(0);
      nodeList.add(element);
    }
  }

  public Mono<Void> start() {
    return Mono.defer(() -> {
      started = true;
      return Mono.empty();
    });
  }

  public Mono<Void> stop() {
    return Mono.defer(() -> {
      started = false;
      return Mono.empty();
    });
  }

  public Mono<Void> shutdown() {
    return stop().then(Mono.defer(() -> {
      if (!pollRegistration.isDisposed()) {
        pollRegistration.dispose();
      }
      return Mono.empty();
    }));
  }

}
