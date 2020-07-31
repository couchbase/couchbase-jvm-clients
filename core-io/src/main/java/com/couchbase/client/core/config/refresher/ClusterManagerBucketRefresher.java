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
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.config.BucketConfigRefreshFailedEvent;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.msg.manager.BucketConfigStreamingRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.reactor.Retry;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterManagerBucketRefresher implements BucketRefresher {

  /**
   * Holds the config provider as a reference.
   */
  private final ConfigurationProvider provider;

  /**
   * Holds the core as a reference.
   */
  private final Core core;

  /**
   * Stores the registrations so they can be unsubscribed when needed.
   */
  private final Map<String, Disposable> registrations = new ConcurrentHashMap<>();

  /**
   * Holds the event bus to send events to.
   */
  private final EventBus eventBus;

  public ClusterManagerBucketRefresher(final ConfigurationProvider provider, final Core core) {
    this.provider = provider;
    this.core = core;
    this.eventBus = core.context().environment().eventBus();
  }

  /**
   * Registers a given bucket for http-based config refresh if not already registered.
   *
   * @param name the name of the bucket.
   * @return the Mono once registered.
   */
  @Override
  public synchronized Mono<Void> register(final String name) {
    if (registrations.containsKey(name)) {
      return Mono.empty();
    }

    return Mono.defer(() -> {
      Disposable registration = registerStream(core.context(), name);
      registrations.put(name, registration);
      return Mono.empty();
    });
  }

  /**
   * Registers the given bucket name with the http stream.
   *
   * <p>Note that this method deliberately subscribes "out of band" and not being flatMapped into the
   * {@link #register(String)} return value. The idea is that the flux config subscription keeps on going
   * forever until specifically unsubscribed through either {@link #deregister(String)} or {@link #shutdown()}.</p>
   *
   * @param ctx the core context to use.
   * @param name the name of the bucket.
   * @return once registered, returns the disposable so it can be later used to deregister.
   */
  private Disposable registerStream(final CoreContext ctx, final String name) {
    return Mono.defer(() -> {
      BucketConfigStreamingRequest request = new BucketConfigStreamingRequest(
        ctx.environment().timeoutConfig().managementTimeout(),
        ctx,
        BestEffortRetryStrategy.INSTANCE,
        name,
        ctx.authenticator()
      );
      core.send(request);
      return Reactor.wrap(request, request.response(), true);
    })
      .flux()
      .flatMap(res -> {
        if (res.status().success()) {
          return res.configs().map(config -> new ProposedBucketConfigContext(name, config, res.address()));
        } else {
          eventBus.publish(new BucketConfigRefreshFailedEvent(
            core.context(),
            BucketConfigRefreshFailedEvent.RefresherType.MANAGER,
            BucketConfigRefreshFailedEvent.Reason.INDIVIDUAL_REQUEST_FAILED,
            Optional.of(res)
          ));
          // If the response did not come back with a 200 (success) we also treat it as an error
          // and retry the whole thing
          return Flux.error(new ConfigException());
        }
      })
      .doOnError(e -> eventBus.publish(new BucketConfigRefreshFailedEvent(
        core.context(),
        BucketConfigRefreshFailedEvent.RefresherType.MANAGER,
        BucketConfigRefreshFailedEvent.Reason.STREAM_FAILED,
        Optional.of(e)
      )))
      .doOnComplete(() -> {
        eventBus.publish(new BucketConfigRefreshFailedEvent(
          core.context(),
          BucketConfigRefreshFailedEvent.RefresherType.MANAGER,
          BucketConfigRefreshFailedEvent.Reason.STREAM_CLOSED,
          Optional.empty()
        ));
        // If the stream completes normally we turn it into an exception so it also gets
        // handled in the retryWhen below.
        throw new ConfigException();
      })
      .retryWhen(Retry
        .any()
        .exponentialBackoff(Duration.ofMillis(32), Duration.ofMillis(4096))
        .toReactorRetry())
      .subscribe(provider::proposeBucketConfig);
  }

  @Override
  public synchronized Mono<Void> deregister(final String name) {
    return Mono.defer(() -> {
      Disposable registration = registrations.get(name);
      if (registration != null && !registration.isDisposed()) {
        registration.dispose();
      }
      return Mono.empty();
    });
  }

  @Override
  public synchronized Mono<Void> shutdown() {
    return Mono.defer(() -> {
      for (Disposable registration : registrations.values()) {
        if (!registration.isDisposed()) {
          registration.dispose();
        }
      }
      registrations.clear();
      return Mono.empty();
    });
  }

  /**
   * No action needed when a config is marked as tainted for the cluster manager
   * refresher, since the server pushes new configs anyways during rebalance.
   *
   * @param name the name of the bucket.
   */
  @Override
  public void markTainted(String name) { }

  /**
   * No action needed when a config is marked as untainted for the cluster manager
   * refresher, since the server pushes new configs anyways during rebalance.
   *
   * @param name the name of the bucket.
   */
  @Override
  public void markUntainted(String name) { }

}
