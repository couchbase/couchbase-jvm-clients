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

  public ClusterManagerBucketRefresher(final ConfigurationProvider provider, final Core core) {
    this.provider = provider;
    this.core = core;
  }

  @Override
  public synchronized Mono<Void> register(final String name) {
    if (registrations.containsKey(name)) {
      return Mono.empty();
    }

    return Mono.defer(() -> {
      final CoreContext ctx = core.context();

      Disposable registration = Mono.defer(() -> {
        BucketConfigStreamingRequest request = new BucketConfigStreamingRequest(
          ctx.environment().timeoutConfig().managerTimeout(),
          ctx,
          BestEffortRetryStrategy.INSTANCE,
          name,
          ctx.environment().credentials()
        );
        core.send(request);
        return Reactor.wrap(request, request.response(), true);
      })
      .flux()
      .flatMap(res -> {
        if (res.status().success()) {
          return res.configs().map(config -> new ProposedBucketConfigContext(name, config, res.address()));
        } else {
          // If the response did not come back with a 200 (success) we also treat it as an error
          // and retry the whole thing
          return Flux.error(new ConfigException());
        }
      })
       .doOnComplete(() -> {
         // If the stream completes normally we turn it into an exception so it also gets
         // handled in the retryWhen below.
         throw new ConfigException();
       })
      .retryWhen(Retry
        .any()
        .exponentialBackoff(Duration.ofMillis(32), Duration.ofMillis(4096)))
      .subscribe(provider::proposeBucketConfig);

      registrations.put(name, registration);
      return Mono.empty();
    });
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
