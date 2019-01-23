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

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.node.KeyValueLocator;
import com.couchbase.client.core.node.Locator;
import com.couchbase.client.core.node.ManagerLocator;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.RoundRobinLocator;
import com.couchbase.client.core.service.ServiceType;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The main entry point into the core layer.
 *
 * <p>This class has been around behind a facade in the 1.x days, but here it is just a plain
 * simple class that can be instantiated and is used across the upper language bindings.</p>
 *
 * @since 2.0.0
 */
public class Core {

  /**
   * Counts up core ids for each new instance.
   */
  private static final AtomicLong CORE_IDS = new AtomicLong();

  private static final KeyValueLocator KEY_VALUE_LOCATOR = new KeyValueLocator();

  private static final ManagerLocator MANAGER_LOCATOR = new ManagerLocator();

  private static final RoundRobinLocator QUERY_LOCATOR = new RoundRobinLocator(ServiceType.QUERY);

  private static final RoundRobinLocator ANALYTICS_LOCATOR = new RoundRobinLocator(ServiceType.ANALYTICS);

  private static final RoundRobinLocator SEARCH_LOCATOR = new RoundRobinLocator(ServiceType.SEARCH);

  private static final RoundRobinLocator VIEWS_LOCATOR = new RoundRobinLocator(ServiceType.VIEWS);

  /**
   * Holds the current core context.
   */
  private final CoreContext coreContext;

  /**
   * Holds the current configuration provider.
   */
  private final ConfigurationProvider configurationProvider;

  private volatile ClusterConfig currentConfig;

  /**
   * The list of currently managed nodes against the cluster.
   */
  private final CopyOnWriteArrayList<Node> nodes;

  private final AtomicBoolean reconfigureInProgress = new AtomicBoolean(false);

  public static Core create(final CoreEnvironment environment) {
    return new Core(environment);
  }

  private Core(final CoreEnvironment environment) {
    this.coreContext = new CoreContext(this, CORE_IDS.incrementAndGet(), environment);
    this.configurationProvider = configurationProvider();
    this.nodes = new CopyOnWriteArrayList<>();
    currentConfig = configurationProvider.config();
    configurationProvider
      .configs()
      .doOnNext(c -> currentConfig = c)
      .subscribe(c -> reconfigure());
  }

  /**
   * During testing this can be overridden so that a custom configuration provider is used
   * in the system.
   *
   * @return by default returns the default config provider.
   */
  protected ConfigurationProvider configurationProvider() {
    return new DefaultConfigurationProvider(this);
  }

  public <R extends Response> void send(final Request<R> request) {
    send(request, true);
  }

  @Stability.Internal
  @SuppressWarnings({"unchecked"})
  public <R extends Response> void send(final Request<R> request, boolean registerForTimeout) {
    if (registerForTimeout) {
      context().environment().timer().register((Request<Response>) request);
    }

    locator(request.serviceType()).dispatch(request, nodes, currentConfig, context());
  }

  /**
   * Returns the {@link CoreContext} of this core instance.
   *
   * @return the core context.
   */
  public CoreContext context() {
    return coreContext;
  }

  /**
   * Attempts to open a bucket and fails the {@link Mono} if there is a persistent error
   * as the reason.
   */
  @Stability.Internal
  public Mono<Void> openBucket(final String name) {
    return configurationProvider.openBucket(name);
  }

  /**
   * This API provides access to the current config that is published throughout the core.
   *
   * <p>Note that this is internal API and might change at any time.</p>
   */
  @Stability.Internal
  public ClusterConfig clusterConfig() {
    return configurationProvider.config();
  }

  /**
   * Attempts to close a bucket and fails the {@link Mono} if there is a persistent error
   * as the reason.
   */
  @Stability.Internal
  public Mono<Void> closeBucket(final String name) {
    return configurationProvider.closeBucket(name);
  }

  /**
   * This method can be used by a caller to make sure a certain service is enabled at the given
   * target node.
   *
   * <p>This is advanced, internal functionality and should only be used if the caller knows
   * what they are doing.</p>
   *
   * @param target the node to check.
   * @param serviceType the service type to enable if not enabled already.
   * @return a {@link Mono} which completes once initiated.
   */
  @Stability.Internal
  public Mono<Void> ensureServiceAt(final NetworkAddress target, final ServiceType serviceType,
                                    int port, Optional<String> bucket) {
    return Flux
      .fromIterable(nodes)
      .filter(n -> n.address().equals(target))
      .switchIfEmpty(Mono.defer(() -> {
        Node node = Node.create(coreContext, target, coreContext.environment().credentials());
        nodes.add(node);
        return Mono.just(node);
      }))
      .flatMap(node -> node.addService(serviceType, port, bucket))
      .then();
  }

  /**
   * Shuts down this core and all associated, owned resources.
   */
  @Stability.Internal
  public Mono<Void> shutdown() {
    // tODO: implement me
    return Mono.empty();
  }

  /**
   * Reconfigures the SDK topology to align with the current server configuration.
   *
   * <p>When reconfigure is called, it will grab a current configuration and then add/remove
   * nodes/services to mirror the current topology and configuration settings.</p>
   *
   * <p>This is a eventually consistent process, so in-flight operations might still be rescheduled
   * and then picked up later (or cancelled, depending on the strategy). For those coming from 1.x,
   * it works very similar.</p>
   */
  private void reconfigure() {
    if (reconfigureInProgress.compareAndSet(false, true)) {
      // TODO: proper error handling/logging!!
      Flux
        .just(currentConfig)
        .flatMap(cc -> Flux.fromIterable(cc.bucketConfigs().values()))
        .flatMap(bc -> Flux.fromIterable(bc.nodes())
          .flatMap(ni -> Flux
            .fromIterable(ni.services().entrySet())
            .flatMap(s -> ensureServiceAt(ni.hostname(), s.getKey(), s.getValue(), Optional.of(bc.name())))))
        .subscribe(new Subscriber<Void>() {
          @Override
          public void onSubscribe(Subscription s) {
            System.err.println("s");
          }

          @Override
          public void onNext(Void aVoid) {
            System.err.println("n");
          }

          @Override
          public void onError(Throwable t) {
            System.err.println("e: " + t);
          }

          @Override
          public void onComplete() {
            System.err.println("oc");
          }
        });
    } else {
      // todo: trace log that reconfigure attempt ignored since one is already in progress
    }
  }

  private static Locator locator(final ServiceType serviceType) {
    switch (serviceType) {
      case KV:
        return KEY_VALUE_LOCATOR;
      case MANAGER:
        return MANAGER_LOCATOR;
      case QUERY:
        return QUERY_LOCATOR;
      case ANALYTICS:
        return ANALYTICS_LOCATOR;
      case SEARCH:
        return SEARCH_LOCATOR;
      case VIEWS:
        return VIEWS_LOCATOR;
      default:
        throw new IllegalStateException("Unsupported ServiceType: " + serviceType);
    }
  }

}
