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
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
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

  public static Core create(final CoreEnvironment environment) {
    return new Core(environment);
  }

  private Core(final CoreEnvironment environment) {
    this.coreContext = new CoreContext(this, CORE_IDS.incrementAndGet(), environment);
    this.configurationProvider = configurationProvider();
    this.nodes = new CopyOnWriteArrayList<>();
    currentConfig = configurationProvider.config();
    configurationProvider.configs().subscribe(config -> Core.this.currentConfig = config);
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
        node.connect();
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

  private static Locator locator(final ServiceType serviceType) {
    switch (serviceType) {
      case KV:
        return KEY_VALUE_LOCATOR;
      case MANAGER:
        return MANAGER_LOCATOR;
      default:
        throw new IllegalStateException("Unsupported ServiceType: " + serviceType);
    }
  }

}
