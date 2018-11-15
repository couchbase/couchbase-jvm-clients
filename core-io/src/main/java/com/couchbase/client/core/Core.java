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
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;
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

  /**
   * Holds the current core context.
   */
  private final CoreContext coreContext;

  /**
   * Holds the current configuration provider.
   */
  private final ConfigurationProvider configurationProvider;

  public static Core create(final CoreEnvironment environment, final Set<NetworkAddress> seedNodes) {
    return new Core(environment, seedNodes);
  }

  private Core(final CoreEnvironment environment, final Set<NetworkAddress> seedNodes) {
    this.coreContext = new CoreContext(CORE_IDS.incrementAndGet(), environment);
    this.configurationProvider = configurationProvider(seedNodes);
  }

  /**
   * During testing this can be overridden so that a custom configuration provider is used
   * in the system.
   *
   * @return by default returns the default config provider.
   */
  protected ConfigurationProvider configurationProvider(final Set<NetworkAddress> seedNodes) {
    return DefaultConfigurationProvider.create(this, seedNodes);
  }

  public <R extends Response> void send(final Request<R> request) {
    return;
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
   * Shuts down this core and all associated, owned resources.
   */
  @Stability.Internal
  public Mono<Void> shutdown(final String name) {
    throw new UnsupportedOperationException("implement me!");
  }

}
