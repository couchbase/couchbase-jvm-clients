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

package com.couchbase.client.core.env;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

/**
 * The {@link IoEnvironment} holds all IO-related configuration and state.
 *
 * @since 2.0.0
 */
public class IoEnvironment {

  private final Duration connectTimeout;
  private final CompressionConfig compressionConfig;
  private final SecurityConfig securityConfig;
  private final Set<SaslMechanism> allowedSaslMechanisms;

  private final Supplier<EventLoopGroup> configEventLoopGroup;
  private final Supplier<EventLoopGroup> kvEventLoopGroup;
  private final Supplier<EventLoopGroup> queryEventLoopGroup;
  private final Supplier<EventLoopGroup> analyticsEventLoopGroup;
  private final Supplier<EventLoopGroup> searchEventLoopGroup;
  private final Supplier<EventLoopGroup> viewEventLoopGroup;

  public static IoEnvironment create() {
    return builder().build();
  }

  public static IoEnvironment.Builder builder() {
    return new Builder();
  }

  private IoEnvironment(final Builder builder) {
    connectTimeout = builder.connectTimeout == null
      ? Duration.ofSeconds(5)
      : builder.connectTimeout;
    compressionConfig = builder.compressionConfig == null
      ? CompressionConfig.disabled() // TODO: switch me to create once implemented
      : builder.compressionConfig;
    securityConfig = builder.securityConfig == null
      ? SecurityConfig.create()
      : builder.securityConfig;
    allowedSaslMechanisms = builder.allowedSaslMechanisms == null
      ? EnumSet.allOf(SaslMechanism.class)
      : builder.allowedSaslMechanisms;

    Supplier<EventLoopGroup> httpDefaultGroup = null;
    if (builder.queryEventLoopGroup == null
      || builder.analyticsEventLoopGroup == null
      || builder.searchEventLoopGroup == null
      || builder.viewEventLoopGroup == null) {
      httpDefaultGroup = createEventLoopGroup(fairThreadCount(), "cb-io-http");
    }

    configEventLoopGroup = builder.configEventLoopGroup == null
      ? createEventLoopGroup(1, "cb-io-config")
      : builder.configEventLoopGroup;
    kvEventLoopGroup = builder.kvEventLoopGroup == null
      ? createEventLoopGroup(fairThreadCount(), "cb-io-kv")
      : builder.kvEventLoopGroup;
    queryEventLoopGroup = builder.queryEventLoopGroup == null
      ? httpDefaultGroup
      : builder.queryEventLoopGroup;
    analyticsEventLoopGroup = builder.analyticsEventLoopGroup == null
      ? httpDefaultGroup
      : builder.queryEventLoopGroup;
    searchEventLoopGroup = builder.searchEventLoopGroup == null
      ? httpDefaultGroup
      : builder.searchEventLoopGroup;
    viewEventLoopGroup = builder.viewEventLoopGroup == null
      ? httpDefaultGroup
      : builder.viewEventLoopGroup;
  }

  /**
   * The full timeout for a channel to be established, includes the
   * socket connect as well all the back and forth depending on the
   * service used.
   *
   * @return the full connect timeout for a channel.
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  /**
   * Configures the way {@link CompressionConfig} is set up in the client.
   *
   * @return the compression settings.
   */
  public CompressionConfig compressionConfig() {
    return compressionConfig;
  }

  /**
   * Configures the way transport layer security is set up in the client.
   *
   * @return the security settings.
   */
  public SecurityConfig securityConfig() {
    return securityConfig;
  }

  /**
   * Customizes the SASL mechanisms that are allowed to be negotiated, even
   * if the server would support more/different ones.
   *
   * @return the set of mechanisms allowed.
   */
  public Set<SaslMechanism> allowedSaslMechanisms() {
    return allowedSaslMechanisms;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for config traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> configEventLoopGroup() {
    return configEventLoopGroup;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for Key/Value traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> kvEventLoopGroup() {
    return kvEventLoopGroup;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for N1QL Query traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> queryEventLoopGroup() {
    return queryEventLoopGroup;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for analytics traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> analyticsEventLoopGroup() {
    return analyticsEventLoopGroup;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for search traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> searchEventLoopGroup() {
    return searchEventLoopGroup;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for view traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> viewEventLoopGroup() {
    return viewEventLoopGroup;
  }

  /**
   * Helper method to select the best event loop group type based on the features
   * available on the current platform.
   *
   * <p>If KQueue or Epoll native transports are available, it will use those. If not
   * there is always the fallback to the Nio transport which is always available.</p>
   *
   * @param numThreads number of threads to to assign to the group.
   * @param poolName the name of the threads.
   * @return the created group.
   */
  private static OwnedSupplier<EventLoopGroup> createEventLoopGroup(int numThreads, String poolName) {
    ThreadFactory threadFactory = new DefaultThreadFactory(poolName);

    if (KQueue.isAvailable()) {
      return new OwnedSupplier<>(new KQueueEventLoopGroup(numThreads, threadFactory));
    } else if (Epoll.isAvailable()) {
      return new OwnedSupplier<>(new EpollEventLoopGroup(numThreads, threadFactory));
    } else {
      return new OwnedSupplier<>(new NioEventLoopGroup(numThreads, threadFactory));
    }
  }

  /**
   * Picks a "fair" and sensible thread count, for use with a default event
   * loop size.
   *
   * <p>It is kinda hard to figure out how big the event loop pools should be, but for
   * now we run with the following algorithm: use half the virtual core size, but a minimum
   * of 2 and a maximum of 8. This can always be overridden by the user if needed.</p>
   *
   * @return the number of threads deemed to be fair for the current system.
   */
  private static int fairThreadCount() {
    int cores = Runtime.getRuntime().availableProcessors();
    cores = cores < 2 ? 2 : cores;
    cores = cores > 8 ? 8 : cores;
    return cores;
  }

  public static class Builder {

    private Duration connectTimeout = null;
    private CompressionConfig compressionConfig = null;
    private SecurityConfig securityConfig = null;
    private Set<SaslMechanism> allowedSaslMechanisms = null;
    private Supplier<EventLoopGroup> configEventLoopGroup = null;
    private Supplier<EventLoopGroup> kvEventLoopGroup = null;
    private Supplier<EventLoopGroup> queryEventLoopGroup = null;
    private Supplier<EventLoopGroup> analyticsEventLoopGroup = null;
    private Supplier<EventLoopGroup> searchEventLoopGroup = null;
    private Supplier<EventLoopGroup> viewEventLoopGroup = null;

    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public IoEnvironment build() {
      return new IoEnvironment(this);
    }
  }

}
