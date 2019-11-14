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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.epoll.Epoll;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueue;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.couchbase.client.core.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.core.deps.io.netty.util.concurrent.GenericFutureListener;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The {@link IoEnvironment} holds all IO-related configuration and state.
 *
 * @since 2.0.0
 */
public class IoEnvironment {

  public static final boolean DEFAULT_NATIVE_IO_ENABLED = true;

  private final boolean nativeIoEnabled;
  private final Supplier<EventLoopGroup> managerEventLoopGroup;
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

  public static Builder managerEventLoopGroup(EventLoopGroup managerEventLoopGroup) {
    return builder().managerEventLoopGroup(managerEventLoopGroup);
  }

  public static Builder kvEventLoopGroup(EventLoopGroup kvEventLoopGroup) {
    return builder().kvEventLoopGroup(kvEventLoopGroup);
  }

  public static Builder queryEventLoopGroup(EventLoopGroup queryEventLoopGroup) {
    return builder().queryEventLoopGroup(queryEventLoopGroup);
  }

  public static Builder analyticsEventLoopGroup(EventLoopGroup analyticsEventLoopGroup) {
    return builder().analyticsEventLoopGroup(analyticsEventLoopGroup);
  }

  public static Builder searchEventLoopGroup(EventLoopGroup searchEventLoopGroup) {
    return builder().searchEventLoopGroup(searchEventLoopGroup);
  }

  public static Builder viewEventLoopGroup(EventLoopGroup viewEventLoopGroup) {
    return builder().viewEventLoopGroup(viewEventLoopGroup);
  }

  public static Builder enableNativeIo(boolean nativeIoEnabled) {
    return builder().enableNativeIo(nativeIoEnabled);
  }

  /**
   * Returns this environment as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("nativeIoEnabled", nativeIoEnabled);

    Set<String> eventLoopGroups = new HashSet<>();
    eventLoopGroups.add(managerEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(kvEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(queryEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(analyticsEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(searchEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(viewEventLoopGroup.get().getClass().getSimpleName());
    export.put("eventLoopGroups", eventLoopGroups);

    return export;
  }

  private IoEnvironment(final Builder builder) {
    nativeIoEnabled = builder.nativeIoEnabled;

    Supplier<EventLoopGroup> httpDefaultGroup = null;
    if (builder.queryEventLoopGroup == null
      || builder.analyticsEventLoopGroup == null
      || builder.searchEventLoopGroup == null
      || builder.viewEventLoopGroup == null) {
      httpDefaultGroup = createEventLoopGroup(nativeIoEnabled, fairThreadCount(), "cb-io-http");
    }

    managerEventLoopGroup = builder.managerEventLoopGroup == null
      ? createEventLoopGroup(nativeIoEnabled, 1, "cb-io-manager")
      : builder.managerEventLoopGroup;
    sanityCheckEventLoop(managerEventLoopGroup);

    kvEventLoopGroup = builder.kvEventLoopGroup == null
      ? createEventLoopGroup(nativeIoEnabled, fairThreadCount(), "cb-io-kv")
      : builder.kvEventLoopGroup;
    sanityCheckEventLoop(kvEventLoopGroup);

    queryEventLoopGroup = builder.queryEventLoopGroup == null
      ? httpDefaultGroup
      : builder.queryEventLoopGroup;
    sanityCheckEventLoop(queryEventLoopGroup);

    analyticsEventLoopGroup = builder.analyticsEventLoopGroup == null
      ? httpDefaultGroup
      : builder.queryEventLoopGroup;
    sanityCheckEventLoop(analyticsEventLoopGroup);

    searchEventLoopGroup = builder.searchEventLoopGroup == null
      ? httpDefaultGroup
      : builder.searchEventLoopGroup;
    sanityCheckEventLoop(searchEventLoopGroup);

    viewEventLoopGroup = builder.viewEventLoopGroup == null
      ? httpDefaultGroup
      : builder.viewEventLoopGroup;
    sanityCheckEventLoop(viewEventLoopGroup);
  }

  /**
   * Helper method to check if the event loop group is allowed with the current setup.
   *
   * @param group the group to check.
   */
  private void sanityCheckEventLoop(final Supplier<EventLoopGroup> group) {
    if (!nativeIoEnabled && !(group.get() instanceof NioEventLoopGroup)) {
      throw new IllegalStateException("Native IO is disabled and the EventLoopGroup is not a NioEventLoopGroup");
    }
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for config traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> managerEventLoopGroup() {
    return managerEventLoopGroup;
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
   * Returns true if native IO is enabled and can be used if supported.
   *
   * @return true if enabled.
   */
  public boolean nativeIoEnabled() {
    return nativeIoEnabled;
  }

  public Mono<Void> shutdown(Duration timeout) {
    return Flux.merge(
      shutdownGroup(managerEventLoopGroup, timeout),
      shutdownGroup(kvEventLoopGroup, timeout),
      shutdownGroup(queryEventLoopGroup, timeout),
      shutdownGroup(analyticsEventLoopGroup, timeout),
      shutdownGroup(searchEventLoopGroup, timeout),
      shutdownGroup(viewEventLoopGroup, timeout)
    ).then();
  }

  private Mono<Void> shutdownGroup(Supplier<EventLoopGroup> suppliedGroup, Duration timeout) {
    if (suppliedGroup instanceof OwnedSupplier) {
      EventLoopGroup group = suppliedGroup.get();
      if (!group.isShutdown() && !group.isShuttingDown()) {
        return Mono.create(sink -> group.shutdownGracefully(0, timeout.toMillis(), TimeUnit.MILLISECONDS)
          .addListener(future -> {
            if (future.isSuccess()) {
              sink.success();
            } else {
              sink.error(future.cause());
            }
          })
        );
      } else {
        return Mono.empty();
      }
    } else {
      return Mono.empty();
    }
  }

  /**
   * Helper method to select the best event loop group type based on the features
   * available on the current platform.
   *
   * <p>If KQueue or Epoll native transports are available, it will use those. If not
   * there is always the fallback to the Nio transport which is always available.</p>
   *
   * @param nativeIoEnabled native IO enabled.
   * @param numThreads number of threads to to assign to the group.
   * @param poolName the name of the threads.
   * @return the created group.
   */
  private static OwnedSupplier<EventLoopGroup> createEventLoopGroup(final boolean nativeIoEnabled, final int numThreads,
                                                                    final String poolName) {
    ThreadFactory threadFactory = new DefaultThreadFactory(poolName, true);

    if (nativeIoEnabled && Epoll.isAvailable()) {
      return new OwnedSupplier<>(new EpollEventLoopGroup(numThreads, threadFactory));
    } else if (nativeIoEnabled && KQueue.isAvailable()) {
      return new OwnedSupplier<>(new KQueueEventLoopGroup(numThreads, threadFactory));
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

    private boolean nativeIoEnabled = DEFAULT_NATIVE_IO_ENABLED;
    private Supplier<EventLoopGroup> managerEventLoopGroup = null;
    private Supplier<EventLoopGroup> kvEventLoopGroup = null;
    private Supplier<EventLoopGroup> queryEventLoopGroup = null;
    private Supplier<EventLoopGroup> analyticsEventLoopGroup = null;
    private Supplier<EventLoopGroup> searchEventLoopGroup = null;
    private Supplier<EventLoopGroup> viewEventLoopGroup = null;

    public Builder managerEventLoopGroup(EventLoopGroup managerEventLoopGroup) {
      this.managerEventLoopGroup = new ExternalSupplier<>(managerEventLoopGroup);
      return this;
    }

    public Builder kvEventLoopGroup(EventLoopGroup kvEventLoopGroup) {
      this.kvEventLoopGroup = new ExternalSupplier<>(kvEventLoopGroup);
      return this;
    }

    public Builder queryEventLoopGroup(EventLoopGroup queryEventLoopGroup) {
      this.queryEventLoopGroup = new ExternalSupplier<>(queryEventLoopGroup);
      return this;
    }

    public Builder analyticsEventLoopGroup(EventLoopGroup analyticsEventLoopGroup) {
      this.analyticsEventLoopGroup = new ExternalSupplier<>(analyticsEventLoopGroup);
      return this;
    }

    public Builder searchEventLoopGroup(EventLoopGroup searchEventLoopGroup) {
      this.searchEventLoopGroup = new ExternalSupplier<>(searchEventLoopGroup);
      return this;
    }

    public Builder viewEventLoopGroup(EventLoopGroup viewEventLoopGroup) {
      this.viewEventLoopGroup = new ExternalSupplier<>(viewEventLoopGroup);
      return this;
    }

    public Builder enableNativeIo(boolean nativeIoEnabled) {
      this.nativeIoEnabled = nativeIoEnabled;
      return this;
    }

    public IoEnvironment build() {
      return new IoEnvironment(this);
    }
  }

}
