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
import com.couchbase.client.core.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.epoll.Epoll;
import com.couchbase.client.core.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueue;
import com.couchbase.client.core.deps.io.netty.channel.kqueue.KQueueEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.core.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.couchbase.client.core.error.InvalidArgumentException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.couchbase.client.core.env.OwnedOrExternal.external;
import static com.couchbase.client.core.env.OwnedOrExternal.owned;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static com.couchbase.client.core.util.Validators.notNull;

/**
 * The {@link IoEnvironment} holds the I/O event loops and state.
 * <p>
 * Note that this class only contains tunables and state for the I/O event loops that drive the actual operations
 * inside netty. If you are looking for general configuration if I/O properties, those are located inside the
 * {@link IoConfig} class instead.
 * <p>
 * By default the IO environment creates 3 distinct {@link EventLoopGroup EventLoopGroups}. One for HTTP services
 * (which includes query, search, analytics and views), one for the KV service and one for the management service. The
 * HTTP and the KV service try to occupy a "fair" number of thread pools for each. Usually it will be half the number
 * of reported logical CPUs by the JVM, but maximum of 8 and minimum of 2 each. The management service will only ever
 * occupy one thread. The HTTP and KV thread pools are split in SDK 3 (they were not in SDK 2) so that longer running
 * N1QL queries and larger streaming results do not interfere with high-throughput low-latency KV workloads as much.
 * <p>
 * By default, the SDK will use "native" event loop groups - epoll on linux and kqueue on OSX in favor of the more
 * generic and slower NIO transport. This should work fine out of the box nearly always, but you can disable it through
 * the {@link #enableNativeIo(boolean)} builder setting.
 * <p>
 * You cannot re-use your own event loop groups from your own application because the SDK ships a repackaged netty
 * version to avoid version and classpath issues.
 *
 * @since 2.0.0
 */
public class IoEnvironment {

  static {
    final String samplingProperty = "com.couchbase.client.core.deps.io.netty.leakDetection.samplingInterval";
    final long defaultSamplingInterval = 65536;

    if (isNullOrEmpty(System.getProperty(samplingProperty))) {
      System.setProperty(samplingProperty, Long.toString(defaultSamplingInterval));
    }
  }

  /**
   * Native IO is enabled by default.
   */
  public static final boolean DEFAULT_NATIVE_IO_ENABLED = true;

  @Stability.Internal
  public static final int DEFAULT_EVENT_LOOP_THREAD_COUNT = fairThreadCount();

  private final boolean nativeIoEnabled;
  private final int eventLoopThreadCount;
  private final OwnedOrExternal<EventLoopGroup> managerEventLoopGroup;
  private final OwnedOrExternal<EventLoopGroup> kvEventLoopGroup;
  private final OwnedOrExternal<EventLoopGroup> queryEventLoopGroup;
  private final OwnedOrExternal<EventLoopGroup> analyticsEventLoopGroup;
  private final OwnedOrExternal<EventLoopGroup> searchEventLoopGroup;
  private final OwnedOrExternal<EventLoopGroup> viewEventLoopGroup;
  private final OwnedOrExternal<EventLoopGroup> eventingEventLoopGroup;
  private final OwnedOrExternal<EventLoopGroup> backupEventLoopGroup;

  /**
   * Creates the {@link IoEnvironment} with default settings.
   *
   * @return the created environment.
   * @deprecated Instead, please use
   * {@link CoreEnvironment.Builder#ioEnvironment(Consumer)}
   * and configure the builder passed to the consumer.
   * Note: CoreEnvironment is a base class; you'll
   * probably call that method via a subclass named
   * {@code ClusterEnvironment}.
   */
  @Deprecated
  public static IoEnvironment create() {
    return new Builder().build();
  }

  /**
   * Creates a Builder for the {@link IoEnvironment} to customize its settings.
   *
   * @return the {@link Builder} to customize the settings.
   *
   * @deprecated Instead of creating a new builder, please use
   * {@link CoreEnvironment.Builder#ioEnvironment(Consumer)}
   * and configure the builder passed to the consumer.
   * Note: CoreEnvironment is a base class; you'll
   * probably call that method via a subclass named
   * {@code ClusterEnvironment}.
   */
  @Deprecated
  public static IoEnvironment.Builder builder() {
    return new Builder();
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the management service.
   * <p>
   * Note that you usually do not need to tweak the event loop for the manager service, only if you perform
   * long-running management queries that interfere with regular traffic.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder managerEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().managerEventLoopGroup(eventLoopGroup);
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the management service.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder kvEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().kvEventLoopGroup(eventLoopGroup);
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the query service.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder queryEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().queryEventLoopGroup(eventLoopGroup);
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the analytics service.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder analyticsEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().analyticsEventLoopGroup(eventLoopGroup);
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the search service.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder searchEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().searchEventLoopGroup(eventLoopGroup);
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the view service.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder viewEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().viewEventLoopGroup(eventLoopGroup);
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the eventing service.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder eventingEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().eventingEventLoopGroup(eventLoopGroup);
  }

  /**
   * Allows to specify a custom event loop group (I/O event loop thread pool) for the backup service.
   * <p>
   * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
   * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
   * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
   * all the services and what effect a custom pool might have.
   *
   * @param eventLoopGroup the dedicated event loop group to use.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  @Stability.Volatile
  public static Builder backupEventLoopGroup(final EventLoopGroup eventLoopGroup) {
    return builder().backupEventLoopGroup(eventLoopGroup);
  }

  /**
   * Overrides the number of threads used per event loop.
   * <p>
   * If not manually overridden, a fair thread count is calculated, see {@link #fairThreadCount()} for more
   * information on the heuristics.
   * <p>
   * It is important to understand that the event loops are asynchronous and non-blocking by nature, which means they
   * can multiplex hundreds, if not thousands of connections. It is therefore not necessary (and in some cases even
   * destructive to performance) to ramp up the number of threads to a high count (i.e. 100+). The value
   * should only really be tuned higher if profiling indicates that the current pool size is exhausted with busy work (a
   * RUNNABLE thread state alone is not indicative of this, since it might just be waiting on epoll/kqueue to be woken
   * up). If in doubt, stick with the defaults.
   * <p>
   * Note that the count provided will only be used by event loops that the SDK creates. If you configure a custom
   * event loop (i.e. through {@link #kvEventLoopGroup(EventLoopGroup)}) you are responsible for sizing it
   * appropriately on your own.
   *
   * @param eventLoopThreadCount the number of event loops to use per pool.
   * @return the {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder eventLoopThreadCount(int eventLoopThreadCount) {
    return builder().eventLoopThreadCount(eventLoopThreadCount);
  }

  /**
   * If set to false (enabled by default) will force using the java NIO based IO transport.
   * <p>
   * Usually the native transports used (epoll on linux and kqueue on OSX) are going to be faster and more efficient
   * than the generic NIO one. We recommend to only set this to false if you experience issues with the native
   * transports or instructed by couchbase support to do so for troubleshooting reasons.
   *
   * @param nativeIoEnabled if native IO should be enabled or disabled.
   * @return this {@link Builder} for chaining purposes.
   * @deprecated This method creates a new builder. Please see the deprecation notice on {@link #builder()}.
   */
  @Deprecated
  public static Builder enableNativeIo(boolean nativeIoEnabled) {
    return builder().enableNativeIo(nativeIoEnabled);
  }

  /**
   * Returns this environment as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    final Map<String, Object> export = new LinkedHashMap<>();
    export.put("nativeIoEnabled", nativeIoEnabled);
    export.put("eventLoopThreadCount", eventLoopThreadCount);

    final Set<String> eventLoopGroups = new HashSet<>();
    eventLoopGroups.add(managerEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(kvEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(queryEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(analyticsEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(searchEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(viewEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(eventingEventLoopGroup.get().getClass().getSimpleName());
    eventLoopGroups.add(backupEventLoopGroup.get().getClass().getSimpleName());
    export.put("eventLoopGroups", eventLoopGroups);

    return export;
  }

  private IoEnvironment(final Builder builder) {
    nativeIoEnabled = builder.nativeIoEnabled;
    eventLoopThreadCount = builder.eventLoopThreadCount;

    OwnedOrExternal<EventLoopGroup> httpDefaultGroup = null;
    if (builder.queryEventLoopGroup == null
      || builder.analyticsEventLoopGroup == null
      || builder.searchEventLoopGroup == null
      || builder.viewEventLoopGroup == null
      || builder.eventingEventLoopGroup == null
      || builder.backupEventLoopGroup == null) {
      httpDefaultGroup = createEventLoopGroup(nativeIoEnabled, eventLoopThreadCount, "cb-io-http");
    }

    managerEventLoopGroup = builder.managerEventLoopGroup == null
      ? createEventLoopGroup(nativeIoEnabled, 1, "cb-io-manager")
      : builder.managerEventLoopGroup;
    sanityCheckEventLoop(managerEventLoopGroup);

    kvEventLoopGroup = builder.kvEventLoopGroup == null
      ? createEventLoopGroup(nativeIoEnabled, eventLoopThreadCount, "cb-io-kv")
      : builder.kvEventLoopGroup;
    sanityCheckEventLoop(kvEventLoopGroup);

    queryEventLoopGroup = builder.queryEventLoopGroup == null
      ? httpDefaultGroup
      : builder.queryEventLoopGroup;
    sanityCheckEventLoop(queryEventLoopGroup);

    analyticsEventLoopGroup = builder.analyticsEventLoopGroup == null
      ? httpDefaultGroup
      : builder.analyticsEventLoopGroup;
    sanityCheckEventLoop(analyticsEventLoopGroup);

    searchEventLoopGroup = builder.searchEventLoopGroup == null
      ? httpDefaultGroup
      : builder.searchEventLoopGroup;
    sanityCheckEventLoop(searchEventLoopGroup);

    viewEventLoopGroup = builder.viewEventLoopGroup == null
      ? httpDefaultGroup
      : builder.viewEventLoopGroup;
    sanityCheckEventLoop(viewEventLoopGroup);

    eventingEventLoopGroup = builder.eventingEventLoopGroup == null
      ? httpDefaultGroup
      : builder.eventingEventLoopGroup;
    sanityCheckEventLoop(eventingEventLoopGroup);

    backupEventLoopGroup = builder.backupEventLoopGroup == null
      ? httpDefaultGroup
      : builder.backupEventLoopGroup;
    sanityCheckEventLoop(backupEventLoopGroup);
  }

  /**
   * Helper method to check if the event loop group is allowed with the current setup.
   *
   * @param group the group to check.
   */
  private void sanityCheckEventLoop(final OwnedOrExternal<EventLoopGroup> group) {
    if (!nativeIoEnabled && !(group.get() instanceof NioEventLoopGroup)) {
      throw InvalidArgumentException.fromMessage("Native IO is disabled and the EventLoopGroup is not a NioEventLoopGroup");
    }
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for config traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> managerEventLoopGroup() {
    return managerEventLoopGroup::get;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for Key/Value traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> kvEventLoopGroup() {
    return kvEventLoopGroup::get;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for N1QL Query traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> queryEventLoopGroup() {
    return queryEventLoopGroup::get;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for analytics traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> analyticsEventLoopGroup() {
    return analyticsEventLoopGroup::get;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for search traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> searchEventLoopGroup() {
    return searchEventLoopGroup::get;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for view traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> viewEventLoopGroup() {
    return viewEventLoopGroup::get;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for eventing traffic.
   *
   * @return the selected event loop group.
   */
  public Supplier<EventLoopGroup> eventingEventLoopGroup() {
    return eventingEventLoopGroup::get;
  }

  /**
   * Returns the {@link EventLoopGroup} to be used for backup traffic.
   *
   * @return the selected event loop group.
   */
  @Stability.Volatile
  public Supplier<EventLoopGroup> backupEventLoopGroup() {
    return backupEventLoopGroup::get;
  }

  /**
   * Returns true if native IO is enabled and can be used if supported.
   *
   * @return true if enabled.
   */
  public boolean nativeIoEnabled() {
    return nativeIoEnabled;
  }

  /**
   * Returns the thread count per event loop.
   */
  public int eventLoopThreadCount() {
    return eventLoopThreadCount;
  }

  /**
   * Instructs all the owned event loops to shut down.
   *
   * @param timeout the maximum amount of time to wait before returning back control.
   * @return a mono that completes once finished.
   */
  public Mono<Void> shutdown(final Duration timeout) {
    return Flux.merge(
      shutdownGroup(managerEventLoopGroup, timeout),
      shutdownGroup(kvEventLoopGroup, timeout),
      shutdownGroup(queryEventLoopGroup, timeout),
      shutdownGroup(analyticsEventLoopGroup, timeout),
      shutdownGroup(searchEventLoopGroup, timeout),
      shutdownGroup(viewEventLoopGroup, timeout),
      shutdownGroup(eventingEventLoopGroup, timeout),
      shutdownGroup(backupEventLoopGroup, timeout)
    ).then();
  }

  /**
   * Helper method to shut down an individual event loop group.
   *
   * @param groupSupplier the event loop group potentially to be shutdown.
   * @param timeout the maximum time to wait until shutdown abort.
   * @return a mono indicating completion.
   */
  private static Mono<Void> shutdownGroup(final OwnedOrExternal<EventLoopGroup> groupSupplier, final Duration timeout) {
    if (groupSupplier.isOwned()) {
      EventLoopGroup group = groupSupplier.get();
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
      }
    }
    return Mono.empty();
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
  private static OwnedOrExternal<EventLoopGroup> createEventLoopGroup(
    final boolean nativeIoEnabled,
    final int numThreads,
    final String poolName
  ) {
    final ThreadFactory threadFactory = new DefaultThreadFactory(poolName, true);

    if (nativeIoEnabled && Epoll.isAvailable()) {
      return owned(new EpollEventLoopGroup(numThreads, threadFactory));
    } else if (nativeIoEnabled && KQueue.isAvailable()) {
      return owned(new KQueueEventLoopGroup(numThreads, threadFactory));
    } else {
      return owned(new NioEventLoopGroup(numThreads, threadFactory));
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
    int cores = Runtime.getRuntime().availableProcessors() / 2;
    cores = Math.max(cores, 2);
    cores = Math.min(cores, 8);
    return cores;
  }

  public static class Builder {

    private boolean nativeIoEnabled = DEFAULT_NATIVE_IO_ENABLED;
    private OwnedOrExternal<EventLoopGroup> managerEventLoopGroup = null;
    private OwnedOrExternal<EventLoopGroup> kvEventLoopGroup = null;
    private OwnedOrExternal<EventLoopGroup> queryEventLoopGroup = null;
    private OwnedOrExternal<EventLoopGroup> analyticsEventLoopGroup = null;
    private OwnedOrExternal<EventLoopGroup> searchEventLoopGroup = null;
    private OwnedOrExternal<EventLoopGroup> viewEventLoopGroup = null;
    private OwnedOrExternal<EventLoopGroup> eventingEventLoopGroup = null;
    private OwnedOrExternal<EventLoopGroup> backupEventLoopGroup = null;
    private int eventLoopThreadCount = DEFAULT_EVENT_LOOP_THREAD_COUNT;

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the management service.
     * <p>
     * Note that you usually do not need to tweak the event loop for the manager service, only if you perform
     * long-running management queries that interfere with regular traffic.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder managerEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.managerEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the KV service.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder kvEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.kvEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the query service.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder queryEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.queryEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the analytics service.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder analyticsEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.analyticsEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the search service.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder searchEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.searchEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the eventing service.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder eventingEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.eventingEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the backup service.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Volatile
    public Builder backupEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.backupEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Allows to specify a custom event loop group (I/O event loop thread pool) for the view service.
     * <p>
     * <strong>Note:</strong> tweaking the dedicated event loops should be done with care and only after profiling
     * indicated that the default event loop setup is not achieving the desired performance characteristics. Please
     * see the javadoc for the {@link IoEnvironment} class for an explanation how the event loops play together for
     * all the services and what effect a custom pool might have.
     *
     * @param eventLoopGroup the dedicated event loop group to use.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder viewEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.viewEventLoopGroup = checkEventLoopGroup(eventLoopGroup);
      return this;
    }

    /**
     * Helper method to perform validation on the event loop group passed in.
     *
     * @param eventLoopGroup the event loop group to check.
     * @return the created external supplier.
     */
    private static OwnedOrExternal<EventLoopGroup> checkEventLoopGroup(final EventLoopGroup eventLoopGroup) {
      return external(notNull(eventLoopGroup, "EventLoopGroup"));
    }

    /**
     * If set to false (enabled by default) will force using the java NIO based IO transport.
     * <p>
     * Usually the native transports used (epoll on linux and kqueue on OSX) are going to be faster and more efficient
     * than the generic NIO one. We recommend to only set this to false if you experience issues with the native
     * transports or instructed by couchbase support to do so for troubleshooting reasons.
     *
     * @param nativeIoEnabled if native IO should be enabled or disabled.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enableNativeIo(final boolean nativeIoEnabled) {
      this.nativeIoEnabled = nativeIoEnabled;
      return this;
    }

    /**
     * Overrides the number of threads used per event loop.
     * <p>
     * If not manually overridden, a fair thread count is calculated, see {@link #fairThreadCount()} for more
     * information on the heuristics.
     * <p>
     * Note that the count provided will only be used by event loops that the SDK creates. If you configure a custom
     * event loop (i.e. through {@link #kvEventLoopGroup(EventLoopGroup)}) you are responsible for sizing it
     * appropriately on your own.
     *
     * @param eventLoopThreadCount the number of event loops to use per pool.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder eventLoopThreadCount(final int eventLoopThreadCount) {
      if (eventLoopThreadCount < 1) {
        throw InvalidArgumentException.fromMessage("EventLoopThreadCount cannot be smaller than 1");
      }
      this.eventLoopThreadCount = eventLoopThreadCount;
      return this;
    }

    @Stability.Internal
    public IoEnvironment build() {
      return new IoEnvironment(this);
    }

  }

}
