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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.service.KeyValueEventLoopGroup;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The {@link IoEnvironment} holds the I/O event loops and state.
 * <p>
 * Note that this class only contains tunables and state for the I/O event loops for the Key Value service.
 * If you are looking for general configuration if I/O properties, those are located inside the
 * {@link IoConfig} class instead.
 * <p>
 * By default, the IO environment creates a KV event loop that uses a "fair" number of threads. Usually it will be half the number
 * of reported logical CPUs by the JVM, but maximum of 8 and minimum of 2. The
 *
 * @since 2.0.0
 */
public class IoEnvironment {
  /**
   * Native IO is enabled by default.
   */
  public static final boolean DEFAULT_NATIVE_IO_ENABLED = true;

  @Stability.Internal
  public static final int DEFAULT_EVENT_LOOP_THREAD_COUNT = fairThreadCount();

  private final boolean nativeIoEnabled;
  private final int eventLoopThreadCount;

  /**
   * The event loops for KV connections, once the first one opens.
   * Guarded by "this".
   */
  private @Nullable KeyValueEventLoopGroup nioKeyValueEventLoopGroup;
  private boolean nioKeyValueEventLoopGroupShutDown;

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
    return export;
  }

  private IoEnvironment(final Builder builder) {
    nativeIoEnabled = builder.nativeIoEnabled;
    eventLoopThreadCount = builder.eventLoopThreadCount;
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
   * Returns the event loops for KV connections
   * (see {@code com.couchbase.client.core.service.NioKeyValueConnection}), with {@link #eventLoopThreadCount} threads.
   * Created when first asked for, so it has no threads unless they're used.
   *
   * @throws IllegalStateException if the environment is shut down.
   */
  @Stability.Internal
  public synchronized KeyValueEventLoopGroup nioKeyValueEventLoopGroup() {
    if (nioKeyValueEventLoopGroup == null) {
      if (nioKeyValueEventLoopGroupShutDown) {
        throw new IllegalStateException("The IoEnvironment is shut down");
      }
      nioKeyValueEventLoopGroup = new KeyValueEventLoopGroup(eventLoopThreadCount);
    }
    return nioKeyValueEventLoopGroup;
  }

  /**
   * Instructs all the owned event loops to shut down.
   *
   * @param timeout the maximum amount of time to wait before returning back control.
   * @return a mono that completes once finished.
   */
  public Mono<Void> shutdown(final Duration timeout) {
    return shutdownNioKeyValueEventLoopGroup(timeout);
  }

  private Mono<Void> shutdownNioKeyValueEventLoopGroup(final Duration timeout) {
    return Mono.defer(() -> {
      KeyValueEventLoopGroup group;
      synchronized (this) {
        nioKeyValueEventLoopGroupShutDown = true;
        group = nioKeyValueEventLoopGroup;
      }
      return group == null ? Mono.empty() : group.shutdown(timeout);
    });
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

    private int eventLoopThreadCount = DEFAULT_EVENT_LOOP_THREAD_COUNT;
    private boolean nativeIoEnabled = DEFAULT_NATIVE_IO_ENABLED;

    /**
     * Controls whether the SDK attempts to load native libraries that assist with I/O.
     * <p>
     * Specifically, this affects whether the SDK will attempt to set the TCP_USER_TIMEOUT socket option
     * when the OS supports it.
     * <p>
     * Default value: true
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
