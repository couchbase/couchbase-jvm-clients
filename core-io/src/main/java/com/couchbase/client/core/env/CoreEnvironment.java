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

import com.couchbase.client.core.Timer;
import com.couchbase.client.core.cnc.DefaultEventBus;
import com.couchbase.client.core.cnc.DiagnosticsMonitor;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.LoggingEventConsumer;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.KeyValueServiceConfig;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@link CoreEnvironment} is an extendable, configurable and stateful
 * config designed to be passed into a core instance.
 *
 * @since 1.0.0
 */
public class CoreEnvironment {

  private static final Supplier<String> DEFAULT_USER_AGENT = () -> "foobar";
  private static final Supplier<Set<String>> DEFAULT_SEED_NODES = () ->
    new HashSet<>(Collections.singletonList("127.0.0.1"));
  private static final RetryStrategy DEFAULT_RETRY_STRATEGY = BestEffortRetryStrategy.INSTANCE;

  private final Supplier<String> userAgent;
  private final Supplier<EventBus> eventBus;
  private final Supplier<Set<String>> seedNodes;
  private final Timer timer;
  private final IoEnvironment ioEnvironment;
  private final DiagnosticsMonitor diagnosticsMonitor;
  private final Duration kvTimeout;
  private final Duration queryTimeout;
  private final Credentials credentials;
  private final MemcachedHashingStrategy memcachedHashingStrategy;
  private final RetryStrategy retryStrategy;
  private final KeyValueServiceConfig keyValueServiceConfig;

  protected CoreEnvironment(final Builder builder) {
    this.userAgent = builder.userAgent == null
      ? DEFAULT_USER_AGENT
      : builder.userAgent;
    this.eventBus = builder.eventBus == null
      ? new OwnedSupplier<>(DefaultEventBus.create())
      : builder.eventBus;
    this.timer = builder.timer == null
      ? Timer.createAndStart()
      : builder.timer;
    this.ioEnvironment = builder.ioEnvironment == null
      ? IoEnvironment.create()
      : builder.ioEnvironment;
    this.kvTimeout = builder.kvTimeout == null
      ? Duration.ofMillis(2500)
      : builder.kvTimeout;
    this.queryTimeout = builder.queryTimeout == null
      ? Duration.ofMillis(75000)
      : builder.queryTimeout;
    this.seedNodes = builder.seedNodes == null
      ? DEFAULT_SEED_NODES
      : builder.seedNodes;
    this.memcachedHashingStrategy = builder.memcachedHashingStrategy == null
      ? StandardMemcachedHashingStrategy.INSTANCE
      : builder.memcachedHashingStrategy;
    this.retryStrategy = builder.retryStrategy == null
      ? DEFAULT_RETRY_STRATEGY
      : builder.retryStrategy;
    this.keyValueServiceConfig = builder.keyValueServiceConfig == null
      ? KeyValueServiceConfig.create()
      : builder.keyValueServiceConfig;

    this.credentials = builder.credentials;

    if (this.eventBus instanceof OwnedSupplier) {
      ((DefaultEventBus) eventBus.get()).start();
    }

    // TODO: make configurable!
    eventBus.get().subscribe(LoggingEventConsumer.create());
    diagnosticsMonitor = DiagnosticsMonitor.create(eventBus.get());
    diagnosticsMonitor.start();
  }

  public static CoreEnvironment create(final String username, final String password) {
    return builder(username, password).build();
  }

  public static CoreEnvironment create(final Credentials credentials) {
    return builder(credentials).build();
  }

  public static CoreEnvironment create(final String connectionString, String username, String password) {
    return builder(connectionString, username, password).build();
  }

  public static CoreEnvironment create(final String connectionString, Credentials credentials) {
    return builder(connectionString, credentials).build();
  }

  public static CoreEnvironment.Builder builder(final String username, final String password) {
    return builder(new RoleBasedCredentials(username, password));
  }

  public static CoreEnvironment.Builder builder(final Credentials credentials) {
    return new Builder(credentials);
  }

  public static CoreEnvironment.Builder builder(final String connectionString, final String username, final String password) {
    return builder(connectionString, new RoleBasedCredentials(username, password));
  }

  public static CoreEnvironment.Builder builder(final String connectionString, final Credentials credentials) {
    return builder(credentials).load(new ConnectionStringPropertyLoader(connectionString));
  }

  public Credentials credentials() {
    return credentials;
  }

  /**
   * User agent used to identify this client against the server.
   *
   * @return the user agent as a string representation.
   */
  public String userAgent() {
    return userAgent.get();
  }

  /**
   * The central event bus which manages all kinds of messages flowing
   * throughout the client.
   *
   * @return the event bus currently in use.
   */
  public EventBus eventBus() {
    return eventBus.get();
  }

  /**
   * Holds the environmental configuration/state that is tied to the IO
   * layer.
   *
   * @return the IO environment currently in use.
   */
  public IoEnvironment ioEnvironment() {
    return ioEnvironment;
  }

  /**
   * Holds the timer which is used to schedule tasks and trigger their callback,
   * for example to time out requests.
   *
   * @return the timer used.
   */
  public Timer timer() {
    return timer;
  }

  public Duration kvTimeout() {
    return kvTimeout;
  }

  public Duration queryTimeout() {
    return queryTimeout;
  }

  public Set<String> seedNodes() {
    return seedNodes.get();
  }

  /**
   * Allows to specify a custom strategy to hash memcached bucket documents.
   *
   * @return the memcached hashing strategy.
   */
  public MemcachedHashingStrategy memcachedHashingStrategy() {
    return memcachedHashingStrategy;
  }

  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

  public KeyValueServiceConfig keyValueServiceConfig() {
    return keyValueServiceConfig;
  }

  public void shutdown(final Duration timeout) {
    shutdownAsync(timeout).block();
  }

  public Mono<Void> shutdownAsync(final Duration timeout) {
    return Mono.defer(() -> {
      // todo: implement
      diagnosticsMonitor.stop();
      return Mono.empty();
    });
  }

  public static class Builder<SELF extends Builder<SELF>> {

    private Supplier<String> userAgent = null;
    private Supplier<EventBus> eventBus = null;
    private Supplier<Set<String>> seedNodes = null;
    private Timer timer = null;
    private IoEnvironment ioEnvironment = null;
    private Duration kvTimeout = null;
    private Duration queryTimeout = null;

    private MemcachedHashingStrategy memcachedHashingStrategy;
    private RetryStrategy retryStrategy;
    private KeyValueServiceConfig keyValueServiceConfig;

    private final Credentials credentials;

    protected Builder(Credentials credentials) {
      this.credentials = credentials;
    }

    @SuppressWarnings({ "unchecked" })
    protected SELF self() {
      return (SELF) this;
    }

    public SELF userAgent(final String userAgent) {
      return userAgent(() -> userAgent);
    }

    public SELF userAgent(final Supplier<String> userAgent) {
      this.userAgent = userAgent;
      return self();
    }

    public SELF load(final PropertyLoader<Builder> loader) {
      loader.load(this);
      return self();
    }

    public SELF ioEnvironment(final IoEnvironment ioEnvironment) {
      this.ioEnvironment = ioEnvironment;
      return self();
    }

    public SELF eventBus(final EventBus eventBus) {
      return eventBus(() -> eventBus);
    }

    public SELF eventBus(final Supplier<EventBus> eventBus) {
      this.eventBus = eventBus;
      return self();
    }

    public SELF seedNodes(Supplier<Set<String>> seedNodes) {
      this.seedNodes = seedNodes;
      return self();
    }

    public SELF seedNodes(Set<String> seedNodes) {
      return seedNodes(() -> seedNodes);
    }

    public SELF kvTimeout(final Duration kvTimeout) {
      this.kvTimeout = kvTimeout;
      return self();
    }

    public SELF queryTimeout(final Duration queryTimeout) {
      this.queryTimeout = queryTimeout;
      return self();
    }


    /**
     * Allows to pass in a custom {@link Timer}.
     *
     * Note that this is advanced API! Also if a timer is passed in, it needs
     * to be started manually. If this is not done it can lead to unintended
     * consequences like requests not timing out!
     *
     * @param timer the timer to use.
     * @return this build for chaining purposes.
     */
    public SELF timer(final Timer timer) {
      this.timer = timer;
      return self();
    }

    public SELF memcachedHashingStrategy(final MemcachedHashingStrategy strategy) {
      this.memcachedHashingStrategy = strategy;
      return self();
    }

    public SELF retryStrategy(final RetryStrategy retryStrategy) {
      this.retryStrategy = retryStrategy;
      return self();
    }

    public SELF keyValueServiceConfig(final KeyValueServiceConfig config) {
      this.keyValueServiceConfig = config;
      return self();
    }

    public CoreEnvironment build() {
      return new CoreEnvironment(this);
    }
  }

}
