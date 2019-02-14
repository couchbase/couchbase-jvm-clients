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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.*;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * The {@link CoreEnvironment} is an extendable, configurable and stateful
 * config designed to be passed into a core instance.
 *
 * @since 1.0.0
 */
public class CoreEnvironment {

  /**
   * Holds the default seed nodes (going to localhost) with default ports.
   */
  public static final Set<SeedNode> DEFAULT_SEED_NODES = new HashSet<>(Collections.singletonList(
    SeedNode.create("127.0.0.1")
  ));

  /**
   * The default retry strategy used for all ops if not overridden.
   */
  public static final RetryStrategy DEFAULT_RETRY_STRATEGY = BestEffortRetryStrategy.INSTANCE;

  /**
   * Holds the user agent for this client instance.
   */
  private final UserAgent userAgent;
  private final Supplier<EventBus> eventBus;
  private final Timer timer;
  private final IoEnvironment ioEnvironment;
  private final IoConfig ioConfig;
  private final CompressionConfig compressionConfig;
  private final SecurityConfig securityConfig;
  private final TimeoutConfig timeoutConfig;
  private final ServiceConfig serviceConfig;

  private final LoggerConfig loggerConfig;
  private final DiagnosticsMonitor diagnosticsMonitor;

  private final Set<SeedNode> seedNodes;
  private final Credentials credentials;
  private final RetryStrategy retryStrategy;
  private final Supplier<Scheduler> scheduler;


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

  @SuppressWarnings({"unchecked"})
  protected CoreEnvironment(final Builder builder) {
    this.credentials = builder.credentials;
    this.userAgent = defaultUserAgent();
    this.eventBus = Optional
      .ofNullable(builder.eventBus)
      .orElse(new OwnedSupplier<>(DefaultEventBus.create()));
    this.timer = Timer.createAndStart();
    this.scheduler = Optional
      .ofNullable(builder.scheduler)
      .orElse(new OwnedSupplier<>(
        Schedulers.newParallel("cb-comp", Schedulers.DEFAULT_POOL_SIZE, true))
      );

    this.ioEnvironment = Optional.ofNullable(builder.ioEnvironment).orElse(IoEnvironment.create());
    this.ioConfig = Optional.ofNullable(builder.ioConfig).orElse(IoConfig.create());
    this.compressionConfig = Optional.ofNullable(builder.compressionConfig).orElse(CompressionConfig.create());
    this.securityConfig = Optional.ofNullable(builder.securityConfig).orElse(SecurityConfig.create());
    this.timeoutConfig = Optional.ofNullable(builder.timeoutConfig).orElse(TimeoutConfig.create());
    this.serviceConfig = Optional.ofNullable(builder.serviceConfig).orElse(ServiceConfig.create());
    this.retryStrategy = Optional.ofNullable(builder.retryStrategy).orElse(DEFAULT_RETRY_STRATEGY);
    this.loggerConfig = Optional.ofNullable(builder.loggerConfig).orElse(LoggerConfig.create());
    this.seedNodes = Optional.ofNullable(builder.seedNodes).orElse(DEFAULT_SEED_NODES);

    if (eventBus instanceof OwnedSupplier) {
      eventBus.get().start().block();
    }
    eventBus.get().subscribe(LoggingEventConsumer.create(loggerConfig()));
    diagnosticsMonitor = DiagnosticsMonitor.create(eventBus.get());
    diagnosticsMonitor.start().block();
  }

  /**
   * Helper method which grabs the title and version for the user agent from the manifest.
   *
   * @return the user agent string, in a best effort manner.
   */
  private UserAgent defaultUserAgent() {
    try {
      final Package p = agentPackage();
      String t = p.getImplementationTitle() == null ? defaultAgentTitle() : p.getImplementationTitle();
      String v = p.getImplementationVersion() == null ? "0.0.0" : p.getImplementationVersion();
      String os = String.format(
        "%s %s %s",
        System.getProperty("os.name"),
        System.getProperty("os.version"),
        System.getProperty("os.arch")
      );
      String platform = String.format(
        "%s %s",
        System.getProperty("java.vm.name"),
        System.getProperty("java.runtime.version")
      );
      return new UserAgent(t, v, Optional.of(os), Optional.of(platform));
    } catch (Throwable t) {
      return new UserAgent(defaultAgentTitle(), "0.0.0", Optional.empty(), Optional.empty());
    }
  }

  /**
   * Make sure to override this in client implementations so it picks up the right manifest.
   *
   * <p>This method should be overridden by client implementations to make sure their version
   * is included instead.</p>
   *
   * @return the package of the target application to extract properties.
   */
  protected Package agentPackage() {
    return CoreEnvironment.class.getPackage();
  }

  /**
   * Returns the default user agent name that is used as part of the resulting string.
   */
  protected String defaultAgentTitle() {
    return "java-core";
  }

  /**
   * Returns the {@link Credentials} attached to this environment.
   */
  public Credentials credentials() {
    return credentials;
  }

  /**
   * User agent used to identify this client against the server.
   */
  public UserAgent userAgent() {
    return userAgent;
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
   * Holds the environmental configuration/state that is tied to the IO layer.
   */
  public IoEnvironment ioEnvironment() {
    return ioEnvironment;
  }

  public IoConfig ioConfig() {
    return ioConfig;
  }

  public TimeoutConfig timeoutConfig() {
    return timeoutConfig;
  }

  public SecurityConfig securityConfig() {
    return securityConfig;
  }

  public ServiceConfig serviceConfig() {
    return serviceConfig;
  }

  public CompressionConfig compressionConfig() {
    return compressionConfig;
  }

  public LoggerConfig loggerConfig() {
    return loggerConfig;
  }

  public Scheduler scheduler() {
    return scheduler.get();
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

  public Set<SeedNode> seedNodes() {
    return seedNodes;
  }

  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

  /**
   * Shuts down this environment reactively.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   *
   * @param timeout the timeout to wait maximum.
   * @return a mono that completes once the shutdown is either successful or aborted.
   */
  public Mono<Void> shutdownReactive(final Duration timeout) {
    return diagnosticsMonitor.stop()
      .then(Mono.defer(() -> eventBus instanceof OwnedSupplier ? eventBus.get().stop() : Mono.empty()))
      .then(Mono.defer(() -> {
        timer.stop();
        return Mono.<Void>empty();
      }))
      .then(Mono.defer(() -> {
        if (scheduler instanceof OwnedSupplier) {
          scheduler.get().dispose();
        }
        return Mono.<Void>empty();
      }))
      .timeout(timeout);
  }

  /**
   * Shuts down this environment asynchronously.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   *
   * @param timeout the timeout to wait maximum.
   * @return a future that completes once the shutdown is either successful or aborted.
   */
  public CompletableFuture<Void> shutdownAsync(final Duration timeout) {
    return shutdownReactive(timeout).toFuture();
  }

  /**
   * Shuts down this Environment.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   *
   * @param timeout the timeout to wait maximum.
   */
  public void shutdown(final Duration timeout) {
    shutdownReactive(timeout).block();
  }

  public static class Builder<SELF extends Builder<SELF>> {

    private IoEnvironment ioEnvironment = null;
    private IoConfig ioConfig = null;
    private CompressionConfig compressionConfig = null;
    private SecurityConfig securityConfig = null;
    private TimeoutConfig timeoutConfig = null;
    private ServiceConfig serviceConfig = null;
    private LoggerConfig loggerConfig = null;
    private Supplier<EventBus> eventBus = null;
    private Supplier<Scheduler> scheduler = null;

    private Set<SeedNode> seedNodes = null;
    private RetryStrategy retryStrategy;

    private final Credentials credentials;

    protected Builder(final Credentials credentials) {
      notNull(credentials, "Credentials");
      this.credentials = credentials;
    }

    @SuppressWarnings({ "unchecked" })
    protected SELF self() {
      return (SELF) this;
    }

    public SELF load(final PropertyLoader<Builder> loader) {
      loader.load(this);
      return self();
    }

    public SELF ioEnvironment(final IoEnvironment ioEnvironment) {
      this.ioEnvironment = ioEnvironment;
      return self();
    }

    public SELF ioConfig(final IoConfig.Builder ioConfig) {
      this.ioConfig = ioConfig.build();
      return self();
    }

    public SELF compressionConfig(final CompressionConfig compressionConfig) {
      this.compressionConfig = compressionConfig;
      return self();
    }

    public SELF securityConfig(final SecurityConfig securityConfig) {
      this.securityConfig = securityConfig;
      return self();
    }

    public SELF timeoutConfig(final TimeoutConfig timeoutConfig) {
      this.timeoutConfig = timeoutConfig;
      return self();
    }

    public SELF serviceConfig(final ServiceConfig serviceConfig) {
      this.serviceConfig = serviceConfig;
      return self();
    }

    public SELF loggerConfig(final LoggerConfig loggerConfig) {
      this.loggerConfig = loggerConfig;
      return self();
    }

    @Stability.Uncommitted
    public SELF eventBus(final EventBus eventBus) {
      this.eventBus = new ExternalSupplier<>(eventBus);
      return self();
    }

    @Stability.Uncommitted
    public SELF scheduler(final Scheduler scheduler) {
      this.scheduler = new ExternalSupplier<>(scheduler);
      return self();
    }

    public SELF seedNodes(final Set<SeedNode> seedNodes) {
      this.seedNodes = seedNodes;
      return self();
    }

    public SELF retryStrategy(final RetryStrategy retryStrategy) {
      this.retryStrategy = retryStrategy;
      return self();
    }

    public CoreEnvironment build() {
      return new CoreEnvironment(this);
    }
  }

}
