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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Timer;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.cnc.DefaultEventBus;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.LoggingEventConsumer;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.OrphanReporter;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.events.config.HighIdleHttpConnectionTimeoutConfiguredEvent;
import com.couchbase.client.core.cnc.events.config.InsecureSecurityConfigDetectedEvent;
import com.couchbase.client.core.cnc.metrics.LoggingMeter;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.cnc.tracing.NoopRequestTracer;
import com.couchbase.client.core.cnc.tracing.ThresholdLoggingTracer;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.AbstractPooledEndpointServiceConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions;
import com.couchbase.client.core.transaction.util.CoreTransactionsSchedulers;
import com.couchbase.client.core.util.ReactorOps;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.couchbase.client.core.env.OwnedOrExternal.external;
import static com.couchbase.client.core.env.OwnedOrExternal.owned;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * The Environment is the main place in the SDK where configuration and state lives (i.e. I/O pools).
 * <p>
 * Note that unless you are using the core directly, you want to consider the child implementations for each
 * language binding (i.e. the ClusterEnvironment for the java client).
 */
public class CoreEnvironment implements ReactorOps, AutoCloseable {
  private static final VersionAndGitHash coreVersion = VersionAndGitHash.from(Core.class);

  private static final String CORE_AGENT_TITLE = "java-core";
  /**
   * Default maximum requests being queued in retry before performing backpressure cancellations.
   */
  public static final long DEFAULT_MAX_NUM_REQUESTS_IN_RETRY = 32768;

  /**
   * Service Loader for Environment Profiles.
   */
  private static final ServiceLoader<ConfigurationProfile> environmentProfileLoader =
    ServiceLoader.load(ConfigurationProfile.class);

  /**
   * The default retry strategy used for all ops if not overridden.
   */
  private static final RetryStrategy DEFAULT_RETRY_STRATEGY = BestEffortRetryStrategy.INSTANCE;

  private final UserAgent userAgent;
  private final OwnedOrExternal<EventBus> eventBus;
  private final Timer timer;
  private final IoEnvironment ioEnvironment;
  private final IoConfig ioConfig;
  private final CompressionConfig compressionConfig;
  private final SecurityConfig securityConfig;
  private final TimeoutConfig timeoutConfig;
  private final OrphanReporterConfig orphanReporterConfig;
  private final ThresholdLoggingTracerConfig thresholdLoggingTracerConfig;
  private final LoggingMeterConfig loggingMeterConfig;
  private final OwnedOrExternal<RequestTracer> requestTracer;
  private final OwnedOrExternal<Meter> meter;
  private final LoggerConfig loggerConfig;
  private final RetryStrategy retryStrategy;
  private final OwnedOrExternal<Scheduler> scheduler;
  private final OwnedOrExternal<Executor> executor;
  @Nullable private final Supplier<Scheduler> userScheduler;
  private final int schedulerThreadCount;
  private final OrphanReporter orphanReporter;
  private final long maxNumRequestsInRetry;
  private final List<RequestCallback> requestCallbacks;
  private final CoreTransactionsConfig transactionsConfig;

  private final Set<String> appliedProfiles;
  private final CoreTransactionsSchedulers transactionsSchedulers = new CoreTransactionsSchedulers();
  private final @Nullable String preferredServerGroup;

  public static CoreEnvironment create() {
    return builder().build();
  }

  public static CoreEnvironment.Builder<?> builder() {
    return new Builder<>();
  }

  protected CoreEnvironment(final Builder<?> builder) {
    this.userAgent = defaultUserAgent();
    this.maxNumRequestsInRetry = builder.maxNumRequestsInRetry;
    this.schedulerThreadCount = builder.schedulerThreadCount;
    this.scheduler = Optional
      .ofNullable(builder.scheduler)
      .orElse(owned(
        Schedulers.newParallel("cb-comp", schedulerThreadCount, true))
      );
    this.userScheduler = builder.userScheduler;

    // JVMCBC-1196: configuration options for the executor will be provided.
    String executorMaxThreadCountRaw = System.getProperty("com.couchbase.protostellar.executorMaxThreadCount");
    int maxThreadCount = Runtime.getRuntime().availableProcessors();
    if (executorMaxThreadCountRaw != null) {
      maxThreadCount = Integer.parseInt(executorMaxThreadCountRaw);
    }

    this.executor = owned(new ThreadPoolExecutor(0, maxThreadCount,
      60L, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>(),
      new CouchbaseThreadFactory("cb-exec")));

    this.eventBus = Optional
      .ofNullable(builder.eventBus)
      .orElse(owned(DefaultEventBus.create(scheduler.get())));
    this.timer = Timer.createAndStart(maxNumRequestsInRetry, builder.ioConfig.timerConfig().build());


    this.securityConfig = builder.securityConfig.build();

    this.ioEnvironment = builder.ioEnvironment.build();
    this.ioConfig = builder.ioConfig.build();
    this.compressionConfig = builder.compressionConfig.build();
    this.timeoutConfig = builder.timeoutConfig.build();
    this.retryStrategy = Optional.ofNullable(builder.retryStrategy).orElse(DEFAULT_RETRY_STRATEGY);
    this.loggerConfig = builder.loggerConfig.build();
    this.orphanReporterConfig = builder.orphanReporterConfig.build();
    this.thresholdLoggingTracerConfig = builder.thresholdLoggingTracerConfig.build();
    this.loggingMeterConfig = builder.loggingMeterConfig.build();
    this.appliedProfiles = builder.appliedProfiles;
    // This is expected to be non-null in all production deployments, as we need at least the list of supported extensions,
    // but cannot assert it here due to affecting too many core tests, and potentially end users of this class.
    this.transactionsConfig = builder.transactionsConfig != null
            ? builder.transactionsConfig
            : CoreTransactionsConfig.createDefault(CoreTransactionsSupportedExtensions.ALL);

    if (eventBus.isOwned()) {
      eventBus.get().start().block();
    }
    eventBus.get().subscribe(LoggingEventConsumer.create(loggerConfig()));

    this.requestTracer = Optional.ofNullable(builder.requestTracer).orElse(owned(
      thresholdLoggingTracerConfig.enabled()
        ? ThresholdLoggingTracer.create(eventBus.get(), thresholdLoggingTracerConfig)
        : NoopRequestTracer.INSTANCE
    ));

    if (requestTracer.isOwned()) {
      requestTracer.get().start().block();
    }

    this.meter = Optional.ofNullable(builder.meter).orElse(owned(
      loggingMeterConfig.enabled()
        ? LoggingMeter.create(eventBus.get(), loggingMeterConfig)
        : NoopMeter.INSTANCE
    ));

    if (meter.isOwned()) {
      meter.get().start().block();
    }

    orphanReporter = new OrphanReporter(eventBus.get(), orphanReporterConfig);
    orphanReporter.start().block();

    if (ioConfig.idleHttpConnectionTimeout().toMillis() > AbstractPooledEndpointServiceConfig.DEFAULT_IDLE_TIME.toMillis()) {
      eventBus.get().publish(new HighIdleHttpConnectionTimeoutConfiguredEvent());
    }

    this.requestCallbacks = Collections.unmodifiableList(builder.requestCallbacks);
    this.preferredServerGroup = builder.preferredServerGroup;

    checkInsecureTlsConfig();
  }

  /**
   * Helper method to check for insecure TLS settings and emit an event to notify users.
   */
  private void checkInsecureTlsConfig() {
    if (securityConfig.tlsEnabled()) {
      boolean validateHosts = securityConfig.hostnameVerificationEnabled();
      boolean insecureTrustManager = securityConfig.trustManagerFactory() instanceof InsecureTrustManagerFactory;

      if (!validateHosts || insecureTrustManager) {
        eventBus.get().publish(new InsecureSecurityConfigDetectedEvent(validateHosts, insecureTrustManager));
      }
    }
  }

  private UserAgent defaultUserAgent() {
    try {
      String os = String.join(" ",
        System.getProperty("os.name"),
        System.getProperty("os.version"),
        System.getProperty("os.arch")
      );
      String platform = String.join(" ",
        System.getProperty("java.vm.name"),
        System.getProperty("java.runtime.version")
      );
      return new UserAgent(defaultAgentTitle(), clientVersion(), Optional.of(os), Optional.of(platform));
    } catch (Throwable t) {
      return new UserAgent(defaultAgentTitle(), clientVersion(), Optional.empty(), Optional.empty());
    }
  }

  /**
   * Returns the default user agent name that is used as part of the resulting string.
   */
  protected String defaultAgentTitle() {
    return CORE_AGENT_TITLE;
  }

  /**
   * Subclasses should override to return version info for the "wrapper" (language-specific SDK).
   */
  protected VersionAndGitHash clientVersionAndGitHash() {
    return VersionAndGitHash.UNKNOWN;
  }

  /**
   * If present, returns the git hash for the client at build time.
   */
  public Optional<String> clientHash() {
    return Optional.of(clientVersionAndGitHash().gitHash());
  }

  /**
   * If present, returns the git hash for the core at build time.
   */
  public Optional<String> coreHash() {
    return Optional.of(coreVersion.gitHash());
  }

  /**
   * If present, returns the client version at build time.
   */
  public Optional<String> clientVersion() {
    return Optional.of(clientVersionAndGitHash().version());
  }

  /**
   * If present, returns the core version at build time.
   */
  public Optional<String> coreVersion() {
    return Optional.of(coreVersion.version());
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

  /**
   * Returns the current configuration for all I/O-related settings.
   */
  public IoConfig ioConfig() {
    return ioConfig;
  }

  /**
   * Returns the configuration for all default timeouts.
   */
  public TimeoutConfig timeoutConfig() {
    return timeoutConfig;
  }

  /**
   * Returns the current security configuration (TLS etc.).
   */
  public SecurityConfig securityConfig() {
    return securityConfig;
  }

  /**
   * Returns the current compression configuration.
   */
  public CompressionConfig compressionConfig() {
    return compressionConfig;
  }

  /**
   * Returns the current logger configuration.
   */
  public LoggerConfig loggerConfig() {
    return loggerConfig;
  }

  /**
   * Returns the scheduler used to schedule reactive, async tasks across the SDK.
   */
  public Scheduler scheduler() {
    return scheduler.get();
  }

  /**
   * Returns the supplier for the scheduler where Reactive API results should be published,
   * or null if the user does not want to switch schedulers.
   */
  @Stability.Internal
  @Nullable
  public Supplier<Scheduler> userScheduler() {
    return userScheduler;
  }

  @Stability.Internal
  public <T> Mono<T> publishOnUserScheduler(Mono<T> mono) {
    return userScheduler == null ? mono : Mono.defer(() -> mono.publishOn(userScheduler.get()));
  }

  @Stability.Internal
  public <T> Flux<T> publishOnUserScheduler(Flux<T> flux) {
    return userScheduler == null ? flux : Flux.defer(() -> flux.publishOn(userScheduler.get()));
  }

  /**
   * Returns the executor used to schedule non-reactive async tasks across the SDK.
   */
  @Stability.Internal
  public Executor executor() {
    return executor.get();
  }

  /**
   * Returns the request tracer for response time observability.
   * <p>
   * Note that this right now is unsupported, volatile API and subject to change!
   *
   * @deprecated consumers should use {@link com.couchbase.client.core.CoreResources} instead, as the RequestTracer returned
   * from that adds useful additional spans that are specific to a Core, and cannot be added here (as this class can be
   * shared between Clusters).
   */
  @Stability.Volatile
  @Deprecated
  public RequestTracer requestTracer() {
    return requestTracer.get();
  }

  @Stability.Volatile
  public Meter meter() {
    return meter.get();
  }

  @Stability.Internal
  public List<RequestCallback> requestCallbacks() {
    return requestCallbacks;
  }

  /**
   * Returns the timer used to schedule timeouts and retries amongst other tasks.
   */
  public Timer timer() {
    return timer;
  }

  /**
   * Returns the retry strategy on this environment.
   */
  public RetryStrategy retryStrategy() {
    return retryStrategy;
  }

  /**
   * Returns the orphan reporter on this environment.
   */
  public OrphanReporter orphanReporter() {
    return orphanReporter;
  }

  /**
   * Returns the maximum number of requests allowed in retry, before no more ops are allowed and canceled.
   */
  public long maxNumRequestsInRetry() {
    return maxNumRequestsInRetry;
  }

  @Stability.Volatile
  public CoreTransactionsConfig transactionsConfig() {
    return transactionsConfig;
  }

  /**
   * The schedulers used for any transactional operations.
   */
  @Stability.Volatile
  public CoreTransactionsSchedulers transactionsSchedulers() {
    return transactionsSchedulers;
  }

  /**
   * The preferred server group to use for operations that support such.
   */
  public @Nullable String preferredServerGroup() {
    return preferredServerGroup;
  }

  /**
   * Shuts down this Environment with the default disconnect timeout.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   */
  public CompletableFuture<Void> shutdownAsync() {
    return shutdownAsync(timeoutConfig.disconnectTimeout());
  }

  /**
   * Shuts down this Environment with a custom timeout.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   *
   * @param timeout the timeout to wait maximum.
   */
  public CompletableFuture<Void> shutdownAsync(final Duration timeout) {
    return shutdownReactive(timeout).toFuture();
  }

  /**
   * Shuts down this Environment with the default disconnect timeout.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   */
  public Mono<Void> shutdownReactive() {
    return shutdownReactive(timeoutConfig.disconnectTimeout());
  }

  /**
   * Shuts down this Environment with a custom timeout.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   *
   * @param timeout the timeout to wait maximum.
   */
  public Mono<Void> shutdownReactive(final Duration timeout) {
    return Mono.defer(() -> eventBus.isOwned() ? eventBus.get().stop(timeout) : Mono.empty())
      .then(Mono.defer(() -> {
        timer.stop();
        return Mono.empty();
      }))
      .then(ioEnvironment.shutdown(timeout))
      .then(Mono.defer(() -> {
        if (requestTracer.isOwned()) {
          return requestTracer.get().stop(timeout);
        }
        return Mono.empty();
      }))
      .then(Mono.defer(() -> {
        if (meter.isOwned()) {
          return meter.get().stop(timeout);
        }
        return Mono.empty();
      }))
      .then(Mono.defer(orphanReporter::stop))
      .then(Mono.defer(() -> {
        if (scheduler.isOwned()) {
          scheduler.get().dispose();
        }
        return Mono.empty();
      }))
      .then(Mono.defer(() -> {
        if (executor.isOwned()) {
          if (executor.get() instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor.get()).shutdown();
          }
          else throw new IllegalStateException("Unknown but owned executor type");
        }
        return Mono.empty();
      }))
      .then(Mono.fromRunnable(() -> transactionsSchedulers().shutdown()))
      .then()
      .timeout(timeout); // this timeout cannot be on our scheduler, since our scheduler is already shut down
  }

  /**
   * Shuts down this Environment with a custom timeout.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   *
   * @param timeout the timeout to wait maximum.
   */
  public void shutdown(final Duration timeout) {
    shutdownReactive(timeout).block();
  }

  /**
   * Shuts down this Environment with the default disconnect timeout.
   *
   * <p>Note that once shutdown, the environment cannot be restarted so it is advised to perform this operation
   * at the very last operation in the SDK shutdown process.</p>
   */
  public void shutdown() {
    shutdown(timeoutConfig.disconnectTimeout());
  }

  /**
   * Export this environment into the specified format.
   *
   * @param format the format to export into.
   * @return the exported format as a string representation.
   */
  public String exportAsString(final Context.ExportFormat format) {
    Map<String, Object> input = new LinkedHashMap<>();

    input.put("clientVersion", clientVersion().orElse(null));
    input.put("clientGitHash", clientHash().orElse(null));
    input.put("coreVersion", coreVersion().orElse(null));
    input.put("coreGitHash", coreHash().orElse(null));

    input.put("userAgent", userAgent.formattedLong());
    input.put("maxNumRequestsInRetry", maxNumRequestsInRetry);

    input.put("ioEnvironment", ioEnvironment.exportAsMap());
    input.put("ioConfig", ioConfig.exportAsMap());
    input.put("compressionConfig", compressionConfig.exportAsMap());
    input.put("securityConfig", securityConfig.exportAsMap());
    input.put("timeoutConfig", timeoutConfig.exportAsMap());
    input.put("loggerConfig", loggerConfig.exportAsMap());
    input.put("orphanReporterConfig", orphanReporterConfig.exportAsMap());
    input.put("thresholdLoggingTracerConfig", thresholdLoggingTracerConfig.exportAsMap());
    input.put("loggingMeterConfig", loggingMeterConfig.exportAsMap());

    input.put("retryStrategy", retryStrategy.getClass().getSimpleName());
    input.put("requestTracer", requestTracer.get().toString());
    input.put("meter", meter.get().getClass().getSimpleName());
    input.put("numRequestCallbacks", requestCallbacks.size());
    input.put("scheduler", scheduler.get().getClass().getSimpleName());
    input.put("schedulerThreadCount", schedulerThreadCount);

    input.put("transactionsConfig", transactionsConfig.exportAsMap());
    if (!appliedProfiles.isEmpty()) {
      input.put("profiles", appliedProfiles);
    }

    return format.apply(input);
  }

  @Override
  public String toString() {
    return exportAsString(Context.ExportFormat.STRING);
  }

  @Override
  public void close() {
    shutdown();
  }

  public static class Builder<SELF extends Builder<SELF>> {

    private IoEnvironment.Builder ioEnvironment = new IoEnvironment.Builder();
    private IoConfig.Builder ioConfig = new IoConfig.Builder();
    private CompressionConfig.Builder compressionConfig = new CompressionConfig.Builder();
    private SecurityConfig.Builder securityConfig = new SecurityConfig.Builder();
    private TimeoutConfig.Builder timeoutConfig = new TimeoutConfig.Builder();
    private LoggerConfig.Builder loggerConfig = new LoggerConfig.Builder();
    private OrphanReporterConfig.Builder orphanReporterConfig = new OrphanReporterConfig.Builder();
    private ThresholdLoggingTracerConfig.Builder thresholdLoggingTracerConfig = new ThresholdLoggingTracerConfig.Builder();
    private LoggingMeterConfig.Builder loggingMeterConfig = new LoggingMeterConfig.Builder();
    private OwnedOrExternal<EventBus> eventBus = null;
    private OwnedOrExternal<Scheduler> scheduler = null;
    private Supplier<Scheduler> userScheduler = null;
    private int schedulerThreadCount = Schedulers.DEFAULT_POOL_SIZE;
    private OwnedOrExternal<RequestTracer> requestTracer = null;
    private OwnedOrExternal<Meter> meter = null;
    private RetryStrategy retryStrategy = null;
    private long maxNumRequestsInRetry = DEFAULT_MAX_NUM_REQUESTS_IN_RETRY;
    private final List<RequestCallback> requestCallbacks = new ArrayList<>();
    protected CoreTransactionsConfig transactionsConfig = null;
    private String preferredServerGroup = null;

    private final Set<String> appliedProfiles = new LinkedHashSet<>();

    protected Builder() { }

    /**
     * Passes the {@link IoEnvironment.Builder} to the provided consumer.
     * <p>
     * Allows customizing I/O thread pools and other resources.
     * For more I/O settings, see: {@link #ioConfig(Consumer)}
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF ioEnvironment(final Consumer<IoEnvironment.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.ioEnvironment);
      return self();
    }

    /**
     * Passes the {@link IoConfig.Builder} to the provided consumer.
     * <p>
     * This is the main way to control how the SDK behaves at the lower levels.
     * It allows customizing settings such as tcp keepalive, number of connections, circuit breakers, etc.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF ioConfig(final Consumer<IoConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.ioConfig);
      return self();
    }

    /**
     * Passes the {@link CompressionConfig.Builder} to the provided consumer.
     * <p>
     * Allows customizing document value compression settings.
     * <p>
     * Usually this does not need to be tuned, but thresholds can be modified
     * or compression can be disabled completely if needed.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF compressionConfig(final Consumer<CompressionConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.compressionConfig);
      return self();
    }

    /**
     * Passes the {@link SecurityConfig.Builder} to the provided consumer.
     * <p>
     * Allows configuring everything related to TLS/encrypted connections.
     * <p>
     * Note that if you are looking to use client certificate authentication,
     * please refer to the {@link CertificateAuthenticator} instead.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF securityConfig(final Consumer<SecurityConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.securityConfig);
      return self();
    }

    /**
     * Passes the {@link TimeoutConfig.Builder} to the provided consumer.
     * <p>
     * Allows customizing the default timeouts for all operations.
     * <p>
     * The default timeout can be overridden on a per-request basis
     * using the "timeout" property of the request's option block.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF timeoutConfig(final Consumer<TimeoutConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.timeoutConfig);
      return self();
    }

    /**
     * Passes the {@link LoggerConfig.Builder} to the provided consumer.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF loggerConfig(final Consumer<LoggerConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.loggerConfig);
      return self();
    }

    /**
     * Passes the {@link OrphanReporterConfig.Builder} to the provided consumer.
     * <p>
     * Allows customizing the behavior of the orphan response reporter.
     * <p>
     * The orphan reporter logs all responses that arrived when the requesting side is not listening anymore (usually
     * because of a timeout). The config can be modified to tune certain properties like the sample size or the emit
     * interval.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF orphanReporterConfig(final Consumer<OrphanReporterConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.orphanReporterConfig);
      return self();
    }

    /**
     * Passes the {@link ThresholdLoggingTracerConfig.Builder} to the provided consumer.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF thresholdLoggingTracerConfig(final Consumer<ThresholdLoggingTracerConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.thresholdLoggingTracerConfig);
      return self();
    }

    /**
     * Passes the {@link LoggingMeterConfig.Builder} to the provided consumer.
     *
     * @param builderConsumer configures the builder.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF loggingMeterConfig(final Consumer<LoggingMeterConfig.Builder> builderConsumer) {
      notNull(builderConsumer, "BuilderConsumer").accept(this.loggingMeterConfig);
      return self();
    }

    @SuppressWarnings("unchecked")
    protected SELF self() {
      return (SELF) this;
    }

    /**
     * Allows to customize the maximum number of requests allowed in the retry timer.
     * <p>
     * If the {@link #DEFAULT_MAX_NUM_REQUESTS_IN_RETRY} is reached, each request that would be queued for retry is
     * instead cancelled with a {@link CancellationReason#TOO_MANY_REQUESTS_IN_RETRY}. This acts as a form of
     * safety net and backpressure.
     *
     * @param maxNumRequestsInRetry the maximum number of requests outstanding for retry.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF maxNumRequestsInRetry(final long maxNumRequestsInRetry) {
      if (maxNumRequestsInRetry < 0) {
        throw InvalidArgumentException.fromMessage("maxNumRequestsInRetry cannot be negative");
      }
      this.maxNumRequestsInRetry = maxNumRequestsInRetry;
      return self();
    }

    /**
     * Immediately loads the properties from the given loader into the environment.
     *
     * @param loader the loader to load the properties from.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF load(final PropertyLoader<Builder> loader) {
      notNull(loader, "PropertyLoader");
      loader.load(this);
      return self();
    }

    /**
     * Allows to customize I/O thread pools.
     * <p>
     * Note that the {@link IoEnvironment} holds thread pools and other resources. If you do not want to customize
     * thread pool sizes, you likely want to look at the {@link IoConfig} instead.
     *
     * @param ioEnvironment the IO environment to customize.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #ioEnvironment(Consumer)} instead.
     */
    @Deprecated
    public SELF ioEnvironment(final IoEnvironment.Builder ioEnvironment) {
      this.ioEnvironment = notNull(ioEnvironment, "IoEnvironment");
      return self();
    }

    /**
     * Returns the currently stored IoEnvironment builder.
     *
     * @return the current builder.
     * @deprecated Please use {@link #ioEnvironmentConfig()} instead.
     */
    @Deprecated
    public IoEnvironment.Builder ioEnvironment() {
      return ioEnvironmentConfig();
    }

    /**
     * Returns the currently stored IoEnvironment builder.
     *
     * @return the current builder.
     */
    public IoEnvironment.Builder ioEnvironmentConfig() {
      return ioEnvironment;
    }

    /**
     * Allows to customize various I/O-related configuration properties.
     * <p>
     * The I/O config is the main way to control how the SDK behaves at the lower levels. It allows to customize
     * properties such as tcp keepalive, number of connections, circuit breakers, etc.
     *
     * @param ioConfig the custom I/O config to use.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #ioConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF ioConfig(final IoConfig.Builder ioConfig) {
      this.ioConfig = notNull(ioConfig, "IoConfig");
      return self();
    }

    /**
     * Returns the currently stored config builder.
     *
     * @return the current builder.
     */
    public IoConfig.Builder ioConfig() {
      return ioConfig;
    }

    /**
     * Allows to customize the behavior of the orphan response reporter.
     * <p>
     * The orphan reporter logs all responses that arrived when the requesting side is not listening anymore (usually
     * because of a timeout). The config can be modified to tune certain properties like the sample size or the emit
     * interval.
     *
     * @param orphanReporterConfig the custom orphan reporter config.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #orphanReporterConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF orphanReporterConfig(final OrphanReporterConfig.Builder orphanReporterConfig) {
      this.orphanReporterConfig = notNull(orphanReporterConfig, "OrphanReporterConfig");
      return self();
    }

    /**
     * Returns the currently stored config builder.
     *
     * @return the current builder.
     */
    public OrphanReporterConfig.Builder orphanReporterConfig() {
      return orphanReporterConfig;
    }

    /**
     * @deprecated This method clobbers any previously configured values. Please use {@link #loggingMeterConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF loggingMeterConfig(final LoggingMeterConfig.Builder loggingMeterConfig) {
      this.loggingMeterConfig = notNull(loggingMeterConfig, "LoggingMeterConfig");
      return self();
    }

    public LoggingMeterConfig.Builder loggingMeterConfig() {
      return loggingMeterConfig;
    }

    /**
     * Allows to customize the threshold request tracer configuration.
     *
     * @param thresholdRequestTracerConfig the configuration which should be used.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #thresholdLoggingTracerConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF thresholdRequestTracerConfig(final ThresholdRequestTracerConfig.Builder thresholdRequestTracerConfig) {
      this.thresholdLoggingTracerConfig = notNull(thresholdRequestTracerConfig, "ThresholdRequestTracerConfig")
        .toNewBuillder();
      return self();
    }

    @Deprecated
    public ThresholdRequestTracerConfig.Builder thresholdRequestTracerConfig() {
      return ThresholdRequestTracerConfig.Builder.fromNewBuilder(thresholdLoggingTracerConfig);
    }

    /**
     * Allows to customize the threshold request tracer configuration.
     *
     * @param thresholdLoggingTracerConfig the configuration which should be used.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #thresholdLoggingTracerConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF thresholdLoggingTracerConfig(final ThresholdLoggingTracerConfig.Builder thresholdLoggingTracerConfig) {
      this.thresholdLoggingTracerConfig = notNull(thresholdLoggingTracerConfig, "ThresholdLoggingTracerConfig");
      return self();
    }

    /**
     * Specifies the supplier the SDK uses to get the Scheduler for publishing Reactive API results.
     * <p>
     * Defaults to null, which means reactive results are published immediately
     * in a thread owned by the SDK -- typically the SDK's Netty event loop.
     * <p>
     * The supplier is invoked once for every subscription, by the same thread that subscribes to the Mono/Flux.
     *
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Volatile
    public SELF publishOnScheduler(@Nullable final Supplier<Scheduler> publishOnScheduler) {
      this.userScheduler = publishOnScheduler;
      return self();
    }

    public ThresholdLoggingTracerConfig.Builder thresholdLoggingTracerConfig() {
      return thresholdLoggingTracerConfig;
    }

    /**
     * Allows to customize document value compression settings.
     * <p>
     * Usually this does not need to be tuned, but thresholds can be modified or compression can be disabled
     * completely if needed.
     *
     * @param compressionConfig the custom compression config.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #compressionConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF compressionConfig(final CompressionConfig.Builder compressionConfig) {
      this.compressionConfig = notNull(compressionConfig, "CompressionConfig");
      return self();
    }

    /**
     * Returns the currently stored config builder.
     *
     * @return the current builder.
     */
    public CompressionConfig.Builder compressionConfig() {
      return compressionConfig;
    }

    /**
     * Allows to configure everything related to TLS/encrypted connections.
     * <p>
     * Note that if you are looking to use client certificate authentication, please refer to the
     * {@link CertificateAuthenticator} instead.
     *
     * @param securityConfig the custom security config to use.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #securityConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF securityConfig(final SecurityConfig.Builder securityConfig) {
      this.securityConfig = notNull(securityConfig, "SecurityConfig");
      return self();
    }

    /**
     * Returns the currently stored config builder.
     *
     * @return the current builder.
     */
    public SecurityConfig.Builder securityConfig() {
      return securityConfig;
    }

    /**
     * Allows to customize the default timeouts for all operations.
     * <p>
     * Each timeout can also be modified on a per-request basis in their respective options blocks.
     *
     * @param timeoutConfig the custom timeout config to use.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #timeoutConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF timeoutConfig(final TimeoutConfig.Builder timeoutConfig) {
      this.timeoutConfig = notNull(timeoutConfig, "TimeoutConfig");
      return self();
    }

    /**
     * Returns the currently stored config builder.
     *
     * @return the current builder.
     */
    public TimeoutConfig.Builder timeoutConfig() {
      return this.timeoutConfig;
    }

    /**
     * Allows to provide a custom configuration for the default logger used.
     * <p>
     * The default logger attaches itself to the {@link EventBus} on the environment and logs consumed events. This
     * configuration allows to customize its behavior, diagnostic context etc.
     *
     * @param loggerConfig the custom logger config to use.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #loggerConfig(Consumer)} instead.
     */
    @Deprecated
    public SELF loggerConfig(final LoggerConfig.Builder loggerConfig) {
      this.loggerConfig = notNull(loggerConfig, "LoggerConfig");
      return self();
    }

    /**
     * Returns the currently stored config builder.
     *
     * @return the current builder.
     */
    public LoggerConfig.Builder loggerConfig() {
      return loggerConfig;
    }

    /**
     * Customizes the event bus for the SDK.
     * <p>
     * The SDK ships with a high-performance implementation of a event bus. Only swap out if you have special needs,
     * usually what you want instead is to register your own consumer on the event bus instead
     * ({@link EventBus#subscribe(Consumer)})!
     *
     * @param eventBus the event bus to use.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Uncommitted
    public SELF eventBus(final EventBus eventBus) {
      this.eventBus = external(notNull(eventBus, "EventBus"));
      return self();
    }

    /**
     * Customizes the default Reactor scheduler used for parallel operations.
     * <p>
     * Usually you do not need to modify the scheduler, use with care.
     *
     * @param scheduler a custom scheduler to use.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Uncommitted
    public SELF scheduler(final Scheduler scheduler) {
      this.scheduler = external(notNull(scheduler, "Scheduler"));
      return self();
    }

    /**
     * Customizes the thread count for the {@link #scheduler()} if not customized by {@link #scheduler(Scheduler)}.
     * <p>
     * By default, {@link Schedulers#DEFAULT_POOL_SIZE} is used. Note that if the scheduler itself is customized, this
     * value is ignored.
     *
     * @param schedulerThreadCount the number of threads to use for the scheduler.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Uncommitted
    public SELF schedulerThreadCount(final int schedulerThreadCount) {
      if (schedulerThreadCount < 1) {
        throw InvalidArgumentException.fromMessage("SchedulerThreadCount cannot be smaller than 1");
      }
      this.schedulerThreadCount = schedulerThreadCount;
      return self();
    }

    /**
     * Allows to customize the default retry strategy.
     * <p>
     * Note that this setting modifies the SDK-wide retry strategy. It can still be overridden on a per-request
     * basis in the respective options block.
     *
     * @param retryStrategy the default retry strategy to use for all operations.
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF retryStrategy(final RetryStrategy retryStrategy) {
      this.retryStrategy = notNull(retryStrategy, "RetryStrategy");
      return self();
    }

    /**
     * Allows to configure a custom tracer implementation.
     * <p>
     * <strong>IMPORTANT:</strong> this is a volatile, likely to change API!
     *
     * @param requestTracer the custom request tracer to use.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Volatile
    public SELF requestTracer(final RequestTracer requestTracer) {
      notNull(requestTracer, "RequestTracer");

      this.requestTracer = external(requestTracer);

      return self();
    }

    /**
     * Allows to configure a custom metrics implementation.
     * <p>
     * <strong>IMPORTANT:</strong> this is a volatile, likely to change API!
     *
     * @param meter the custom metrics implementation to use.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Volatile
    public SELF meter(final Meter meter) {
      this.meter = external(notNull(meter, "Meter"));
      return self();
    }

    /**
     * Allows configuring the transactions config.
     *
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Internal
    public SELF transactionsConfig(final CoreTransactionsConfig transactionsConfig) {
      this.transactionsConfig = transactionsConfig;
      return self();
    }

    /**
     * Allows to configure callbacks across the lifetime of a request.
     * <p>
     * <strong>IMPORTANT:</strong> this is internal API and might change at any point in time.
     * @param requestCallback the callback to use.
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Internal
    public SELF addRequestCallback(final RequestCallback requestCallback) {
      this.requestCallbacks.add(notNull(requestCallback, "RequestCallback"));
      return self();
    }

    /**
     * Turns this builder into a real {@link CoreEnvironment}.
     *
     * @return the created core environment.
     */
    public CoreEnvironment build() {
      return new CoreEnvironment(this);
    }

    /**
     * Applies custom properties based on a profile name.
     * <p>
     * At the moment only the "wan-development" profile is supported, but its actual values are not determined yet.
     * Since this is volatile API, the actual profile names and their properties are subject to change.
     * <p>
     * New profiles can be registered by utilizing the ServiceRegistry mechanism. Create a file with the name of
     * "com.couchbase.client.core.env.ConfigurationProfile" in your META-INF/services folder and the content contains
     * each line of classes that implement the "EnvironmentProfile" interface. See the {@link WanDevelopmentProfile} for
     * examples and usage.
     *
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Volatile
    public SELF applyProfile(final String profileName)  {
      notNullOrEmpty(profileName, "ProfileName");

      for (ConfigurationProfile profile : environmentProfileLoader) {
        if (profile.name().equals(profileName)) {
          appliedProfiles.add(profileName);
          return load(PropertyLoader.fromMap(profile.properties()));
        }
      }

      throw InvalidArgumentException.fromMessage("Unknown profile: '" + profileName + "', valid profiles are: "
        + registeredProfileNames());
    }

    /**
     * Sets a preferred server group, that will be used for operations that support this feature.
     *
     * @return this {@link Builder} for chaining purposes.
     */
    public SELF preferredServerGroup(final @Nullable String preferredServerGroup) {
      this.preferredServerGroup = preferredServerGroup;
      return self();
    }

    /**
     * You might wonder why callers can't use
     * {@link #load(PropertyLoader)} to load system properties.
     * <p>
     * It's because that method requires a raw builder type, while
     * {@link SystemPropertyPropertyLoader}'s builder type
     * has an unbounded wildcard.
     * <p>
     * Fixing the types would be a source-incompatible change.
     * We'll get it right next time!
     */
    @Stability.Internal
    public SELF loadSystemProperties() {
      new SystemPropertyPropertyLoader().load(this);
      return self();
    }
  }

  /**
   * Returns the registered environment profile names.
   *
   * @return the names of the profiles.
   */
  private static Set<String> registeredProfileNames() {
    Set<String> names = new HashSet<>();
    for (ConfigurationProfile profile : environmentProfileLoader) {
      names.add(profile.name());
    }
    return names;
  }

}
