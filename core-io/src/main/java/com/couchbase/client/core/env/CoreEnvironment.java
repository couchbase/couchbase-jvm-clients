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
import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.cnc.DefaultEventBus;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.LoggingEventConsumer;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.tracing.ThresholdRequestTracer;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.net.URL;
import java.time.Duration;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static java.util.Objects.requireNonNull;

/**
 * The {@link CoreEnvironment} is an extendable, configurable and stateful
 * config designed to be passed into a core instance.
 *
 * @since 1.0.0
 */
public class CoreEnvironment {

  private static final String CORE_AGENT_TITLE = "java-core";

  private static final Map<String, Attributes> MANIFEST_INFOS = new ConcurrentHashMap<>();

  static {
    try {
      Enumeration<URL> resources = CoreEnvironment.class.getClassLoader().getResources(JarFile.MANIFEST_NAME);
      while (resources.hasMoreElements()) {
        URL manifestUrl = resources.nextElement();
        if (manifestUrl == null) {
          continue;
        }
        Manifest manifest = new Manifest(manifestUrl.openStream());
        if (manifest.getEntries() == null) {
          continue;
        }
        for (Map.Entry<String, Attributes> entry : manifest.getEntries().entrySet()) {
          if (entry.getKey().startsWith("couchbase-")) {
            MANIFEST_INFOS.put(entry.getKey(), entry.getValue());
          }
        }
      }
    } catch (Exception e) {
      // Ignored on purpose.
    }
  }

  /**
   * The default retry strategy used for all ops if not overridden.
   */
  private static final RetryStrategy DEFAULT_RETRY_STRATEGY = BestEffortRetryStrategy.INSTANCE;

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
  private final DiagnosticsConfig diagnosticsConfig;
  private final Supplier<RequestTracer> requestTracer;

  private final LoggerConfig loggerConfig;

  private final RetryStrategy retryStrategy;
  private final Supplier<Scheduler> scheduler;


  public static CoreEnvironment create() {
    return builder().build();
  }

  public static CoreEnvironment.Builder builder() {
    return new Builder();
  }

  @SuppressWarnings("unchecked")
  protected CoreEnvironment(final Builder builder) {
    new SystemPropertyPropertyLoader().load(builder);

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

    this.securityConfig = builder.securityConfig.build();

    this.ioEnvironment = builder.ioEnvironment.build();
    this.ioConfig = builder.ioConfig.build();
    this.compressionConfig = builder.compressionConfig.build();
    this.timeoutConfig = builder.timeoutConfig.build();
    this.retryStrategy = Optional.ofNullable(builder.retryStrategy).orElse(DEFAULT_RETRY_STRATEGY);
    this.loggerConfig = builder.loggerConfig.build();
    this.diagnosticsConfig = builder.diagnosticsConfig.build();

    if (eventBus instanceof OwnedSupplier) {
      eventBus.get().start().block();
    }
    eventBus.get().subscribe(LoggingEventConsumer.create(loggerConfig()));

    this.requestTracer = Optional.ofNullable(builder.requestTracer).orElse(new OwnedSupplier<RequestTracer>(
      ThresholdRequestTracer.create(eventBus.get())
    ));

    if (requestTracer instanceof OwnedSupplier) {
      requestTracer.get().start().block();
    }
  }

  /**
   * Helper method which grabs the title and version for the user agent from the manifest.
   *
   * @return the user agent string, in a best effort manner.
   */
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
   * If present, returns the git hash for the client at build time.
   */
  public Optional<String> clientHash() {
    return loadFromManifest(defaultAgentTitle(), "Impl-Git-Revision");
  }

  /**
   * If present, returns the git hash for the core at build time.
   */
  public Optional<String> coreHash() {
    return loadFromManifest(CORE_AGENT_TITLE, "Impl-Git-Revision");
  }

  /**
   * If present, returns the client version at build time.
   */
  public Optional<String> clientVersion() {
    return loadFromManifest(defaultAgentTitle(), "Impl-Version");
  }

  /**
   * If present, returns the core version at build time.
   */
  public Optional<String> coreVersion() {
    return loadFromManifest(CORE_AGENT_TITLE, "Impl-Version");
  }

  /**
   * Helper method to load the value from the parsed manifests (if present).
   *
   * @param agent the agent suffix, either core or client per pom file.
   * @param value the value of the manifest attribute to fetch.
   * @return if found, returns the attribute value or an empty optional otherwise.
   */
  private Optional<String> loadFromManifest(final String agent, final String value) {
    Attributes attributes = MANIFEST_INFOS.get("couchbase-" + agent);
    if (attributes == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(attributes.getValue(value));
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

  public CompressionConfig compressionConfig() {
    return compressionConfig;
  }

  public LoggerConfig loggerConfig() {
    return loggerConfig;
  }

  public Scheduler scheduler() {
    return scheduler.get();
  }

  public RequestTracer requestTracer() {
    return requestTracer.get();
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

  public RetryStrategy retryStrategy() {
    return retryStrategy;
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
    return Mono.defer(() -> eventBus instanceof OwnedSupplier ? eventBus.get().stop(timeout) : Mono.empty())
      .then(Mono.defer(() -> {
        timer.stop();
        return Mono.empty();
      }))
      .then(ioEnvironment.shutdown(timeout))
      .then(Mono.defer(() -> {
        if (scheduler instanceof OwnedSupplier) {
          scheduler.get().dispose();
        }
        return Mono.empty();
      }))
      .then(Mono.defer(() -> {
        if (requestTracer instanceof OwnedSupplier) {
          return requestTracer.get().stop(timeout);
        }
        return Mono.empty();
      }))
      .timeout(timeout);
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

    input.put("ioEnvironment", ioEnvironment.exportAsMap());
    input.put("ioConfig", ioConfig.exportAsMap());
    input.put("compressionConfig", compressionConfig.exportAsMap());
    input.put("securityConfig", securityConfig.exportAsMap());
    input.put("timeoutConfig", timeoutConfig.exportAsMap());
    input.put("loggerConfig", loggerConfig.exportAsMap());
    input.put("diagnosticsConfig", diagnosticsConfig.exportAsMap());

    input.put("retryStrategy", retryStrategy.getClass().getSimpleName());
    input.put("requestTracer", requestTracer.getClass().getSimpleName());

    return format.apply(input);
  }

  @Override
  public String toString() {
    return exportAsString(Context.ExportFormat.STRING);
  }

  public static class Builder<SELF extends Builder<SELF>> {

    private IoEnvironment.Builder ioEnvironment = IoEnvironment.builder();
    private IoConfig.Builder ioConfig = IoConfig.builder();
    private CompressionConfig.Builder compressionConfig = CompressionConfig.builder();
    private SecurityConfig.Builder securityConfig = SecurityConfig.builder();
    private TimeoutConfig.Builder timeoutConfig = TimeoutConfig.builder();
    private LoggerConfig.Builder loggerConfig = LoggerConfig.builder();
    private DiagnosticsConfig.Builder diagnosticsConfig = DiagnosticsConfig.builder();
    private Supplier<EventBus> eventBus = null;
    private Supplier<Scheduler> scheduler = null;
    private Supplier<RequestTracer> requestTracer = null;

    private RetryStrategy retryStrategy;

    protected Builder() { }

    @SuppressWarnings("unchecked")
    protected SELF self() {
      return (SELF) this;
    }

    public SELF load(final PropertyLoader<Builder> loader) {
      loader.load(this);
      return self();
    }

    public SELF ioEnvironment(final IoEnvironment.Builder ioEnvironment) {
      this.ioEnvironment = ioEnvironment;
      return self();
    }

    public SELF ioConfig(final IoConfig.Builder ioConfig) {
      this.ioConfig = requireNonNull(ioConfig);
      return self();
    }

    public IoConfig.Builder ioConfig() {
      return ioConfig;
    }

    public SELF compressionConfig(final CompressionConfig.Builder compressionConfig) {
      this.compressionConfig = requireNonNull(compressionConfig);
      return self();
    }

    public CompressionConfig.Builder compressionConfig() {
      return compressionConfig;
    }

    public SELF securityConfig(final SecurityConfig.Builder securityConfig) {
      this.securityConfig = requireNonNull(securityConfig);
      return self();
    }

    public SecurityConfig.Builder securityConfig() {
      return securityConfig;
    }

    public SELF timeoutConfig(final TimeoutConfig.Builder timeoutConfig) {
      this.timeoutConfig = requireNonNull(timeoutConfig);
      return self();
    }

    public TimeoutConfig.Builder timeoutConfig() {
      return this.timeoutConfig;
    }

    public SELF loggerConfig(final LoggerConfig.Builder loggerConfig) {
      this.loggerConfig = requireNonNull(loggerConfig);
      return self();
    }

    public LoggerConfig.Builder loggerConfig() {
      return loggerConfig;
    }

    @Stability.Volatile
    public SELF diagnosticsConfig(final DiagnosticsConfig.Builder diagnosticsConfig) {
      this.diagnosticsConfig = requireNonNull(diagnosticsConfig);
      return self();
    }

    public DiagnosticsConfig.Builder diagnosticsConfig() {
      return diagnosticsConfig;
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

    public SELF retryStrategy(final RetryStrategy retryStrategy) {
      this.retryStrategy = retryStrategy;
      return self();
    }

    public SELF requestTracer(final RequestTracer requestTracer) {
      this.requestTracer = new ExternalSupplier<>(requestTracer);
      return self();
    }

    public CoreEnvironment build() {
      return new CoreEnvironment(this);
    }
  }

}
