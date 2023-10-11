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
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.api.kv.CoreKvBinaryOps;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.manager.search.ClassicCoreClusterSearchIndexManager;
import com.couchbase.client.core.api.manager.search.ClassicCoreScopeSearchIndexManager;
import com.couchbase.client.core.api.manager.search.CoreSearchIndexManager;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.search.ClassicCoreSearchOps;
import com.couchbase.client.core.api.search.CoreSearchOps;
import com.couchbase.client.core.callbacks.BeforeSendRequestCallback;
import com.couchbase.client.core.classic.kv.ClassicCoreKvBinaryOps;
import com.couchbase.client.core.classic.kv.ClassicCoreKvOps;
import com.couchbase.client.core.classic.manager.ClassicCoreBucketManager;
import com.couchbase.client.core.classic.manager.ClassicCoreCollectionManagerOps;
import com.couchbase.client.core.classic.query.ClassicCoreQueryOps;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.cnc.events.core.BucketClosedEvent;
import com.couchbase.client.core.cnc.events.core.BucketOpenFailedEvent;
import com.couchbase.client.core.cnc.events.core.BucketOpenInitiatedEvent;
import com.couchbase.client.core.cnc.events.core.BucketOpenedEvent;
import com.couchbase.client.core.cnc.events.core.CoreCreatedEvent;
import com.couchbase.client.core.cnc.events.core.InitGlobalConfigFailedEvent;
import com.couchbase.client.core.cnc.events.core.ReconfigurationCompletedEvent;
import com.couchbase.client.core.cnc.events.core.ReconfigurationErrorDetectedEvent;
import com.couchbase.client.core.cnc.events.core.ServiceReconfigurationFailedEvent;
import com.couchbase.client.core.cnc.events.core.ShutdownCompletedEvent;
import com.couchbase.client.core.cnc.events.core.ShutdownInitiatedEvent;
import com.couchbase.client.core.cnc.events.core.WatchdogInvalidStateIdentifiedEvent;
import com.couchbase.client.core.cnc.events.core.WatchdogRunFailedEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionsStartedEvent;
import com.couchbase.client.core.config.AlternateAddress;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.config.GlobalConfig;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.GlobalConfigNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.manager.CoreBucketManagerOps;
import com.couchbase.client.core.manager.CoreCollectionManager;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.core.node.AnalyticsLocator;
import com.couchbase.client.core.node.KeyValueLocator;
import com.couchbase.client.core.node.Locator;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.node.RoundRobinLocator;
import com.couchbase.client.core.node.ViewLocator;
import com.couchbase.client.core.service.ServiceScope;
import com.couchbase.client.core.service.ServiceState;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.components.CoreTransactionRequest;
import com.couchbase.client.core.transaction.context.CoreTransactionsContext;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.ConnectionStringUtil;
import com.couchbase.client.core.util.CoreIdGenerator;
import com.couchbase.client.core.util.LatestStateSubscription;
import com.couchbase.client.core.util.NanoTimestamp;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.api.CoreCouchbaseOps.checkConnectionStringScheme;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString;
import static java.util.Objects.requireNonNull;

/**
 * The main entry point into the core layer.
 *
 * <p>This class has been around behind a facade in the 1.x days, but here it is just a plain
 * simple class that can be instantiated and is used across the upper language bindings.</p>
 *
 * @since 2.0.0
 */
@Stability.Volatile
public class Core implements CoreCouchbaseOps, AutoCloseable {

  /**
   * Locates the right node for the KV service.
   */
  private static final KeyValueLocator KEY_VALUE_LOCATOR = new KeyValueLocator();

  /**
   * Locates the right node for the manager service.
   */
  private static final RoundRobinLocator MANAGER_LOCATOR =
    new RoundRobinLocator(ServiceType.MANAGER);

  /**
   * Locates the right node for the query service.
   */
  private static final RoundRobinLocator QUERY_LOCATOR =
    new RoundRobinLocator(ServiceType.QUERY);

  /**
   * Locates the right node for the analytics service.
   */
  private static final RoundRobinLocator ANALYTICS_LOCATOR =
    new AnalyticsLocator();

  /**
   * Locates the right node for the search service.
   */
  private static final RoundRobinLocator SEARCH_LOCATOR =
    new RoundRobinLocator(ServiceType.SEARCH);

  /**
   * Locates the right node for the view service.
   */
  private static final RoundRobinLocator VIEWS_LOCATOR =
    new ViewLocator();

  private static final RoundRobinLocator EVENTING_LOCATOR =
    new RoundRobinLocator(ServiceType.EVENTING);

  private static final RoundRobinLocator BACKUP_LOCATOR =
      new RoundRobinLocator(ServiceType.BACKUP);

  /**
   * The interval under which the invalid state watchdog should be scheduled to run.
   */
  private static final Duration INVALID_STATE_WATCHDOG_INTERVAL = Duration.ofSeconds(5);

  /**
   * Holds the current core context.
   */
  private final CoreContext coreContext;

  /**
   * Holds the current configuration provider.
   */
  private final ConfigurationProvider configurationProvider;

  /**
   * Holds the current configuration for all buckets.
   */
  private volatile ClusterConfig currentConfig;

  /**
   * The list of currently managed nodes against the cluster.
   */
  private final CopyOnWriteArrayList<Node> nodes;

  /**
   * Reconfigures the core in response to configs emitted by {@link #configurationProvider}.
   */
  private final LatestStateSubscription<ClusterConfig> configurationProcessor;

  /**
   * Once shutdown, this will be set to true and as a result no further ops are allowed to go through.
   */
  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  /**
   * Reference to the event bus on the environment.
   */
  private final EventBus eventBus;

  /**
   * Holds a reference to the timer used for timeout registration.
   */
  private final Timer timer;

  private final Set<SeedNode> seedNodes;

  private final List<BeforeSendRequestCallback> beforeSendRequestCallbacks;

  /**
   * Holds the timer to dispose for the invalid state watchdog
   */
  private final Disposable invalidStateWatchdog;

  /**
   * Holds the response metrics per
   */
  private final Map<ResponseMetricIdentifier, ValueRecorder> responseMetrics = new ConcurrentHashMap<>();

  private final CoreTransactionsCleanup transactionsCleanup;

  private final CoreTransactionsContext transactionsContext;

  private final ConnectionString connectionString;

  /**
   * @deprecated Please use {@link #create(CoreEnvironment, Authenticator, ConnectionString)} instead.
   */
  @Deprecated
  public static Core create(
    final CoreEnvironment environment,
    final Authenticator authenticator,
    final Set<SeedNode> seedNodes
  ) {
    return create(environment, authenticator, asConnectionString(seedNodes));
  }

  public static Core create(
    final CoreEnvironment environment,
    final Authenticator authenticator,
    final ConnectionString connectionString
  ) {
    return new Core(environment, authenticator, connectionString);
  }

  protected Core(
    final CoreEnvironment environment,
    final Authenticator authenticator,
    final ConnectionString connectionString
  ) {
    if (environment.securityConfig().tlsEnabled() && !authenticator.supportsTls()) {
      throw new InvalidArgumentException("TLS enabled but the Authenticator does not support TLS!", null, null);
    } else if (!environment.securityConfig().tlsEnabled() && !authenticator.supportsNonTls()) {
      throw new InvalidArgumentException("TLS not enabled but the Authenticator does only support TLS!", null, null);
    }

    checkConnectionStringScheme(connectionString, ConnectionString.Scheme.COUCHBASE, ConnectionString.Scheme.COUCHBASES);

    CoreLimiter.incrementAndVerifyNumInstances(environment.eventBus());

    this.connectionString = requireNonNull(connectionString);
    this.seedNodes = seedNodesFromConnectionString(connectionString, environment);
    this.coreContext = new CoreContext(this, CoreIdGenerator.nextId(), environment, authenticator);
    this.configurationProvider = createConfigurationProvider();
    this.nodes = new CopyOnWriteArrayList<>();
    this.eventBus = environment.eventBus();
    this.timer = environment.timer();
    this.currentConfig = configurationProvider.config();

    Flux<ClusterConfig> configs = configurationProvider
      .configs()
      .concatWith(Mono.just(new ClusterConfig())); // Disconnect everything!

    this.configurationProcessor = new LatestStateSubscription<>(
      configs,
      environment.scheduler(),
      (config, doFinally) -> {
        currentConfig = config;
        reconfigure(doFinally);
      }
    );

    this.beforeSendRequestCallbacks = environment
      .requestCallbacks()
      .stream()
      .filter(c -> c instanceof BeforeSendRequestCallback)
      .map(c -> (BeforeSendRequestCallback) c)
      .collect(Collectors.toList());

    eventBus.publish(new CoreCreatedEvent(coreContext, environment, seedNodes, CoreLimiter.numInstances(), connectionString));

    long watchdogInterval = INVALID_STATE_WATCHDOG_INTERVAL.getSeconds();
    if (watchdogInterval <= 1) {
      throw InvalidArgumentException.fromMessage("The Watchdog Interval cannot be smaller than 1 second!");
    }

    invalidStateWatchdog = environment.scheduler().schedulePeriodically(new InvalidStateWatchdog(),
      watchdogInterval, watchdogInterval, TimeUnit.SECONDS
    );

    this.transactionsCleanup = new CoreTransactionsCleanup(this, environment.transactionsConfig());
    this.transactionsContext = new CoreTransactionsContext(environment.meter());
    context().environment().eventBus().publish(new TransactionsStartedEvent(environment.transactionsConfig().cleanupConfig().runLostAttemptsCleanupThread(),
            environment.transactionsConfig().cleanupConfig().runRegularAttemptsCleanupThread()));
  }

  private static Set<SeedNode> seedNodesFromConnectionString(final ConnectionString cs, final CoreEnvironment env) {
    return ConnectionStringUtil.seedNodesFromConnectionString(
      cs,
      env.ioConfig().dnsSrvEnabled(),
      env.securityConfig().tlsEnabled(),
      env.eventBus()
    );
  }

  /**
   * During testing this can be overridden so that a custom configuration provider is used
   * in the system.
   *
   * @return by default returns the default config provider.
   */
  ConfigurationProvider createConfigurationProvider() {
    return new DefaultConfigurationProvider(this, seedNodes, connectionString);
  }

  /**
   * Returns the attached configuration provider.
   *
   * <p>Internal API, use with care!</p>
   */
  @Stability.Internal
  public ConfigurationProvider configurationProvider() {
    return configurationProvider;
  }

  /**
   * Sends a command into the core layer and registers the request with the timeout timer.
   *
   * @param request the request to dispatch.
   */
  public <R extends Response> void send(final Request<R> request) {
    send(request, true);
  }

  /**
   * Sends a command into the core layer and allows to avoid timeout registration.
   *
   * <p>Usually you want to use {@link #send(Request)} instead, this method should only be used during
   * retry situations where the request has already been registered with a timeout timer before.</p>
   *
   * @param request the request to dispatch.
   * @param registerForTimeout if the request should be registered with a timeout.
   */
  @Stability.Internal
  @SuppressWarnings({"unchecked"})
  public <R extends Response> void send(final Request<R> request, final boolean registerForTimeout) {
    if (shutdown.get()) {
      request.cancel(CancellationReason.SHUTDOWN);
      return;
    }

    if (registerForTimeout) {
      timer.register((Request<Response>) request);
      for (BeforeSendRequestCallback cb : beforeSendRequestCallbacks) {
        cb.beforeSend(request);
      }
    }

    locator(request.serviceType()).dispatch(request, nodes, currentConfig, context());
  }

  /**
   * Returns the {@link CoreContext} of this core instance.
   */
  public CoreContext context() {
    return coreContext;
  }

  /**
   * Returns a client for issuing HTTP requests to servers in the cluster.
   */
  @Stability.Internal
  public CoreHttpClient httpClient(RequestTarget target) {
    return new CoreHttpClient(this, target);
  }

  @Stability.Internal
  public Stream<EndpointDiagnostics> diagnostics() {
    return nodes.stream().flatMap(Node::diagnostics);
  }

  @Stability.Internal
  public Stream<InternalEndpointDiagnostics> internalDiagnostics() {
    return nodes.stream().flatMap(Node::internalDiagnostics);
  }

  /**
   * If present, returns a flux that allows to monitor the state changes of a specific service.
   *
   * @param nodeIdentifier the node identifier for the node.
   * @param type the type of service.
   * @param bucket the bucket, if present.
   * @return if found, a flux with the service states.
   */
  @Stability.Internal
  public Optional<Flux<ServiceState>> serviceState(NodeIdentifier nodeIdentifier, ServiceType type, Optional<String> bucket) {
    for (Node node : nodes) {
      if (node.identifier().equals(nodeIdentifier)) {
        return node.serviceState(type, bucket);
      }
    }
    return Optional.empty();
  }

  /**
   * Instructs the client to, if possible, load and initialize the global config.
   *
   * <p>Since global configs are an "optional" feature depending on the cluster version, if an error happens
   * this method will not fail. Rather it will log the exception (with some logic dependent on the type of error)
   * and will allow the higher level components to move on where possible.</p>
   */
  @Stability.Internal
  public void initGlobalConfig() {
    NanoTimestamp start = NanoTimestamp.now();
    configurationProvider
      .loadAndRefreshGlobalConfig()
      .subscribe(
        v -> {},
        throwable -> {
          InitGlobalConfigFailedEvent.Reason reason = InitGlobalConfigFailedEvent.Reason.UNKNOWN;
          if (throwable instanceof UnsupportedConfigMechanismException) {
            reason = InitGlobalConfigFailedEvent.Reason.UNSUPPORTED;
          } else if (throwable instanceof GlobalConfigNotFoundException) {
            reason = InitGlobalConfigFailedEvent.Reason.NO_CONFIG_FOUND;
          } else if (throwable instanceof ConfigException) {
            if (throwable.getCause() instanceof RequestCanceledException) {
              RequestContext ctx = ((RequestCanceledException) throwable.getCause()).context().requestContext();
              if (ctx.request().cancellationReason() == CancellationReason.SHUTDOWN) {
                reason = InitGlobalConfigFailedEvent.Reason.SHUTDOWN;
              }
            } else if (throwable.getMessage().contains("NO_ACCESS")) {
              reason = InitGlobalConfigFailedEvent.Reason.NO_ACCESS;
            }
          } else if (throwable instanceof AlreadyShutdownException) {
            reason = InitGlobalConfigFailedEvent.Reason.SHUTDOWN;
          }
          eventBus.publish(new InitGlobalConfigFailedEvent(
            reason.severity(),
            start.elapsed(),
            context(),
            reason,
            throwable
          ));
        }
      );
  }

  /**
   * Attempts to open a bucket and fails the {@link Mono} if there is a persistent error
   * as the reason.
   */
  @Stability.Internal
  public void openBucket(final String name) {
    eventBus.publish(new BucketOpenInitiatedEvent(coreContext, name));

    NanoTimestamp start = NanoTimestamp.now();
    configurationProvider
      .openBucket(name)
      .subscribe(
        v -> {},
        t -> {
          Event.Severity severity = t instanceof AlreadyShutdownException
            ? Event.Severity.DEBUG
            : Event.Severity.WARN;
          eventBus.publish(new BucketOpenFailedEvent(
            name,
            severity,
            start.elapsed(),
            coreContext,
            t
          ));
        },
        () -> eventBus.publish(new BucketOpenedEvent(
          start.elapsed(),
          coreContext,
          name
        )));
  }

  /**
   * This API provides access to the current config that is published throughout the core.
   *
   * <p>Note that this is internal API and might change at any time.</p>
   */
  @Stability.Internal
  public ClusterConfig clusterConfig() {
    return configurationProvider.config();
  }

  /**
   * Attempts to close a bucket and fails the {@link Mono} if there is a persistent error
   * as the reason.
   */
  private Mono<Void> closeBucket(final String name) {
    return Mono.defer(() -> {
      NanoTimestamp start = NanoTimestamp.now();
      return configurationProvider
        .closeBucket(name, !shutdown.get())
        .doOnSuccess(ignored -> eventBus.publish(new BucketClosedEvent(
          start.elapsed(),
          coreContext,
          name
        )));
    });
  }

  /**
   * This method can be used by a caller to make sure a certain service is enabled at the given
   * target node.
   *
   * <p>This is advanced, internal functionality and should only be used if the caller knows
   * what they are doing.</p>
   *
   * @param identifier the node to check.
   * @param serviceType the service type to enable if not enabled already.
   * @param port the port where the service is listening on.
   * @param bucket if the service is bound to a bucket, it needs to be provided.
   * @param alternateAddress if an alternate address is present, needs to be provided since it is passed down
   *                         to the node and its services.
   * @return a {@link Mono} which completes once initiated.
   */
  @Stability.Internal
  public Mono<Void> ensureServiceAt(final NodeIdentifier identifier, final ServiceType serviceType, final int port,
                                    final Optional<String> bucket, final Optional<String> alternateAddress) {
    if (shutdown.get()) {
      // We don't want do add a node if we are already shutdown!
      return Mono.empty();
    }

    return Flux
      .fromIterable(nodes)
      .filter(n -> n.identifier().equals(identifier))
      .switchIfEmpty(Mono.defer(() -> {
        Node node = createNode(identifier, alternateAddress);
        nodes.add(node);
        return Mono.just(node);
      }))
      .flatMap(node -> node.addService(serviceType, port, bucket))
      .then();
  }

  @Stability.Internal
  public ValueRecorder responseMetric(final Request<?> request, @Nullable Throwable err) {
    String exceptionSimpleName = null;
    if (err instanceof CompletionException) {
      exceptionSimpleName = err.getCause().getClass().getSimpleName().replace("Exception", "");
    } else if (err != null) {
      exceptionSimpleName = err.getClass().getSimpleName().replace("Exception", "");
    }
    final String finalExceptionSimpleName = exceptionSimpleName;

    return responseMetrics.computeIfAbsent(new ResponseMetricIdentifier(request, exceptionSimpleName), key -> {
      Map<String, String> tags = new HashMap<>(7);
      if (key.serviceType == null) {
        // Virtual service
        if (request instanceof CoreTransactionRequest) {
          tags.put(TracingIdentifiers.ATTR_SERVICE, TracingIdentifiers.SERVICE_TRANSACTIONS);
        }
      }
      else {
        tags.put(TracingIdentifiers.ATTR_SERVICE, key.serviceType);
      }
      tags.put(TracingIdentifiers.ATTR_OPERATION, key.requestName);
      if (key.bucketName != null) {
        tags.put(TracingIdentifiers.ATTR_NAME, key.bucketName);
      }
      if (key.scopeName != null) {
        tags.put(TracingIdentifiers.ATTR_SCOPE, key.scopeName);
      }
      if (key.collectionName != null) {
        tags.put(TracingIdentifiers.ATTR_COLLECTION, key.collectionName);
      }

      if (finalExceptionSimpleName != null) {
        tags.put(TracingIdentifiers.ATTR_OUTCOME, finalExceptionSimpleName);
      } else {
        tags.put(TracingIdentifiers.ATTR_OUTCOME, "Success");
      }

      return coreContext.environment().meter().valueRecorder(TracingIdentifiers.METER_OPERATIONS, tags);
    });
  }

  /**
   * Create a {@link Node} from the given identifier.
   *
   * <p>This method is here so it can be overridden in tests.</p>
   *
   * @param identifier the identifier for the node.
   * @param alternateAddress the alternate address if present.
   * @return the created node instance.
   */
  protected Node createNode(final NodeIdentifier identifier, final Optional<String> alternateAddress) {
    return Node.create(coreContext, identifier, alternateAddress);
  }

  /**
   * Check if the given {@link Node} needs to be removed from the cluster topology.
   *
   * @param node the node in question
   * @param config the current config.
   * @return a mono once disconnected (or completes immediately if there is no need to do so).
   */
  private Mono<Void> maybeRemoveNode(final Node node, final ClusterConfig config) {
    return Mono.defer(() -> {
      boolean stillPresentInBuckets = config
        .bucketConfigs()
        .values()
        .stream()
        .flatMap(bc -> bc.nodes().stream())
        .anyMatch(ni -> ni.identifier().equals(node.identifier()));


      boolean stillPresentInGlobal;
      if (config.globalConfig() != null) {
        stillPresentInGlobal = config
          .globalConfig()
          .portInfos()
          .stream()
          .anyMatch(ni -> ni.identifier().equals(node.identifier()));
      } else {
        stillPresentInGlobal = false;
      }

      if ((!stillPresentInBuckets && !stillPresentInGlobal) || !node.hasServicesEnabled()) {
        return node.disconnect().doOnTerminate(() -> nodes.remove(node));
      }

      return Mono.empty();
    });
  }

  /**
   * This method is used to remove a service from a node.
   *
   * @param identifier the node to check.
   * @param serviceType the service type to remove if present.
   * @return a {@link Mono} which completes once initiated.
   */
  private Mono<Void> removeServiceFrom(final NodeIdentifier identifier, final ServiceType serviceType,
                                       final Optional<String> bucket) {
    return Flux
      .fromIterable(new ArrayList<>(nodes))
      .filter(n -> n.identifier().equals(identifier))
      .filter(node -> node.serviceEnabled(serviceType))
      .flatMap(node -> node.removeService(serviceType, bucket))
      .then();
  }

  @Stability.Internal
  public Mono<Void> shutdown() {
    return shutdown(coreContext.environment().timeoutConfig().disconnectTimeout());
  }

  /**
   * Shuts down this core and all associated, owned resources.
   */
  @Stability.Internal
  public Mono<Void> shutdown(final Duration timeout) {
    return transactionsCleanup.shutdown(timeout)
      .then(Mono.defer(() -> {
        NanoTimestamp start = NanoTimestamp.now();
        if (shutdown.compareAndSet(false, true)) {
          eventBus.publish(new ShutdownInitiatedEvent(coreContext));
          invalidStateWatchdog.dispose();

          return Flux
            .fromIterable(currentConfig.bucketConfigs().keySet())
            .flatMap(this::closeBucket)
            .then(configurationProvider.shutdown())
            .then(configurationProcessor.awaitTermination())
            .doOnTerminate(() -> {
              CoreLimiter.decrement();
              eventBus.publish(new ShutdownCompletedEvent(start.elapsed(), coreContext));
            })
            .then();
        }
        return Mono.empty();
      })).timeout(timeout, coreContext.environment().scheduler());
  }

  /**
   * Reconfigures the SDK topology to align with the current server configuration.
   * <p>
   * When reconfigure is called, it will grab a current configuration and then add/remove
   * nodes/services to mirror the current topology and configuration settings.
   * <p>
   * This is an eventually consistent process, so in-flight operations might still be rescheduled
   * and then picked up later (or cancelled, depending on the strategy). For those coming from 1.x,
   * it works very similar.
   *
   * @param doFinally A callback to execute after reconfiguration is complete.
   */
  private void reconfigure(Runnable doFinally) {
    final ClusterConfig configForThisAttempt = currentConfig;

    if (configForThisAttempt.bucketConfigs().isEmpty() && configForThisAttempt.globalConfig() == null) {
      reconfigureDisconnectAll(doFinally);
      return;
    }

    final NanoTimestamp start = NanoTimestamp.now();
    Flux<BucketConfig> bucketConfigFlux = Flux
      .just(configForThisAttempt)
      .flatMap(cc -> Flux.fromIterable(cc.bucketConfigs().values()));

    reconfigureBuckets(bucketConfigFlux)
      .then(reconfigureGlobal(configForThisAttempt.globalConfig()))
      .then(Mono.defer(() ->
        Flux
          .fromIterable(new ArrayList<>(nodes))
          .flatMap(n -> maybeRemoveNode(n, configForThisAttempt))
          .then()
      ))
      .subscribe(
        v -> {
        },
        e -> {
          doFinally.run();
          eventBus.publish(new ReconfigurationErrorDetectedEvent(context(), e));
        },
        () -> {
          doFinally.run();
          eventBus.publish(new ReconfigurationCompletedEvent(
            start.elapsed(),
            coreContext
          ));
        }
      );
  }

  /**
   * This reconfiguration sequence takes all nodes and disconnects them.
   * <p>
   * This must only be called by {@link #reconfigure}, and only when all buckets are closed,
   * which points to a shutdown/all buckets closed disconnect phase.
   */
  private void reconfigureDisconnectAll(Runnable doFinally) {
    NanoTimestamp start = NanoTimestamp.now();
    Flux
      .fromIterable(new ArrayList<>(nodes))
      .flatMap(Node::disconnect)
      .doOnComplete(nodes::clear)
      .subscribe(
        v -> {
        },
        e -> {
          doFinally.run();
          eventBus.publish(new ReconfigurationErrorDetectedEvent(context(), e));
        },
        () -> {
          doFinally.run();
          eventBus.publish(new ReconfigurationCompletedEvent(
            start.elapsed(),
            coreContext
          ));
        }
      );
  }

  private Mono<Void> reconfigureGlobal(final GlobalConfig config) {
    return Mono.defer(() -> {
      if (config == null) {
        return Mono.empty();
      }

      return Flux
        .fromIterable(config.portInfos())
        .flatMap(ni -> {
          boolean tls = coreContext.environment().securityConfig().tlsEnabled();

          Set<Map.Entry<ServiceType, Integer>> aServices = null;
          Optional<String> alternateAddress = coreContext.alternateAddress();
          String aHost = null;
          if (alternateAddress.isPresent()) {
            AlternateAddress aa = ni.alternateAddresses().get(alternateAddress.get());
            aHost = aa.hostname();
            aServices = tls ? aa.sslServices().entrySet() : aa.services().entrySet();
          }

          if (aServices == null || aServices.isEmpty()) {
            aServices = tls ? ni.sslPorts().entrySet() : ni.ports().entrySet();
          }

          final String alternateHost = aHost;
          final Set<Map.Entry<ServiceType, Integer>> services = aServices;

          Flux<Void> serviceRemoveFlux = Flux
            .fromIterable(Arrays.asList(ServiceType.values()))
            .filter(s -> {
              for (Map.Entry<ServiceType, Integer> inConfig : services) {
                if (inConfig.getKey() == s) {
                  return false;
                }
              }
              return true;
            })
            .flatMap(s -> removeServiceFrom(
              ni.identifier(),
              s,
              Optional.empty())
              .onErrorResume(throwable -> {
                eventBus.publish(new ServiceReconfigurationFailedEvent(
                  coreContext,
                  ni.hostname(),
                  s,
                  throwable
                ));
                return Mono.empty();
              })
            );


          Flux<Void> serviceAddFlux = Flux
            .fromIterable(services)
            .flatMap(s -> ensureServiceAt(
              ni.identifier(),
              s.getKey(),
              s.getValue(),
              Optional.empty(),
              Optional.ofNullable(alternateHost))
              .onErrorResume(throwable -> {
                eventBus.publish(new ServiceReconfigurationFailedEvent(
                  coreContext,
                  ni.hostname(),
                  s.getKey(),
                  throwable
                ));
                return Mono.empty();
              })
            );

          return Flux.merge(serviceAddFlux, serviceRemoveFlux);
        })
        .then();
    });
  }

  /**
   * Contains logic to perform reconfiguration for a bucket config.
   *
   * @param bucketConfigs the flux of bucket configs currently open.
   * @return a mono once reconfiguration for all buckets is complete
   */
  private Mono<Void> reconfigureBuckets(final Flux<BucketConfig> bucketConfigs) {
    return bucketConfigs.flatMap(bc ->
      Flux.fromIterable(bc.nodes())
        .flatMap(ni -> {
          boolean tls = coreContext.environment().securityConfig().tlsEnabled();

          Set<Map.Entry<ServiceType, Integer>> aServices = null;
          Optional<String> alternateAddress = coreContext.alternateAddress();
          String aHost = null;
          if (alternateAddress.isPresent()) {
            AlternateAddress aa = ni.alternateAddresses().get(alternateAddress.get());
            aHost = aa.hostname();
            aServices = tls ? aa.sslServices().entrySet() : aa.services().entrySet();
          }


          if (isNullOrEmpty(aServices)) {
            aServices = tls ? ni.sslServices().entrySet() : ni.services().entrySet();
          }

          final String alternateHost = aHost;
          final Set<Map.Entry<ServiceType, Integer>> services = aServices;

          Flux<Void> serviceRemoveFlux = Flux
            .fromIterable(Arrays.asList(ServiceType.values()))
            .filter(s -> {
              for (Map.Entry<ServiceType, Integer> inConfig : services) {
                if (inConfig.getKey() == s) {
                  return false;
                }
              }
              return true;
            })
            .flatMap(s -> removeServiceFrom(
              ni.identifier(),
              s,
              s.scope() == ServiceScope.BUCKET ? Optional.of(bc.name()) : Optional.empty())
              .onErrorResume(throwable -> {
                eventBus.publish(new ServiceReconfigurationFailedEvent(
                  coreContext,
                  ni.hostname(),
                  s,
                  throwable
                ));
                return Mono.empty();
              })
            );

          Flux<Void> serviceAddFlux = Flux
            .fromIterable(services)
            .flatMap(s -> ensureServiceAt(
              ni.identifier(),
              s.getKey(),
              s.getValue(),
              s.getKey().scope() == ServiceScope.BUCKET ? Optional.of(bc.name()) : Optional.empty(),
              Optional.ofNullable(alternateHost))
              .onErrorResume(throwable -> {
                eventBus.publish(new ServiceReconfigurationFailedEvent(
                  coreContext,
                  ni.hostname(),
                  s.getKey(),
                  throwable
                ));
                return Mono.empty();
              })
            );

          return Flux.merge(serviceAddFlux, serviceRemoveFlux);
        })
    ).then();
  }

  /**
   * Helper method to match the right locator to the given service type.
   *
   * @param serviceType the service type for which a locator should be returned.
   * @return the locator for the service type, or an exception if unknown.
   */
  private static Locator locator(final ServiceType serviceType) {
    switch (serviceType) {
      case KV:
        return KEY_VALUE_LOCATOR;
      case MANAGER:
        return MANAGER_LOCATOR;
      case QUERY:
        return QUERY_LOCATOR;
      case ANALYTICS:
        return ANALYTICS_LOCATOR;
      case SEARCH:
        return SEARCH_LOCATOR;
      case VIEWS:
        return VIEWS_LOCATOR;
      case EVENTING:
        return EVENTING_LOCATOR;
      case BACKUP:
        return BACKUP_LOCATOR;
      default:
        throw new IllegalStateException("Unsupported ServiceType: " + serviceType);
    }
  }

  @Stability.Internal
  public CoreTransactionsCleanup transactionsCleanup() {
    return transactionsCleanup;
  }

  @Stability.Internal
  public CoreTransactionsContext transactionsContext() {
    return transactionsContext;
  }

  @Override
  public void close() {
    shutdown().block();
  }

  @Stability.Internal
  @Override
  public CoreKvOps kvOps(CoreKeyspace keyspace) {
    return new ClassicCoreKvOps(this, keyspace);
  }

  @Stability.Internal
  @Override
  public CoreQueryOps queryOps() {
    return new ClassicCoreQueryOps(this);
  }

  @Stability.Internal
  @Override
  public CoreSearchOps searchOps(@Nullable CoreBucketAndScope scope) {
    return new ClassicCoreSearchOps(this, scope);
  }
  @Stability.Internal
  @Override
  public CoreKvBinaryOps kvBinaryOps(CoreKeyspace keyspace) {
    return new ClassicCoreKvBinaryOps(this, keyspace);
  }

  @Stability.Internal
  @Override
  public CoreBucketManagerOps bucketManager() {
    return new ClassicCoreBucketManager(this);
  }

  @Stability.Internal
  @Override
  public CoreCollectionManager collectionManager(String bucketName) {
    return new ClassicCoreCollectionManagerOps(this, bucketName);
  }

  @Override
  public CoreSearchIndexManager clusterSearchIndexManager() {
    return new ClassicCoreClusterSearchIndexManager(this);
  }

  @Override
  public CoreSearchIndexManager scopeSearchIndexManager(CoreBucketAndScope scope) {
    return new ClassicCoreScopeSearchIndexManager(this, scope);
  }

  @Override
  public CoreEnvironment environment() {
    return context().environment();
  }

  @Override
  public CompletableFuture<Void> waitUntilReady(
    Set<ServiceType> serviceTypes,
    Duration timeout,
    ClusterState desiredState,
    @Nullable String bucketName
  ) {
    return WaitUntilReadyHelper.waitUntilReady(this, serviceTypes, timeout, desiredState, Optional.ofNullable(bucketName));
  }

  @Stability.Internal
  public static class ResponseMetricIdentifier {

    private final String serviceType;
    private final String requestName;
    private final @Nullable String bucketName;
    private final @Nullable String scopeName;
    private final @Nullable String collectionName;
    private final @Nullable String exceptionSimpleName;

    ResponseMetricIdentifier(final Request<?> request, @Nullable String exceptionSimpleName) {
      this.exceptionSimpleName = exceptionSimpleName;
      if (request.serviceType() == null) {
        if (request instanceof CoreTransactionRequest) {
          this.serviceType = TracingIdentifiers.SERVICE_TRANSACTIONS;
        } else {
          // Safer than throwing
          this.serviceType = TracingIdentifiers.SERVICE_UNKNOWN;
        }
      } else {
        this.serviceType = CbTracing.getTracingId(request.serviceType());
      }
      this.requestName = request.name();
      if (request instanceof KeyValueRequest) {
        KeyValueRequest<?> kv = (KeyValueRequest<?>) request;
        bucketName = request.bucket();
        scopeName = kv.collectionIdentifier().scope().orElse(CollectionIdentifier.DEFAULT_SCOPE);
        collectionName = kv.collectionIdentifier().collection().orElse(CollectionIdentifier.DEFAULT_SCOPE);
      } else if (request instanceof QueryRequest) {
        QueryRequest query = (QueryRequest) request;
        bucketName = request.bucket();
        scopeName = query.scope();
        collectionName = null;
      } else if (request instanceof SearchRequest) {
        SearchRequest search = (SearchRequest) request;
        if (search.scope() != null) {
          bucketName = search.scope().bucketName();
          scopeName = search.scope().scopeName();
        } else {
          bucketName = null;
          scopeName = null;
        }
        collectionName = null;
      } else {
        bucketName = null;
        scopeName = null;
        collectionName = null;
      }
    }

    public ResponseMetricIdentifier(final String serviceType, final String requestName) {
      this.serviceType = serviceType;
      this.requestName = requestName;
      this.bucketName = null;
      this.scopeName = null;
      this.collectionName = null;
      this.exceptionSimpleName = null;
    }

    public String serviceType() {
      return serviceType;
    }

    public String requestName() {
      return requestName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ResponseMetricIdentifier that = (ResponseMetricIdentifier) o;
      return serviceType.equals(that.serviceType)
        && Objects.equals(requestName, that.requestName)
        && Objects.equals(bucketName, that.bucketName)
        && Objects.equals(scopeName, that.scopeName)
        && Objects.equals(collectionName, that.collectionName)
        && Objects.equals(exceptionSimpleName, that.exceptionSimpleName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(serviceType, requestName, bucketName, scopeName, collectionName, exceptionSimpleName);
    }
  }

  /**
   * Watchdog responsible for checking for potentially invalid states and initiating corrective action.
   * <p>
   * The following checks are run at the moment:
   * <ol>
   *   <li>If the number of managed nodes differs from the number of nodes in the config, either we are currently undergoing
   *    a rebalance/failover, or the system is in a state where there is some stray state that needs to be cleaned up. In
   *    any case we can trigger a reconfiguration, which is safe under both an expected and unexpected state and will
   *    help to clean up any weird leftover node state.</li>
   * </ol>
   */
  class InvalidStateWatchdog implements Runnable {
    @Override
    public void run() {
      try {
        if (currentConfig != null && currentConfig.hasClusterOrBucketConfig()) {
          int numNodes = nodes.size();
          int numConfigNodes = currentConfig.allNodeAddresses().size();

          if (numNodes != numConfigNodes) {
            String message = "Number of managed nodes (" + numNodes + ") differs from the current config ("
              + numConfigNodes + "), triggering reconfiguration.";
            eventBus.publish(new WatchdogInvalidStateIdentifiedEvent(context(), message));
            configurationProvider.republishCurrentConfig();
          }
        }
      } catch (Throwable ex) {
        eventBus.publish(new WatchdogRunFailedEvent(context(), ex));
      }
    }
  }

}
