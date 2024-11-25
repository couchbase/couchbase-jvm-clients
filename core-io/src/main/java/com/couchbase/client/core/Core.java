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
import com.couchbase.client.core.cnc.RequestTracer;
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
import com.couchbase.client.core.cnc.metrics.LoggingMeter;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.DefaultConfigurationProvider;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.InternalEndpointDiagnostics;
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.RequestTracerDecorator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.GlobalConfigNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
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
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.node.AnalyticsLocator;
import com.couchbase.client.core.node.KeyValueLocator;
import com.couchbase.client.core.node.Locator;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.node.RoundRobinLocator;
import com.couchbase.client.core.node.ViewLocator;
import com.couchbase.client.core.service.ServiceScope;
import com.couchbase.client.core.service.ServiceState;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterIdentifier;
import com.couchbase.client.core.topology.ClusterIdentifierUtil;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.components.CoreTransactionRequest;
import com.couchbase.client.core.transaction.context.CoreTransactionsContext;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.CoreIdGenerator;
import com.couchbase.client.core.util.Deadline;
import com.couchbase.client.core.util.LatestStateSubscription;
import com.couchbase.client.core.util.NanoTimestamp;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
import static com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString;
import static com.couchbase.client.core.util.ConnectionStringUtil.sanityCheckPorts;
import static java.util.Collections.emptySet;
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

  private final List<BeforeSendRequestCallback> beforeSendRequestCallbacks;

  /**
   * Holds the timer to dispose for the invalid state watchdog
   */
  private final Disposable invalidStateWatchdog;

  /**
   * Holds the response metrics.
   * Note that because tags have to be provided on ValueRecorder creation, every unique combination of tags needs to be represented in the ResponseMetricIdentifier key.
   */
  private final Map<ResponseMetricIdentifier, ValueRecorder> responseMetrics = new ConcurrentHashMap<>();

  private final CoreTransactionsCleanup transactionsCleanup;

  private final CoreTransactionsContext transactionsContext;

  private final ConnectionString connectionString;

  private final CoreResources coreResources;

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
    sanityCheckPorts(connectionString);

    CoreLimiter.incrementAndVerifyNumInstances(environment.eventBus());

    this.connectionString = requireNonNull(connectionString);
    boolean ignoresAttributes = CbTracing.isInternalTracer(environment.requestTracer());
    RequestTracer requestTracerDecoratedIfRequired = ignoresAttributes
      ? environment.requestTracer()
      : new RequestTracerDecorator(environment.requestTracer(), () -> {
      if (currentConfig == null) {
        return null;
      }
      if (currentConfig.globalConfig() == null) {
        return null;
      }
      return currentConfig.globalConfig().clusterIdent();
    });
    this.coreResources = new CoreResources() {
      @Override
      public RequestTracer requestTracer() {
        return requestTracerDecoratedIfRequired;
      }
    };
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

    eventBus.publish(new CoreCreatedEvent(coreContext, environment, emptySet(), CoreLimiter.numInstances(), connectionString));

    long watchdogInterval = INVALID_STATE_WATCHDOG_INTERVAL.getSeconds();
    if (watchdogInterval <= 1) {
      throw InvalidArgumentException.fromMessage("The Watchdog Interval cannot be smaller than 1 second!");
    }

    invalidStateWatchdog = environment.scheduler().schedulePeriodically(new InvalidStateWatchdog(),
      watchdogInterval, watchdogInterval, TimeUnit.SECONDS
    );

    this.transactionsCleanup = new CoreTransactionsCleanup(this, environment.transactionsConfig());
    this.transactionsContext = new CoreTransactionsContext(this, environment.meter());
    context().environment().eventBus().publish(new TransactionsStartedEvent(environment.transactionsConfig().cleanupConfig().runLostAttemptsCleanupThread(),
            environment.transactionsConfig().cleanupConfig().runRegularAttemptsCleanupThread()));
  }

  /**
   * During testing this can be overridden so that a custom configuration provider is used
   * in the system.
   *
   * @return by default returns the default config provider.
   */
  ConfigurationProvider createConfigurationProvider() {
    return new DefaultConfigurationProvider(this, connectionString);
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

  @Stability.Internal
  public Mono<ClusterTopology> waitForClusterTopology(Duration timeout) {
    return Mono.defer(() -> {
      Deadline deadline = Deadline.of(timeout);

      return Mono.fromCallable(() -> {
          ClusterTopology globalTopology = clusterConfig().globalTopology();
          if (globalTopology != null) {
            return globalTopology;
          }

          for (ClusterTopologyWithBucket topology : clusterConfig().bucketTopologies()) {
            return topology;
          }

          throw deadline.exceeded()
            ? new UnambiguousTimeoutException("Timed out while waiting for cluster topology", null)
            : new NoSuchElementException(); // trigger retry!
        })
        .retryWhen(Retry
          .fixedDelay(Long.MAX_VALUE, Duration.ofMillis(100))
          .filter(t -> t instanceof NoSuchElementException)
        );
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
   * @return a {@link Mono} which completes once initiated.
   */
  @Stability.Internal
  public Mono<Void> ensureServiceAt(
    final NodeIdentifier identifier,
    final ServiceType serviceType,
    final int port,
    final Optional<String> bucket
  ) {
    if (shutdown.get()) {
      // We don't want do add a node if we are already shutdown!
      return Mono.empty();
    }

    return Flux
      .fromIterable(nodes)
      .filter(n -> n.identifier().equals(identifier))
      .switchIfEmpty(Mono.defer(() -> {
        Node node = createNode(identifier);
        nodes.add(node);
        return Mono.just(node);
      }))
      .flatMap(node -> node.addService(serviceType, port, bucket))
      .then();
  }

  @Stability.Internal
  public ValueRecorder responseMetric(final Request<?> request, @Nullable Throwable err) {
    boolean isDefaultLoggingMeter = coreContext.environment().meter() instanceof LoggingMeter;
    String exceptionSimpleName = null;
    if (!isDefaultLoggingMeter) {
      if (err instanceof CompletionException) {
        exceptionSimpleName = err.getCause().getClass().getSimpleName().replace("Exception", "");
      } else if (err != null) {
        exceptionSimpleName = err.getClass().getSimpleName().replace("Exception", "");
      }
    }
    final String finalExceptionSimpleName = exceptionSimpleName;
    final ClusterIdentifier clusterIdent = ClusterIdentifierUtil.fromConfig(currentConfig);

    return responseMetrics.computeIfAbsent(new ResponseMetricIdentifier(request, exceptionSimpleName, clusterIdent), key -> {
      Map<String, String> tags = new HashMap<>(9);
      if (key.serviceType == null) {
        // Virtual service
        if (request instanceof CoreTransactionRequest) {
          tags.put(TracingIdentifiers.ATTR_SERVICE, TracingIdentifiers.SERVICE_TRANSACTIONS);
        }
      } else {
        tags.put(TracingIdentifiers.ATTR_SERVICE, key.serviceType);
      }
      tags.put(TracingIdentifiers.ATTR_OPERATION, key.requestName);

      // The LoggingMeter only uses the service and operation labels, so optimise this hot-path by skipping
      // assigning other labels.
      if (!isDefaultLoggingMeter) {
        // Crucial note for Micrometer:
        // If we are ever going to output an attribute from a given JVM run then we must always
        // output that attribute in this run.  Specifying null as an attribute value allows the OTel backend to strip it, and
        // the Micrometer backend to provide a default value.
        // See (internal to Couchbase) discussion here for full details:
        // https://issues.couchbase.com/browse/CBSE-17070?focusedId=779820&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-779820
        // If this rule is not followed, then Micrometer will silently discard some metrics.  Micrometer requires that
        // every value output under a given metric has the same set of attributes.

        tags.put(TracingIdentifiers.ATTR_NAME, key.bucketName);
        tags.put(TracingIdentifiers.ATTR_SCOPE, key.scopeName);
        tags.put(TracingIdentifiers.ATTR_COLLECTION, key.collectionName);

        tags.put(TracingIdentifiers.ATTR_CLUSTER_UUID, key.clusterUuid);
        tags.put(TracingIdentifiers.ATTR_CLUSTER_NAME, key.clusterName);

        if (finalExceptionSimpleName != null) {
          tags.put(TracingIdentifiers.ATTR_OUTCOME, finalExceptionSimpleName);
        } else {
          tags.put(TracingIdentifiers.ATTR_OUTCOME, "Success");
        }
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
   * @return the created node instance.
   */
  protected Node createNode(final NodeIdentifier identifier) {
    return Node.create(coreContext, identifier);
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
      boolean stillPresentInBuckets = config.bucketTopologies().stream()
        .anyMatch(topology -> hasNode(topology, node.identifier()));

      ClusterTopology globalTopology = config.globalTopology();
      boolean stillPresentInGlobal = globalTopology != null && hasNode(globalTopology, node.identifier());

      if ((!stillPresentInBuckets && !stillPresentInGlobal) || !node.hasServicesEnabled()) {
        return node.disconnect().doOnTerminate(() -> nodes.remove(node));
      }

      return Mono.empty();
    });
  }

  private static boolean hasNode(ClusterTopology topology, NodeIdentifier nodeId) {
    return topology.nodes().stream()
      .anyMatch(node -> node.id().equals(nodeId));
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
            .fromIterable(currentConfig.bucketNames())
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

    final ClusterTopology globalTopology = configForThisAttempt.globalTopology();
    final Collection<ClusterTopologyWithBucket> bucketTopologies = configForThisAttempt.bucketTopologies();

    if (bucketTopologies.isEmpty() && globalTopology == null) {
      reconfigureDisconnectAll(doFinally);
      return;
    }

    final NanoTimestamp start = NanoTimestamp.now();

    reconfigureBuckets(Flux.fromIterable(bucketTopologies))
      .then(reconfigureGlobal(globalTopology))
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

  private Mono<Void> reconfigureGlobal(final @Nullable ClusterTopology topology) {
    return topology == null
      ? Mono.empty()
      : reconfigureGlobalOrBucket(topology, null);
  }

  /**
   * Contains logic to perform reconfiguration for a bucket config.
   *
   * @param bucketTopologies the flux of topologies from currently open buckets
   * @return a mono once reconfiguration for all buckets is complete
   */
  private Mono<Void> reconfigureBuckets(final Flux<ClusterTopologyWithBucket> bucketTopologies) {
    return bucketTopologies.flatMap(bc -> reconfigureGlobalOrBucket(bc, bc.bucket().name()))
      .then();
  }

  /**
   * @param bucketName pass non-null if using the topology to configure bucket-scoped services.
   *
   * @implNote Maybe in the future we can inspect the ClusterTopology to see if it has a BucketTopology,
   * and get the bucket name from there. However, let's make it explicit for now; this leaves the door open
   * to using a ClusterTopologyWithBucket to configure global services (by passing a null bucket name).
   */
  private Mono<Void> reconfigureGlobalOrBucket(
    ClusterTopology topology,
    @Nullable String bucketName
  ) {
    return Flux.fromIterable(topology.nodes())
      .flatMap(ni -> {
        Flux<Void> serviceRemoveFlux = Flux
          .fromArray(ServiceType.values())
          .filter(s -> !ni.has(s))
          .flatMap(s -> removeServiceFrom(
              ni.id(),
              s,
              s.scope() == ServiceScope.BUCKET ? Optional.ofNullable(bucketName) : Optional.empty())
              .onErrorResume(throwable -> {
                eventBus.publish(new ServiceReconfigurationFailedEvent(
                  coreContext,
                  ni.host(),
                  s,
                  throwable
                ));
                return Mono.empty();
              })
          );

        Flux<Void> serviceAddFlux = Flux
          .fromIterable(ni.ports().entrySet())
          .flatMap(s -> ensureServiceAt(
              ni.id(),
              s.getKey(),
              s.getValue(),
              s.getKey().scope() == ServiceScope.BUCKET ? Optional.ofNullable(bucketName) : Optional.empty())
              .onErrorResume(throwable -> {
                eventBus.publish(new ServiceReconfigurationFailedEvent(
                  coreContext,
                  ni.host(),
                  s.getKey(),
                  throwable
                ));
                return Mono.empty();
              })
          );

        return Flux.merge(serviceAddFlux, serviceRemoveFlux);
      })
      .then();
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

  @Stability.Internal
  @Override
  public CoreResources coreResources() {
    return coreResources;
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
    private final @Nullable String clusterName;
    private final @Nullable String clusterUuid;

    ResponseMetricIdentifier(final Request<?> request, @Nullable String exceptionSimpleName, @Nullable ClusterIdentifier clusterIdent) {
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
      this.clusterName = clusterIdent == null ? null : clusterIdent.clusterName();
      this.clusterUuid = clusterIdent == null ? null : clusterIdent.clusterUuid();
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
      } else if (request instanceof ServerSearchRequest) {
        ServerSearchRequest search = (ServerSearchRequest) request;
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
      this.clusterName = null;
      this.clusterUuid = null;
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
        && Objects.equals(exceptionSimpleName, that.exceptionSimpleName)
        && Objects.equals(clusterName, that.clusterName)
        && Objects.equals(clusterUuid, that.clusterUuid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(serviceType, requestName, bucketName, scopeName, collectionName, exceptionSimpleName, clusterName, clusterUuid);
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
