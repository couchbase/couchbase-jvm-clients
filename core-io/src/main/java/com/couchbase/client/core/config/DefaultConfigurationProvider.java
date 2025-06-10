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

package com.couchbase.client.core.config;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.config.BucketConfigUpdatedEvent;
import com.couchbase.client.core.cnc.events.config.BucketOpenRetriedEvent;
import com.couchbase.client.core.cnc.events.config.CollectionMapRefreshFailedEvent;
import com.couchbase.client.core.cnc.events.config.CollectionMapRefreshIgnoredEvent;
import com.couchbase.client.core.cnc.events.config.CollectionMapRefreshSucceededEvent;
import com.couchbase.client.core.cnc.events.config.ConfigIgnoredEvent;
import com.couchbase.client.core.cnc.events.config.ConfigPushFailedEvent;
import com.couchbase.client.core.cnc.events.config.DnsSrvRefreshAttemptCompletedEvent;
import com.couchbase.client.core.cnc.events.config.DnsSrvRefreshAttemptFailedEvent;
import com.couchbase.client.core.cnc.events.config.GlobalConfigRetriedEvent;
import com.couchbase.client.core.cnc.events.config.GlobalConfigUpdatedEvent;
import com.couchbase.client.core.cnc.events.config.IndividualGlobalConfigLoadFailedEvent;
import com.couchbase.client.core.cnc.events.config.SeedNodesUpdateFailedEvent;
import com.couchbase.client.core.cnc.events.config.SeedNodesUpdatedEvent;
import com.couchbase.client.core.config.loader.ClusterManagerBucketLoader;
import com.couchbase.client.core.config.loader.GlobalLoader;
import com.couchbase.client.core.config.loader.KeyValueBucketLoader;
import com.couchbase.client.core.config.refresher.ClusterManagerBucketRefresher;
import com.couchbase.client.core.config.refresher.GlobalRefresher;
import com.couchbase.client.core.config.refresher.KeyValueBucketRefresher;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.BucketNotFoundDuringLoadException;
import com.couchbase.client.core.error.BucketNotReadyDuringLoadException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.NoAccessDuringConfigLoadException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.SeedNodeOutdatedException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetCollectionIdRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.BucketTopology;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import com.couchbase.client.core.topology.NetworkSelector;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.topology.PortSelector;
import com.couchbase.client.core.topology.TopologyParser;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.ConnectionStringUtil;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.core.util.UnsignedLEB128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.retry.Retry;

import javax.naming.NamingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.couchbase.client.core.Reactor.emitFailureHandler;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.ConnectionStringUtil.fromDnsSrvOrThrowIfTlsRequired;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * The standard {@link ConfigurationProvider} that is used by default.
 *
 * <p>This provider has been around since the 1.x days, but it has been revamped and reworked
 * for the 2.x breakage - the overall functionality remains very similar though.</p>
 *
 * @since 1.0.0
 */
public class DefaultConfigurationProvider implements ConfigurationProvider {
  private static final Logger log = LoggerFactory.getLogger(DefaultConfigurationProvider.class);

  /**
   * Don't perform DNS SRV lookups more quickly than every 10 seconds.
   */
  private static final Duration MIN_TIME_BETWEEN_DNS_LOOKUPS = Duration.ofSeconds(10);

  /**
   * The default port used for kv bootstrap if not otherwise set on the env.
   */
  private static final int DEFAULT_KV_PORT = 11210;

  /**
   * The default port used for manager bootstrap if not otherwise set on the env.
   */
  private static final int DEFAULT_MANAGER_PORT = 8091;

  /**
   * The default port used for kv bootstrap if encryption is enabled.
   */
  private static final int DEFAULT_KV_TLS_PORT = 11207;

  /**
   * The default port used for manager bootstrap if encryption is enabled.
   */
  private static final int DEFAULT_MANAGER_TLS_PORT = 18091;

  /**
   * The number of loaders which will (at maximum) try to load a config in parallel.
   */
  private static final int MAX_PARALLEL_LOADERS = 5;

  private final Core core;
  private final EventBus eventBus;
  @Nullable private volatile TopologyParser topologyParser;

  private final KeyValueBucketLoader keyValueLoader;
  private final ClusterManagerBucketLoader clusterManagerLoader;
  private final KeyValueBucketRefresher keyValueRefresher;
  private final ClusterManagerBucketRefresher clusterManagerRefresher;
  private final GlobalLoader globalLoader;
  private final GlobalRefresher globalRefresher;

  private final Sinks.Many<ClusterConfig> configsSink = Sinks.many().replay().latest();
  private final Flux<ClusterConfig> configs = configsSink.asFlux();
  private final ClusterConfig currentConfig = new ClusterConfig();

  private final Disposable seedNodeResolver; // async task that gets the initial seed nodes from the connection string

  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final CollectionMap collectionMap = new CollectionMap();

  private volatile boolean globalConfigLoadInProgress = false;
  private final AtomicInteger bucketConfigLoadInProgress = new AtomicInteger();

  /**
   * Made package private so it can be inspected during testing.
   */
  final Set<CollectionIdentifier> collectionMapRefreshInProgress = ConcurrentHashMap.newKeySet();

  /**
   * Stores the current seed nodes used to bootstrap buckets and global configs.
   */
  private final AtomicReference<Set<SeedNode>> currentSeedNodes = new AtomicReference<>(emptySet());
  private final Sinks.Many<Set<SeedNode>> seedNodesSink = Sinks.many().replay().latest();
  private final Flux<Set<SeedNode>> seedNodes = seedNodesSink.asFlux();

  private final ConnectionString connectionString;

  private volatile NanoTimestamp lastDnsSrvLookup = NanoTimestamp.never();

  private final Sinks.Many<Long> configPollTrigger = Sinks.many().multicast().directBestEffort();

  /**
   * Creates a new configuration provider.
   *
   * @param core the core against which all ops are executed.
   */
  public DefaultConfigurationProvider(final Core core, final ConnectionString connectionString) {
    this.core = core;
    eventBus = core.context().environment().eventBus();
    this.connectionString = requireNonNull(connectionString);
    this.seedNodeResolver = launchSeedNodeResolver(connectionString, core.environment());

    keyValueLoader = new KeyValueBucketLoader(core);
    clusterManagerLoader = new ClusterManagerBucketLoader(core);
    keyValueRefresher = new KeyValueBucketRefresher(this, core);
    clusterManagerRefresher = new ClusterManagerBucketRefresher(this, core);
    globalLoader = new GlobalLoader(core);
    globalRefresher = new GlobalRefresher(this, core);

    // Start with pushing the current config into the sink for all subscribers currently attached.
    configsSink.emitNext(currentConfig, emitFailureHandler());
  }

  /**
   * Launches an async task that resolves connection string addresses
   * into seed nodes, then publishes the results to the seed nodes sink.
   * <p>
   * This task must complete (publish the seed nodes) before the SDK can
   * attempt to connect to the server and fetch cluster topology.
   * <p>
   * If DNS SRV is disabled or not applicable, this is basically a noop.
   * Otherwise, the task attempts DNS SRV resolution. If the environment
   * _requires_ SRV, the task retries until SRV lookup succeeds.
   *
   * @return a handle for cancelling the task.
   */
  private Disposable launchSeedNodeResolver(ConnectionString connectionString, CoreEnvironment env) {
    return Mono.fromRunnable(() -> {
        Set<SeedNode> seedNodes = ConnectionStringUtil.seedNodesFromConnectionString(
          connectionString,
          env.ioConfig().dnsSrvEnabled(),
          env.securityConfig().tlsEnabled(),
          env.eventBus()
        );
        topologyParser = createTopologyParser(core.environment(), seedNodes);
        setSeedNodes(seedNodes).orThrow();
        log.info("Resolved seed nodes: {}", redactSystem(seedNodes));
      })
      .subscribeOn(Schedulers.boundedElastic()) // DNS SRV lookup blocks
      .doOnError(t -> log.warn("Seed node resolution failed. Will try again after a brief delay. Cause:", t))
      // keep retrying until the task is cancelled (config provider is closed)
      .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(10))
        .maxBackoff(Duration.ofSeconds(60))
        .jitter(1.0)
      )
      .subscribe();
  }

  private static TopologyParser createTopologyParser(
    CoreEnvironment env,
    Set<SeedNode> seedNodes
  ) {
    boolean tls = env.securityConfig().tlsEnabled();
    return new TopologyParser(
      NetworkSelector.create(
        env.ioConfig().networkResolution(),
        makeDefaultPortsExplicitForNetworkDetection(seedNodes, tls)
      ),
      tls ? PortSelector.TLS : PortSelector.NON_TLS,
      env.ioConfig().memcachedHashingStrategy()
    );
  }

  private static Set<SeedNode> makeDefaultPortsExplicitForNetworkDetection(
    Set<SeedNode> seedNodes,
    boolean tls
  ) {
    return seedNodes.stream()
      .map(it -> {
        if (it.kvPort().isPresent() || it.clusterManagerPort().isPresent()) {
          // User specified at least one port, which is sufficient for network detection.
          return it;
        }
        // User didn't specify any ports, so assume defaults.
        return it
          .withKvPort(tls ? 11207 : 11210)
          .withManagerPort(tls ? 18091 : 8091);
      })
      .collect(toSet());
  }

  @Override
  public CollectionMap collectionMap() {
    return collectionMap;
  }

  @Override
  public Flux<ClusterConfig> configs() {
    return configs;
  }

  @Override
  public ClusterConfig config() {
    return currentConfig;
  }

  @Override
  public Flux<Set<SeedNode>> seedNodes() {
    return seedNodes;
  }

  private Mono<Set<SeedNode>> waitForSeedNodes() {
    return seedNodes.next()
      // Mono might be empty if cluster was shut down before first set of seed nodes was published.
      .switchIfEmpty(Mono.error(new AlreadyShutdownException()));
  }

  @Override
  public Mono<Void> openBucket(final String name) {
    return Mono.defer(() -> {
      if (!shutdown.get()) {
        bucketConfigLoadInProgress.incrementAndGet();
        boolean tls = core.context().environment().securityConfig().tlsEnabled();

        return waitForSeedNodes()
          .flatMap(seedNodes -> fetchBucketConfigs(name, seedNodes, tls).switchIfEmpty(Mono.error(
              new ConfigException("Could not locate a single bucket configuration for bucket: " + name)
            ))
          )
          .map(ctx -> {
            proposeBucketConfig(ctx);
            return ctx;
          })
          .then(registerRefresher(name))
          .doOnTerminate(bucketConfigLoadInProgress::decrementAndGet)
          .onErrorResume(t -> closeBucketIgnoreShutdown(name, true).then(Mono.error(t)));
      } else {
        return Mono.error(new AlreadyShutdownException());
      }
    });
  }

  /**
   * Encapsulates the logic to load the bucket config from kv and then fall back to manager.
   * <p>
   * This method can be overridden in tests to simulate various states/errors from the loaders.
   *
   * @param identifier the identifier to load it from.
   * @param mappedKvPort the port of the kv loader.
   * @param mappedManagerPort the port for the manager.
   * @param name the name of the bucket.
   * @return returns the bucket config context if present, or an error.
   */
  protected Mono<ProposedBucketConfigContext> loadBucketConfigForSeed(
    NodeIdentifier identifier,
    int mappedKvPort,
    int mappedManagerPort,
    String name
  ) {
    return keyValueLoader
      .load(identifier, mappedKvPort, name)
      .onErrorResume(t -> {
        boolean removedWhileOpInFlight = t instanceof ConfigException
          && t.getCause() instanceof RequestCanceledException
          && ((RequestCanceledException) t.getCause()).reason() == CancellationReason.TARGET_NODE_REMOVED;
        boolean seedNodeOutdated = t instanceof SeedNodeOutdatedException;

        if (removedWhileOpInFlight || seedNodeOutdated) {
          return Mono.error(t);
        }
        return clusterManagerLoader.load(
          identifier, mappedManagerPort, name
        );
      })
      .flatMap(ctx -> {
        JsonNode configRoot = Mapper.decodeIntoTree(ctx.config());
        if (configRoot.get("nodes").isEmpty()) {
          return Mono.error(new BucketNotReadyDuringLoadException("No KV node in the config (yet), can't use it for now."));
        } else {
          return Mono.just(ctx);
        }
      });
  }

  @Override
  public Mono<Void> loadAndRefreshGlobalConfig() {
    return Mono.defer(() -> {
      if (!shutdown.get()) {
        globalConfigLoadInProgress = true;
        boolean tls = core.context().environment().securityConfig().tlsEnabled();

        return waitForSeedNodes()
          .flatMap(seedNodes -> fetchGlobalConfigs(seedNodes, tls, false, true).switchIfEmpty(Mono.error(
              new ConfigException("Could not locate a single global configuration")
            ))
          )
          .map(ctx -> {
            proposeGlobalConfig(ctx);
            return ctx;
          })
          .then(globalRefresher.start())
          .doOnTerminate(() -> globalConfigLoadInProgress = false);
      } else {
        return Mono.error(new AlreadyShutdownException());
      }
    });
  }

  private ClusterTopology parseClusterTopology(String json, String origin) {
    TopologyParser parser = this.topologyParser;
    if (parser == null) {
      // Shouldn't be possible. The parser is initialized after seed nodes are resolved, and before they are published.
      // The SDK can't connect to the server until seed nodes are published.
      // Outside unit tests, all topology JSON comes from the server.
      // Therefore, if we have topology JSON from the server, the parser has already been initialized.
      throw new IllegalStateException("Can't parse cluster topology until seed nodes are resolved.");
    }
    return parser.parse(json, origin);
  }

  @Override
  public void proposeGlobalConfig(final ProposedGlobalConfigContext ctx) {
    proposeTopology(ctx.config(), ctx.origin(), null, ctx.forcesOverride());
  }

  @Override
  public void proposeBucketConfig(final ProposedBucketConfigContext ctx) {
    proposeTopology(ctx.config(), ctx.origin(), ctx.bucketName(), ctx.forcesOverride());
  }

  private void proposeTopology(
    String topologyJson,
    String origin,
    @Nullable String bucketName,
    boolean forceApply
  ) {
    if (shutdown.get()) {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.ALREADY_SHUTDOWN,
        Optional.empty(),
        Optional.of(topologyJson),
        Optional.ofNullable(bucketName)
      ));
      return;
    }

    if (topologyJson.isEmpty()) {
      // It came from an "ifNewerThan" request, and the result wasn't newer.
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.OLD_OR_SAME_REVISION,
        Optional.empty(),
        Optional.empty(),
        Optional.ofNullable(bucketName)
      ));
      return;
    }

    final ClusterTopology topology;
    try {
      topology = parseClusterTopology(topologyJson, origin);
    } catch (Exception ex) {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.PARSE_FAILURE,
        Optional.of(ex),
        Optional.of(topologyJson),
        Optional.ofNullable(bucketName)
      ));
      return;
    }

    synchronized (this) {
      try {
        final boolean globalTopologyApplied = checkAndApplyGlobalTopology(topology, forceApply);
        log.debug("{} global topology revision {} from origin {}",
          globalTopologyApplied ? "Applying" : "Ignoring",
          topology.revision(),
          origin
        );

        final boolean bucketTopologyApplied;
        if (!(topology instanceof ClusterTopologyWithBucket)) {
          bucketTopologyApplied = false;
        } else {
          bucketTopologyApplied = checkAndApplyBucketTopology(topology.requireBucket(), forceApply);

          log.debug("{} bucket topology revision {} for bucket [{}] from origin {}",
            bucketTopologyApplied ? "Applying" : "Ignoring",
            topology.revision(),
            topology.requireBucket().bucket().name(),
            origin
          );
        }

        if (globalTopologyApplied || bucketTopologyApplied) {
          pushConfig(false);
        }

      } catch (Exception e) {
        log.error("Failed to process proposed cluster topology: {}", redactMeta(topology), e);
      }
    }
  }

  @Override
  public Mono<Void> closeBucket(final String name, boolean pushConfig) {
    return Mono.defer(() -> shutdown.get()
      ? Mono.error(new AlreadyShutdownException())
      : closeBucketIgnoreShutdown(name, pushConfig));
  }

  /**
   * Helper method to close the bucket but ignore the shutdown variable.
   *
   * <p>This method is only intended to be used by safe wrappers, i.e. the closeBucket and
   * shutdown methods that do check the shutdown variable. This method is needed since to be
   * DRY, we need to close the bucket inside the shutdown method which leads to a race condition
   * on checking the shutdown atomic variable.</p>
   *
   * @param name the bucket name.
   * @param pushConfig whether to push the updated config.  Here so that during shutdown we only
   *             push one config.
   * @return completed mono once done.
   */
  private Mono<Void> closeBucketIgnoreShutdown(final String name, final boolean pushConfig) {
    return Mono
      .defer(() -> {
        currentConfig.deleteBucketConfig(name);
        if (pushConfig) {
          pushConfig(false);
        }
        return Mono.empty();
      })
      .then(keyValueRefresher.deregister(name))
      .then(clusterManagerRefresher.deregister(name));
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(() -> {
      if (shutdown.compareAndSet(false, true)) {
        return Flux
          .fromIterable(currentConfig.bucketConfigs().values())
          // Don't push the updated config here - we'll push one final updated config in doOnTerminate
          .flatMap(bucketConfig -> closeBucketIgnoreShutdown(bucketConfig.name(), false))
          .then(Mono.defer(this::disableAndClearGlobalConfig))
          .doOnTerminate(() -> {
            // make sure to push a final, empty config before complete to give downstream
            // consumers a chance to clean up
            // Note that Core.shutdown itself now also emits an empty config after this
            // is complete, so this is probably redundant.
            pushConfig(true);
            configsSink.emitComplete(emitFailureHandler());
            seedNodesSink.emitComplete(emitFailureHandler());
            seedNodeResolver.dispose(); // in case it hasn't completed and is still retrying.
          })
          .then(keyValueRefresher.shutdown())
          .then(clusterManagerRefresher.shutdown())
          .then(globalRefresher.shutdown());
      } else {
        return Mono.error(new AlreadyShutdownException());
      }
    });
  }

  private Mono<Void> disableAndClearGlobalConfig() {
    return globalRefresher.stop().then(Mono.defer(() -> {
      currentConfig.deleteGlobalConfig();
      return Mono.empty();
    }));
  }

  @Override
  public synchronized void refreshCollectionId(final CollectionIdentifier identifier) {
    if (collectionRefreshInProgress(identifier)) {
      eventBus.publish(new CollectionMapRefreshIgnoredEvent(core.context(), identifier));
      return;
    }
    collectionMapRefreshInProgress.add(identifier);

    NanoTimestamp start = NanoTimestamp.now();
    GetCollectionIdRequest request = new GetCollectionIdRequest(
      core.context().environment().timeoutConfig().kvTimeout(),
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      identifier
    );
    core.send(request);
    request.response().whenComplete((response, throwable) -> {
      try {
        final Duration duration = start.elapsed();
        if (throwable != null) {
          eventBus.publish(new CollectionMapRefreshFailedEvent(
            duration,
            core.context(),
            identifier,
            throwable,
            CollectionMapRefreshFailedEvent.Reason.FAILED
          ));
          return;
        }

        if (response.status().success()) {
          if (response.collectionId().isPresent()) {
            long cid = response.collectionId().get();
            collectionMap.put(identifier, UnsignedLEB128.encode(cid));
            eventBus.publish(new CollectionMapRefreshSucceededEvent(duration, core.context(), identifier, cid));
          } else {
            eventBus.publish(new CollectionMapRefreshFailedEvent(
              duration,
              core.context(),
              identifier,
              null,
              CollectionMapRefreshFailedEvent.Reason.COLLECTION_ID_NOT_PRESENT
            ));
          }
        } else {
          Throwable cause = null;
          CollectionMapRefreshFailedEvent.Reason reason;

          if (response.status() == ResponseStatus.UNKNOWN
            || response.status() == ResponseStatus.NO_COLLECTIONS_MANIFEST) {
            reason = CollectionMapRefreshFailedEvent.Reason.NOT_SUPPORTED;
          } else if (response.status() == ResponseStatus.UNKNOWN_COLLECTION) {
            reason = CollectionMapRefreshFailedEvent.Reason.UNKNOWN_COLLECTION;
          } else if (response.status() == ResponseStatus.UNKNOWN_SCOPE) {
            reason = CollectionMapRefreshFailedEvent.Reason.UNKNOWN_SCOPE;
          } else if (response.status() == ResponseStatus.INVALID_REQUEST) {
            reason = CollectionMapRefreshFailedEvent.Reason.INVALID_REQUEST;
          } else {
            cause = new CouchbaseException(response.toString());
            reason = CollectionMapRefreshFailedEvent.Reason.UNKNOWN;
          }

          eventBus.publish(new CollectionMapRefreshFailedEvent(
            duration,
            core.context(),
            identifier,
            cause,
            reason
          ));
        }
      } finally {
        collectionMapRefreshInProgress.remove(identifier);
      }
    });
  }

  @Override
  public boolean collectionRefreshInProgress() {
    return !collectionMapRefreshInProgress.isEmpty();
  }

  @Override
  public boolean collectionRefreshInProgress(final CollectionIdentifier identifier) {
    return collectionMapRefreshInProgress.contains(identifier);
  }

  /**
   * Applies the given topology if it is newer than the current topology, or if forceApply is true.
   *
   * @param newConfig the proposed topology to consider.
   * @return true if the proposed topology was applied.
   */
  private synchronized boolean checkAndApplyBucketTopology(final ClusterTopologyWithBucket newConfig, final boolean forceApply) {
    final String name = newConfig.bucket().name();

    if (shouldIgnore(currentConfig.bucketTopology(name), newConfig, forceApply)) {
      return false;
    }

    if (tainted(newConfig.bucket())) {
      keyValueRefresher.markTainted(name);
      clusterManagerRefresher.markTainted(name);
    } else {
      keyValueRefresher.markUntainted(name);
      clusterManagerRefresher.markUntainted(name);
    }

    eventBus.publish(new BucketConfigUpdatedEvent(core.context(), LegacyConfigHelper.toLegacyBucketConfig(newConfig)));
    currentConfig.setBucketConfig(newConfig);
    return true;
  }

  private static boolean tainted(BucketTopology bucketTopology) {
    return bucketTopology instanceof CouchbaseBucketTopology &&
      ((CouchbaseBucketTopology) bucketTopology).partitionsForward().isPresent();
  }

  /**
   * Applies the given topology if it is newer than the current topology, or if forceApply is true.
   *
   * @param topology the proposed topology to consider.
   * @return true if the proposed topology was applied.
   */
  private synchronized boolean checkAndApplyGlobalTopology(final ClusterTopology topology, final boolean forceApply) {
    if (shouldIgnore(currentConfig.globalTopology(), topology, forceApply)) {
      return false;
    }

    eventBus.publish(new GlobalConfigUpdatedEvent(core.context(), new GlobalConfig(topology)));
    currentConfig.setGlobalConfig(topology);
    updateSeedNodeList(topology);
    return true;
  }

  private boolean shouldIgnore(
    @Nullable ClusterTopology current,
    ClusterTopology proposed,
    boolean forceApply
  ) {
    if (forceApply || current == null || proposed.revision().newerThan(current.revision())) {
      return false;
    }

    String bucketName = proposed instanceof ClusterTopologyWithBucket ? proposed.requireBucket().bucket().name() : null;
    eventBus.publish(new ConfigIgnoredEvent(
      core.context(),
      ConfigIgnoredEvent.Reason.OLD_OR_SAME_REVISION,
      Optional.empty(),
      Optional.empty(),
      Optional.ofNullable(bucketName)
    ));

    return true;
  }

  /**
   * Helper method to take the current config and update the seed node list with the latest topology.
   */
  private void updateSeedNodeList(ClusterTopology topology) {
    Set<SeedNode> seedNodes = topology.nodes().stream()
      .filter(it -> it.has(ServiceType.KV))
      .map(it -> SeedNode.create(
        it.host(),
        Optional.of(it.ports().get(ServiceType.KV)),
        Optional.ofNullable(it.ports().get(ServiceType.MANAGER))
      ))
      .collect(toSet());

    if (seedNodes.isEmpty()) {
      log.warn("Cluster topology has no eligible seed nodes; skipping seed node update.");
    } else {
      eventBus.publish(new SeedNodesUpdatedEvent(core.context(), currentSeedNodes(), seedNodes));
      setSeedNodes(seedNodes);
    }
  }

  /**
   * Pushes out the current configuration to all config subscribers.
   * <p>
   * Implementation Note: This method needs to be synchronized in order to prevent
   * {@link reactor.core.publisher.Sinks.EmitResult#FAIL_NON_SERIALIZED} from happening. All other results
   * should not be happening, but just to be sure we log them as WARN, so we have a chance to debug them in the field.
   */
  private synchronized void pushConfig(boolean ignoreShutdown) {
    if (ignoreShutdown || !shutdown.get()) {
      Sinks.EmitResult emitResult = configsSink.tryEmitNext(currentConfig);
      if (emitResult != Sinks.EmitResult.OK) {
        eventBus.publish(new ConfigPushFailedEvent(core.context(), emitResult));
      }
    } else {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.ALREADY_SHUTDOWN,
        Optional.empty(),
        Optional.empty(),
        Optional.empty()));
    }
  }

  @Override
  public void republishCurrentConfig() {
    pushConfig(false);
  }

  /**
   * Registers the given bucket for refreshing.
   *
   * <p>Note that this changes the implementation from the 1.x series a bit. In the past, whatever
   * loader succeeded automatically registered the same type of refresher. This is still the case
   * for situations like a memcache bucket, but in cases where we bootstrap from i.e. a query node
   * only the manager loader will work but we'll be able to use the key value refresher going
   * forward.</p>
   *
   * <p>As a result, this method is a bit more intelligent in selecting the right refresher based
   * on the loaded configuration.</p>
   *
   * @param bucket the name of the bucket.
   * @return a {@link Mono} once registered.
   */
  protected Mono<Void> registerRefresher(final String bucket) {
    return Mono.defer(() -> {
      BucketConfig config = currentConfig.bucketConfig(bucket);
      if (config == null) {
        return Mono.error(new CouchbaseException("Bucket for registration does not exist, "
          + "this is an error! Please report"));
      }

      if (config instanceof CouchbaseBucketConfig) {
        return keyValueRefresher.register(bucket);
      } else {
        return clusterManagerRefresher.register(bucket);
      }
    });
  }

  @Override
  public boolean globalConfigLoadInProgress() {
    return globalConfigLoadInProgress;
  }

  @Override
  public boolean bucketConfigLoadInProgress() {
    return bucketConfigLoadInProgress.get() > 0;
  }

  @Override
  public void signalConfigRefreshFailed(final ConfigRefreshFailure failure) {
    if (failure == ConfigRefreshFailure.ALL_NODES_TRIED_ONCE_WITHOUT_SUCCESS) {
      handlePotentialDnsSrvRefresh();
    }
  }

  @Override
  public synchronized void signalConfigChanged() {
    configPollTrigger.tryEmitNext(TRIGGERED_BY_CONFIG_CHANGE_NOTIFICATION);
  }

  @Override
  public Flux<Long> configChangeNotifications() {
    return configPollTrigger.asFlux();
  }

  /**
   * Performs DNS SRV refresh.
   * <p>
   * The config provider got a signal that config refresh failed on all nodes, so it will now try to proactively
   * perform DNS SRV refresh (if possible) to recover a sane state.
   */
  private synchronized void handlePotentialDnsSrvRefresh() {
    final CoreContext ctx = core.context();
    final CoreEnvironment env = ctx.environment();
    boolean isValidDnsSrv = connectionString.isValidDnsSrv()
      && env.ioConfig().dnsSrvEnabled();
    boolean tlsEnabled = env.securityConfig().tlsEnabled();
    boolean enoughTimeElapsed = lastDnsSrvLookup.hasElapsed(MIN_TIME_BETWEEN_DNS_LOOKUPS);
    boolean refreshAllowed = isValidDnsSrv && enoughTimeElapsed;

    if (refreshAllowed) {
      lastDnsSrvLookup = NanoTimestamp.now();
      NanoTimestamp started = NanoTimestamp.now();

      // DNS SRV lookups are blocking, so move it to its own thread for execution.
      Schedulers.boundedElastic().schedule(() -> {
        try {
          List<String> foundNodes = performDnsSrvLookup(tlsEnabled);

          if (foundNodes.isEmpty()) {
            env.eventBus().publish(new DnsSrvRefreshAttemptFailedEvent(started.elapsed(),
              ctx, DnsSrvRefreshAttemptFailedEvent.Reason.NO_NEW_SEEDS_RETURNED, null));
            return;
          }

          Set<SeedNode> seedNodes = foundNodes.stream().map(SeedNode::create).collect(Collectors.toSet());

          ProposedGlobalConfigContext foundGlobalConfig = fetchGlobalConfigs(seedNodes, tlsEnabled,
            true, false).block();
          if (foundGlobalConfig != null) {
            proposeGlobalConfig(foundGlobalConfig.forceOverride());
          }
          for (String name : keyValueRefresher.registered()) {
            ProposedBucketConfigContext bucketConfig = fetchBucketConfigs(name, seedNodes, tlsEnabled).block();
            if (bucketConfig != null) {
              proposeBucketConfig(bucketConfig.forceOverride());
            }
          }

          env.eventBus().publish(new DnsSrvRefreshAttemptCompletedEvent(started.elapsed(), ctx, foundNodes));
        } catch (Exception e) {
          env.eventBus().publish(new DnsSrvRefreshAttemptFailedEvent(started.elapsed(),
            ctx, DnsSrvRefreshAttemptFailedEvent.Reason.OTHER, e));
        }
      });
    }
  }

  /**
   * Performs DNS SRV lookups - and can be overridden by test classes.
   *
   * @return the (potentially empty) DNS SRV records after the lookup.
   */
  protected List<String> performDnsSrvLookup(boolean tlsEnabled) throws NamingException {
    return fromDnsSrvOrThrowIfTlsRequired(connectionString.hosts().get(0).host(), tlsEnabled);
  }

  private Mono<ProposedBucketConfigContext> fetchBucketConfigs(final String name, final Set<SeedNode> seedNodes,
                                                              final boolean tls) {
    int kvPort = tls ? DEFAULT_KV_TLS_PORT : DEFAULT_KV_PORT;
    int managerPort = tls ? DEFAULT_MANAGER_TLS_PORT : DEFAULT_MANAGER_PORT;

    return Flux
      .range(1, Math.min(MAX_PARALLEL_LOADERS, seedNodes.size()))
      .flatMap(index -> Flux
        .fromIterable(seedNodes)
        .take(Math.min(index, seedNodes.size()))
        .last()
        .flatMap(seed -> {
          NodeIdentifier identifier = NodeIdentifier.forBootstrap(
            seed.address(),
            seed.clusterManagerPort().orElse(DEFAULT_MANAGER_PORT)
          );

          final int mappedKvPort = seed.kvPort().orElse(kvPort);
          final int mappedManagerPort = seed.clusterManagerPort().orElse(managerPort);

          return loadBucketConfigForSeed(identifier, mappedKvPort, mappedManagerPort, name);
        })
        // Exponential backoff for certain errors.
        .retryWhen(Retry
          .backoff(Long.MAX_VALUE, Duration.ofMillis(500))
          .maxBackoff(Duration.ofSeconds(10))
          .filter(bucketConfigLoadRetryFilter(name, t -> isInstanceOfAnyOf(t,
            BucketNotFoundDuringLoadException.class,
            BucketNotReadyDuringLoadException.class,
            NoAccessDuringConfigLoadException.class
          )))
        )
        // Short fixed delay for the others we want to retry.
        .retryWhen(Retry
          .fixedDelay(Long.MAX_VALUE, Duration.ofMillis(10))
          .filter(bucketConfigLoadRetryFilter(name, t -> !(t instanceof UnsupportedConfigMechanismException)))
        )
      )
      .next();
  }

  /**
   * Wraps a retry filter with common logic that checks for shutdown and publishes events.
   */
  private Predicate<? super Throwable> bucketConfigLoadRetryFilter(
    String bucketName,
    Predicate<? super Throwable> errorFilter
  ) {
    return t -> {
      if (shutdown.get()) {
        throw new AlreadyShutdownException();
      }

      boolean retry = errorFilter.test(t);
      if (retry) {
        eventBus.publish(new BucketOpenRetriedEvent(bucketName, Duration.ZERO, core.context(), t));
      }
      return retry;
    };
  }

  private static boolean isInstanceOfAnyOf(Object o, Class<?>... candidates) {
    return Arrays.stream(candidates).anyMatch(it -> it.isInstance(o));
  }

  private Mono<ProposedGlobalConfigContext> fetchGlobalConfigs(final Set<SeedNode> seedNodes, final boolean tls,
                                                               boolean allowStaleSeeds, boolean retryTimeouts) {
    final AtomicBoolean hasErrored = new AtomicBoolean();
    int kvPort = tls ? DEFAULT_KV_TLS_PORT : DEFAULT_KV_PORT;

    return Flux
      .range(1, Math.min(MAX_PARALLEL_LOADERS, seedNodes.size()))
      .flatMap(index -> Flux
        .fromIterable(seedNodes)
        .take(Math.min(index, seedNodes.size()))
        .last()
        .flatMap(seed -> {
          NanoTimestamp start = NanoTimestamp.now();

          if (!allowStaleSeeds && !currentSeedNodes().contains(seed)) {
            // Since updating the seed nodes can race loading the global config, double check that the
            // node we are about to load is still part of the list.
            return Mono.empty();
          }

          NodeIdentifier identifier = NodeIdentifier.forBootstrap(
            seed.address(),
            seed.clusterManagerPort().orElse(DEFAULT_MANAGER_PORT)
          );
          return globalLoader
            .load(identifier, seed.kvPort().orElse(kvPort))
            .doOnError(throwable -> core.context().environment().eventBus().publish(new IndividualGlobalConfigLoadFailedEvent(
              start.elapsed(),
              core.context(),
              throwable,
              seed.address()
            )));
        })
        .retryWhen(Retry.from(companion -> companion.flatMap(rs -> {
          Throwable f = rs.failure();

          if (shutdown.get()) {
            return Mono.error(new AlreadyShutdownException());
          }
          if (f instanceof UnsupportedConfigMechanismException) {
            return Mono.error(Exceptions.propagate(f));
          }
          if (!retryTimeouts && f.getCause() instanceof TimeoutException) {
            return Mono.error(f.getCause());
          }

          Duration delay = Duration.ofMillis(1);
          eventBus.publish(new GlobalConfigRetriedEvent(delay, core.context(), f));
          return Mono
            .just(rs.totalRetries())
            .delayElement(delay, core.context().environment().scheduler());
        })))
        .onErrorResume(throwable -> {
          if (hasErrored.compareAndSet(false, true)) {
            return Mono.error(throwable);
          }
          return Mono.empty();
        }))
      .next();
  }

  private Set<SeedNode> currentSeedNodes() {
    return currentSeedNodes.get();
  }

  /**
   * Pushes out the current seed nodes to all seed node subscribers.
   * <p>
   * Implementation Note: This method needs to be synchronized in order to prevent
   * {@link reactor.core.publisher.Sinks.EmitResult#FAIL_NON_SERIALIZED} from happening. All other results
   * should not be happening, but just to be sure we log them as WARN, so we have a chance to debug them in the field.
   */
  private synchronized Sinks.EmitResult setSeedNodes(Set<SeedNode> seedNodes) {
    currentSeedNodes.set(seedNodes);
    Sinks.EmitResult emitResult = seedNodesSink.tryEmitNext(seedNodes);

    if (emitResult != Sinks.EmitResult.OK) {
      eventBus.publish(new SeedNodesUpdateFailedEvent(core.context(), emitResult));
    }

    return emitResult;
  }

}
