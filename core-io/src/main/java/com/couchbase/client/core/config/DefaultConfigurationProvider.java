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
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.BucketNotFoundDuringLoadException;
import com.couchbase.client.core.error.BucketNotReadyDuringLoadException;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.CouchbaseException;
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
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.core.util.UnsignedLEB128;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import javax.naming.NamingException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.Reactor.emitFailureHandler;
import static com.couchbase.client.core.util.CbCollections.copyToUnmodifiableSet;
import static com.couchbase.client.core.util.ConnectionStringUtil.fromDnsSrvOrThrowIfTlsRequired;
import static java.util.Collections.unmodifiableSet;

/**
 * The standard {@link ConfigurationProvider} that is used by default.
 *
 * <p>This provider has been around since the 1.x days, but it has been revamped and reworked
 * for the 2.x breakage - the overall functionality remains very similar though.</p>
 *
 * @since 1.0.0
 */
public class DefaultConfigurationProvider implements ConfigurationProvider {

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

  private final KeyValueBucketLoader keyValueLoader;
  private final ClusterManagerBucketLoader clusterManagerLoader;
  private final KeyValueBucketRefresher keyValueRefresher;
  private final ClusterManagerBucketRefresher clusterManagerRefresher;
  private final GlobalLoader globalLoader;
  private final GlobalRefresher globalRefresher;

  private final Sinks.Many<ClusterConfig> configsSink = Sinks.many().replay().latest();
  private final Flux<ClusterConfig> configs = configsSink.asFlux();
  private final ClusterConfig currentConfig = new ClusterConfig();

  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final CollectionMap collectionMap = new CollectionMap();

  private final AtomicBoolean alternateAddrChecked = new AtomicBoolean(false);

  private volatile boolean globalConfigLoadInProgress = false;
  private final AtomicInteger bucketConfigLoadInProgress = new AtomicInteger();

  /**
   * Made package private so it can be inspected during testing.
   */
  final Set<CollectionIdentifier> collectionMapRefreshInProgress = ConcurrentHashMap.newKeySet();

  /**
   * Stores the current seed nodes used to bootstrap buckets and global configs.
   */
  private final AtomicReference<Set<SeedNode>> currentSeedNodes;
  private final Sinks.Many<Set<SeedNode>> seedNodesSink = Sinks.many().replay().latest();
  private final Flux<Set<SeedNode>> seedNodes = seedNodesSink.asFlux();

  private final ConnectionString connectionString;

  private volatile NanoTimestamp lastDnsSrvLookup = NanoTimestamp.never();

  public DefaultConfigurationProvider(final Core core, final Set<SeedNode> seedNodes) {
    this(core, seedNodes, null);
  }

  /**
   * Creates a new configuration provider.
   *
   * @param core the core against which all ops are executed.
   */
  public DefaultConfigurationProvider(final Core core, final Set<SeedNode> seedNodes, final String connectionString) {
    this.core = core;
    eventBus = core.context().environment().eventBus();
    this.connectionString = connectionString != null ? ConnectionString.create(connectionString) : null;

    // Don't publish the initial seed nodes, since they probably came from the user
    // and might not be KV nodes, or might have incomplete port information.
    this.currentSeedNodes = new AtomicReference<>(copyToUnmodifiableSet(seedNodes));

    keyValueLoader = new KeyValueBucketLoader(core);
    clusterManagerLoader = new ClusterManagerBucketLoader(core);
    keyValueRefresher = new KeyValueBucketRefresher(this, core);
    clusterManagerRefresher = new ClusterManagerBucketRefresher(this, core);
    globalLoader = new GlobalLoader(core);
    globalRefresher = new GlobalRefresher(this, core);

    // Start with pushing the current config into the sink for all subscribers currently attached.
    configsSink.emitNext(currentConfig, emitFailureHandler());
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

  @Override
  public Mono<Void> openBucket(final String name) {
    return Mono.defer(() -> {
      if (!shutdown.get()) {
        bucketConfigLoadInProgress.incrementAndGet();
        boolean tls = core.context().environment().securityConfig().tlsEnabled();

        return fetchBucketConfigs(name, currentSeedNodes.get(), tls)
          .switchIfEmpty(Mono.error(
            new ConfigException("Could not locate a single bucket configuration for bucket: " + name)
          ))
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
   * @param alternateAddress the alternate address, if present.
   * @return returns the bucket config context if present, or an error.
   */
  protected Mono<ProposedBucketConfigContext> loadBucketConfigForSeed(NodeIdentifier identifier, int mappedKvPort,
                                                                      int mappedManagerPort, String name,
                                                                      Optional<String> alternateAddress) {
    return keyValueLoader
      .load(identifier, mappedKvPort, name, alternateAddress)
      .onErrorResume(t -> {
        boolean removedWhileOpInFlight = t instanceof ConfigException
          && t.getCause() instanceof RequestCanceledException
          && ((RequestCanceledException) t.getCause()).reason() == CancellationReason.TARGET_NODE_REMOVED;
        boolean seedNodeOutdated = t instanceof SeedNodeOutdatedException;

        if (removedWhileOpInFlight || seedNodeOutdated) {
          return Mono.error(t);
        }
        return clusterManagerLoader.load(
          identifier, mappedManagerPort, name, alternateAddress
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

  /**
   * Maps a logical address to its alternate address, if present.
   *
   * @param a the input address.
   * @param seed the seed node to check against.
   * @param tls if TLS is enabled or not.
   * @param alternatePorts where to store the alternate ports as a side effect.
   * @return the mapped hostname.
   */
  private String mapAlternateAddress(final String a, final SeedNode seed, final boolean tls,
                                     final AtomicReference<Map<ServiceType, Integer>> alternatePorts) {
    ClusterConfig c = currentConfig;
    if (c.globalConfig() != null) {
      for (PortInfo pi : c.globalConfig().portInfos()) {
        if (seed.address().equals(pi.hostname())) {
          alternatePorts.set(tls
            ? pi.alternateAddresses().get(a).sslServices()
            : pi.alternateAddresses().get(a).services()
          );
          return pi.alternateAddresses().get(a).hostname();
        }
      }
    }

    List<NodeInfo> nodeInfos = c
      .bucketConfigs()
      .values()
      .stream()
      .flatMap(bc -> bc.nodes().stream()).collect(Collectors.toList());
    for (NodeInfo ni : nodeInfos) {
      if (ni.hostname().equals(seed.address())) {
        alternatePorts.set(tls
          ? ni.alternateAddresses().get(a).sslServices()
          : ni.alternateAddresses().get(a).services()
        );
        return ni.alternateAddresses().get(a).hostname();
      }
    }

    return null;
  }

  @Override
  public Mono<Void> loadAndRefreshGlobalConfig() {
    return Mono.defer(() -> {
      if (!shutdown.get()) {
        globalConfigLoadInProgress = true;
        boolean tls = core.context().environment().securityConfig().tlsEnabled();

        return fetchGlobalConfigs(currentSeedNodes.get(), tls, false, true).switchIfEmpty(Mono.error(
            new ConfigException("Could not locate a single global configuration")
          ))
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

  @Override
  public void proposeBucketConfig(final ProposedBucketConfigContext ctx) {
    if (!shutdown.get()) {
      try {
        BucketConfig config = BucketConfigParser.parse(
          ctx.config(),
          core.context().environment(),
          ctx.origin()
        );
        checkAndApplyConfig(config, ctx.forcesOverride());
      } catch (Exception ex) {
        eventBus.publish(new ConfigIgnoredEvent(
          core.context(),
          ConfigIgnoredEvent.Reason.PARSE_FAILURE,
          Optional.of(ex),
          Optional.of(ctx.config()),
          Optional.of(ctx.bucketName())
        ));
      }
    } else {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.ALREADY_SHUTDOWN,
        Optional.empty(),
        Optional.of(ctx.config()),
        Optional.of(ctx.bucketName())
      ));
    }
  }

  @Override
  public void proposeGlobalConfig(final ProposedGlobalConfigContext ctx) {
    if (!shutdown.get()) {
      try {
        GlobalConfig config = GlobalConfigParser.parse(ctx.config(), ctx.origin());
        checkAndApplyConfig(config, ctx.forcesOverride());
      } catch (Exception ex) {
        eventBus.publish(new ConfigIgnoredEvent(
          core.context(),
          ConfigIgnoredEvent.Reason.PARSE_FAILURE,
          Optional.of(ex),
          Optional.of(ctx.config()),
          Optional.empty()
        ));
      }
    } else {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.ALREADY_SHUTDOWN,
        Optional.empty(),
        Optional.of(ctx.config()),
        Optional.empty()
      ));
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
   * Analyzes the given config and decides if to apply it (and does so if needed).
   *
   * @param newConfig the config to apply.
   */
  private void checkAndApplyConfig(final BucketConfig newConfig, final boolean force) {
    final String name = newConfig.name();
    final BucketConfig oldConfig = currentConfig.bucketConfig(name);

    if (!force && oldConfig != null && configIsOlderOrSame(oldConfig.revEpoch(), newConfig.revEpoch(), oldConfig.rev(), newConfig.rev())) {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.OLD_OR_SAME_REVISION,
        Optional.empty(),
        Optional.empty(),
        Optional.of(newConfig.name())
      ));
      return;
    }

    if (newConfig.tainted()) {
      keyValueRefresher.markTainted(name);
      clusterManagerRefresher.markTainted(name);
    } else {
      keyValueRefresher.markUntainted(name);
      clusterManagerRefresher.markUntainted(name);
    }

    eventBus.publish(new BucketConfigUpdatedEvent(core.context(), newConfig));
    currentConfig.setBucketConfig(newConfig);
    checkAlternateAddress();
    updateSeedNodeList();
    pushConfig(false);
  }

  /**
   * Analyzes the given config and decides if to apply it (and does so if needed).
   *
   * @param newConfig the config to apply.
   */
  private void checkAndApplyConfig(final GlobalConfig newConfig, final boolean force) {
    final GlobalConfig oldConfig = currentConfig.globalConfig();

    if (!force && oldConfig != null && configIsOlderOrSame(oldConfig.revEpoch(), newConfig.revEpoch(), oldConfig.rev(), newConfig.rev())) {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.OLD_OR_SAME_REVISION,
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
      ));
      return;
    }

    eventBus.publish(new GlobalConfigUpdatedEvent(core.context(), newConfig));
    currentConfig.setGlobalConfig(newConfig);
    checkAlternateAddress();
    updateSeedNodeList();
    pushConfig(false);
  }

  /**
   * Helper method to check if a bucket or global config is older or the same (and can be ignored).
   *
   * @param oldRevEpoch the rev epoch from the old config, might be 0.
   * @param newRevEpoch the rev epoch of the new config, might be 0.
   * @param oldRev the rev of the old config, might be 0 but likely not unless with super old servers.
   * @param newRev the rev of the new config, might be 0 but likely not unless with super old servers.
   * @return true of old or same and safe to be ignored.
   */
  private boolean configIsOlderOrSame(long oldRevEpoch, long newRevEpoch, long oldRev, long newRev) {
    if (newRevEpoch < oldRevEpoch) {
      // The new rev epoch is definitely older, so ignore right away.
      return true;
    } else if (newRevEpoch > oldRevEpoch) {
      // The new rev epoch is definitely newer, so we definitely need that config.
      return false;
    }

    // Now we know that the epochs are the same (could be 0 as well, so not present), we ned to compare
    // the revs themselves to figure out if it is newer or older.
    return newRev > 0 && newRev <= oldRev;
  }

  /**
   * Helper method to take the current config and update the seed node list with the latest topology.
   *
   * <p>If we have a global config it is used for simplicity reasons. Otherwise we iterate the configs and collect
   * all the nodes to build the list.</p>
   */
  private void updateSeedNodeList() {
    ClusterConfig config = currentConfig;
    boolean tlsEnabled = core.context().environment().securityConfig().tlsEnabled();

    if (config.globalConfig() != null) {
      Set<SeedNode> seedNodes = unmodifiableSet(config.globalConfig().portInfos().stream().map(ni -> {
        Map<ServiceType, Integer> ports = tlsEnabled ? ni.sslPorts() : ni.ports();

        if (!ports.containsKey(ServiceType.KV)) {
          // We  only want seed nodes where the KV service is enabled
          return null;
        }

        return SeedNode.create(
          ni.hostname(),
          Optional.ofNullable(ports.get(ServiceType.KV)),
          Optional.ofNullable(ports.get(ServiceType.MANAGER))
        );
      }).filter(Objects::nonNull).collect(Collectors.toSet()));

      if (!seedNodes.isEmpty()) {
        eventBus.publish(new SeedNodesUpdatedEvent(core.context(), currentSeedNodes(), seedNodes));
        setSeedNodes(seedNodes);
      }

      return;
    }

    Set<SeedNode> seedNodes = unmodifiableSet(config
      .bucketConfigs()
      .values()
      .stream()
      .flatMap(bc -> bc.nodes().stream())
      .map(ni -> {
        Map<ServiceType, Integer> ports = tlsEnabled ? ni.sslServices() : ni.services();

        if (!ports.containsKey(ServiceType.KV)) {
          // We  only want seed nodes where the KV service is enabled
          return null;
        }

        return SeedNode.create(
          ni.hostname(),
          Optional.ofNullable(ports.get(ServiceType.KV)),
          Optional.ofNullable(ports.get(ServiceType.MANAGER))
        );
      }).filter(Objects::nonNull).collect(Collectors.toSet()));

    if (!seedNodes.isEmpty()) {
      eventBus.publish(new SeedNodesUpdatedEvent(core.context(), currentSeedNodes(), seedNodes));
      setSeedNodes(seedNodes);
    }
  }

  /**
   * Check and apply alternate address setting if it hasn't been done already.
   */
  private synchronized void checkAlternateAddress() {
    if (alternateAddrChecked.compareAndSet(false, true)) {
      String resolved = determineNetworkResolution(
        extractAlternateAddressInfos(currentConfig),
        core.context().environment().ioConfig().networkResolution(),
        currentSeedNodes().stream().map(SeedNode::address).collect(Collectors.toSet())
      );
      core.context().alternateAddress(Optional.ofNullable(resolved));
    }
  }

  /**
   * Helper method to turn either the port info or the node info into a list of hosts to use for the
   * alternate address resolution.
   *
   * @return a list of hostname/alternate address mappings.
   */
  public static List<AlternateAddressHolder> extractAlternateAddressInfos(final ClusterConfig config) {
    Stream<AlternateAddressHolder> holders;

    if (config.globalConfig() != null) {
      holders = config
        .globalConfig()
        .portInfos()
        .stream()
        .map(pi -> new AlternateAddressHolder(pi.hostname(), pi.alternateAddresses()));
    } else {
      holders = config.bucketConfigs()
        .values()
        .stream()
        .flatMap(bc -> bc.nodes().stream())
        .map(ni -> new AlternateAddressHolder(ni.hostname(), ni.alternateAddresses()));
    }

    return holders.collect(Collectors.toList());
  }

  /**
   * Helper method to figure out which network resolution should be used.
   * <p>
   * If DEFAULT is selected, then null is returned which is equal to the "internal" or default
   * config mode. If AUTO is used then we perform the select heuristic based off of the seed
   * hosts given. All other resolution settings (i.e. EXTERNAL) are returned directly and are
   * considered to be part of the alternate address configs.
   *
   * @param nodes the list of nodes to check.
   * @param nr the network resolution setting from the environment
   * @param seedHosts the seed hosts from bootstrap for autoconfig.
   * @return the found setting if external is used, null if internal/default is used.
   */
  public static String determineNetworkResolution(final List<AlternateAddressHolder> nodes, final NetworkResolution nr,
                                                  final Set<String> seedHosts) {
    if (nr.equals(NetworkResolution.DEFAULT)) {
      return null;
    } else if (nr.equals(NetworkResolution.AUTO)) {
      for (AlternateAddressHolder info : nodes) {
        if (seedHosts.contains(info.hostname())) {
          return null;
        }

        Map<String, AlternateAddress> aa = info.alternateAddresses();
        if (aa != null && !aa.isEmpty()) {
          for (Map.Entry<String, AlternateAddress> entry : aa.entrySet()) {
            AlternateAddress alternateAddress = entry.getValue();
            if (alternateAddress != null && seedHosts.contains(alternateAddress.hostname())) {
              return entry.getKey();
            }
          }
        }
      }
      return null;
    } else {
      return nr.name();
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
    }
    else {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.ALREADY_SHUTDOWN,
        Optional.empty(),
        Optional.empty(),
        Optional.empty()));
    }
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

  /**
   * Performs DNS SRV refresh.
   * <p>
   * The config provider got a signal that config refresh failed on all nodes, so it will now try to proactively
   * perform DNS SRV refresh (if possible) to recover a sane state.
   */
  private synchronized void handlePotentialDnsSrvRefresh() {
    final CoreContext ctx = core.context();
    final CoreEnvironment env = ctx.environment();
    boolean isValidDnsSrv = connectionString != null
      && connectionString.isValidDnsSrv()
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
    return fromDnsSrvOrThrowIfTlsRequired(connectionString.hosts().get(0).hostname(), tlsEnabled);
  }

  private Mono<ProposedBucketConfigContext> fetchBucketConfigs(final String name, final Set<SeedNode> seedNodes,
                                                              final boolean tls) {
    int kvPort = tls ? DEFAULT_KV_TLS_PORT : DEFAULT_KV_PORT;
    int managerPort = tls ? DEFAULT_MANAGER_TLS_PORT : DEFAULT_MANAGER_PORT;
    final Optional<String> alternate = core.context().alternateAddress();

    return Flux
      .range(1, Math.min(MAX_PARALLEL_LOADERS, seedNodes.size()))
      .flatMap(index -> Flux
        .fromIterable(seedNodes)
        .take(Math.min(index, seedNodes.size()))
        .last()
        .flatMap(seed -> {
          NodeIdentifier identifier = new NodeIdentifier(seed.address(), seed.clusterManagerPort().orElse(DEFAULT_MANAGER_PORT));

          final AtomicReference<Map<ServiceType, Integer>> alternatePorts = new AtomicReference<>();
          final Optional<String> alternateAddress = alternate.map(a -> mapAlternateAddress(a, seed, tls, alternatePorts));

          final int mappedKvPort;
          final int mappedManagerPort;

          if (alternateAddress.isPresent()) {
            Map<ServiceType, Integer> ports = alternatePorts.get();
            mappedKvPort = ports.get(ServiceType.KV);
            mappedManagerPort = ports.get(ServiceType.MANAGER);
          } else {
            mappedKvPort = seed.kvPort().orElse(kvPort);
            mappedManagerPort = seed.clusterManagerPort().orElse(managerPort);
          }

          return loadBucketConfigForSeed(identifier, mappedKvPort, mappedManagerPort, name, alternateAddress);
        })
        .retryWhen(
          Retry.from(companion -> companion.flatMap(rs -> {
            final Throwable f = rs.failure();
            if (shutdown.get()) {
              return Mono.error(new AlreadyShutdownException());
            }
            if (f instanceof UnsupportedConfigMechanismException) {
              return Mono.error(Exceptions.propagate(f));
            }

            boolean bucketNotFound = f instanceof BucketNotFoundDuringLoadException;
            boolean bucketNotReady = f instanceof BucketNotReadyDuringLoadException;

            // For bucket not found or not ready wait a bit longer, retry the rest quickly
            Duration delay = bucketNotFound || bucketNotReady
              ? Duration.ofMillis(500)
              : Duration.ofMillis(1);
            eventBus.publish(new BucketOpenRetriedEvent(name, delay, core.context(), f));
            return Mono
              .just(rs.totalRetries())
              .delayElement(delay, core.context().environment().scheduler());
          })))
      )
      .next();
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

          NodeIdentifier identifier = new NodeIdentifier(seed.address(), seed.clusterManagerPort().orElse(DEFAULT_MANAGER_PORT));
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

  /**
   * Visible for testing.
   */
  Set<SeedNode> currentSeedNodes() {
    return currentSeedNodes.get();
  }

  /**
   * Pushes out the current seed nodes to all seed node subscribers.
   * <p>
   * Implementation Note: This method needs to be synchronized in order to prevent
   * {@link reactor.core.publisher.Sinks.EmitResult#FAIL_NON_SERIALIZED} from happening. All other results
   * should not be happening, but just to be sure we log them as WARN, so we have a chance to debug them in the field.
   */
  private synchronized void setSeedNodes(Set<SeedNode> seedNodes) {
    currentSeedNodes.set(seedNodes);
    Sinks.EmitResult emitResult = seedNodesSink.tryEmitNext(seedNodes);

    if (emitResult != Sinks.EmitResult.OK) {
      eventBus.publish(new SeedNodesUpdateFailedEvent(core.context(), emitResult));
    }
  }

  /**
   * This class is needed since both port info and node info need to be abstracted for alternate address resolving.
   */
  public static class AlternateAddressHolder {

    private final String hostname;
    private final Map<String, AlternateAddress> alternateAddresses;

    AlternateAddressHolder(final String hostname, final Map<String, AlternateAddress> alternateAddresses) {
      this.hostname = hostname;
      this.alternateAddresses = alternateAddresses;
    }

    public String hostname() {
      return hostname;
    }

    public Map<String, AlternateAddress> alternateAddresses() {
      return alternateAddresses;
    }
  }

}
