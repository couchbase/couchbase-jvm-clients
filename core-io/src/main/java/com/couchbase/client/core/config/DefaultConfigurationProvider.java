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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.config.CollectionMapDecodingFailedEvent;
import com.couchbase.client.core.cnc.events.config.ConfigIgnoredEvent;
import com.couchbase.client.core.cnc.events.config.BucketConfigUpdatedEvent;
import com.couchbase.client.core.cnc.events.config.GlobalConfigUpdatedEvent;
import com.couchbase.client.core.config.loader.ClusterManagerBucketLoader;
import com.couchbase.client.core.config.loader.GlobalLoader;
import com.couchbase.client.core.config.loader.KeyValueBucketLoader;
import com.couchbase.client.core.config.refresher.ClusterManagerBucketRefresher;
import com.couchbase.client.core.config.refresher.GlobalRefresher;
import com.couchbase.client.core.config.refresher.KeyValueBucketRefresher;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.CollectionsNotAvailableException;
import com.couchbase.client.core.error.ConfigException;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetCollectionManifestRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.UnsignedLEB128;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  private final DirectProcessor<ClusterConfig> configs = DirectProcessor.create();
  private final FluxSink<ClusterConfig> configsSink = configs.sink();
  private final ClusterConfig currentConfig = new ClusterConfig();

  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final CollectionMap collectionMap = new CollectionMap();

  private final AtomicBoolean alternateAddrChecked = new AtomicBoolean(false);

  /**
   * Stores the current seed nodes used to bootstrap buckets and global configs.
   */
  private final AtomicReference<Set<SeedNode>> seedNodes;

  /**
   * Creates a new configuration provider.
   *
   * @param core the core against which all ops are executed.
   */
  public DefaultConfigurationProvider(final Core core) {
    this.core = core;
    eventBus = core.context().environment().eventBus();
    seedNodes = new AtomicReference<>(new HashSet<>(core.context().environment().seedNodes()));

    keyValueLoader = new KeyValueBucketLoader(core);
    clusterManagerLoader = new ClusterManagerBucketLoader(core);
    keyValueRefresher = new KeyValueBucketRefresher(this, core);
    clusterManagerRefresher = new ClusterManagerBucketRefresher(this, core);
    globalLoader = new GlobalLoader(core);
    globalRefresher = new GlobalRefresher(this, core);

    // Start with pushing the current config into the sink for all subscribers currently attached.
    configsSink.next(currentConfig);
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
  public Mono<Void> openBucket(final String name) {
    return Mono.defer(() -> {
      if (!shutdown.get()) {

        boolean tls = core.context().environment().securityConfig().tlsEnabled();
        int kvPort = tls ? DEFAULT_KV_TLS_PORT : DEFAULT_KV_PORT;
        int managerPort = tls ? DEFAULT_MANAGER_TLS_PORT : DEFAULT_MANAGER_PORT;
        final Optional<String> alternate = core.context().alternateAddress();

        return Flux
          .fromIterable(seedNodes.get())
          .take(MAX_PARALLEL_LOADERS)
          .flatMap(seed -> {
            NodeIdentifier identifier = new NodeIdentifier(seed.address(), seed.httpPort().orElse(DEFAULT_MANAGER_PORT));

            final Optional<String> alternateAddress = alternate.map(a -> {
              ClusterConfig c = currentConfig;
              if (c.globalConfig() != null) {
                for (PortInfo pi : c.globalConfig().portInfos()) {
                  if (seed.address().equals(pi.hostname())) {
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
                  return ni.alternateAddresses().get(a).hostname();
                }
              }

              return null;
            });

            return keyValueLoader
              .load(identifier, seed.kvPort().orElse(kvPort), name, alternateAddress)
              .onErrorResume(t -> clusterManagerLoader.load(
                identifier, seed.httpPort().orElse(managerPort), name, alternateAddress
              ));
          })
          .take(1)
          .switchIfEmpty(Mono.error(
            new ConfigException("Could not locate a single bucket configuration for bucket: " + name)
          ))
          .map(ctx -> {
            proposeBucketConfig(ctx);
            return ctx;
          })
          .then(registerRefresher(name))
          .onErrorResume(t -> closeBucketIgnoreShutdown(name).then(Mono.error(t)));
      } else {
        return Mono.error(new AlreadyShutdownException());
      }
    });
  }

  @Override
  public Mono<Void> loadAndRefreshGlobalConfig() {
    return Mono.defer(() -> {
      if (!shutdown.get()) {

        boolean tls = core.context().environment().securityConfig().tlsEnabled();
        int kvPort = tls ? DEFAULT_KV_TLS_PORT : DEFAULT_KV_PORT;

        return Flux
          .fromIterable(seedNodes.get())
          .take(MAX_PARALLEL_LOADERS)
          .flatMap(seed -> {
            NodeIdentifier identifier = new NodeIdentifier(seed.address(), seed.httpPort().orElse(DEFAULT_MANAGER_PORT));
            return globalLoader.load(identifier, seed.kvPort().orElse(kvPort));
          })
          .take(1)
          .switchIfEmpty(Mono.error(
            new ConfigException("Could not locate a single global configuration")
          ))
          .map(ctx -> {
            proposeGlobalConfig(ctx);
            return ctx;
          })
          .then(globalRefresher.start());
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
        checkAndApplyConfig(config);
      } catch (Exception ex) {
        eventBus.publish(new ConfigIgnoredEvent(
          core.context(),
          ConfigIgnoredEvent.Reason.PARSE_FAILURE,
          Optional.of(ex),
          Optional.of(ctx.config())
        ));
      }
    } else {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.ALREADY_SHUTDOWN,
        Optional.empty(),
        Optional.of(ctx.config())
      ));
    }
  }

  @Override
  public void proposeGlobalConfig(final ProposedGlobalConfigContext ctx) {
    if (!shutdown.get()) {
      try {
        GlobalConfig config = GlobalConfigParser.parse(ctx.config(), ctx.origin());
        checkAndApplyConfig(config);
      } catch (Exception ex) {
        eventBus.publish(new ConfigIgnoredEvent(
          core.context(),
          ConfigIgnoredEvent.Reason.PARSE_FAILURE,
          Optional.of(ex),
          Optional.of(ctx.config())
        ));
      }
    } else {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.ALREADY_SHUTDOWN,
        Optional.empty(),
        Optional.of(ctx.config())
      ));
    }
  }

  @Override
  public Mono<Void> closeBucket(final String name) {
    return Mono.defer(() -> shutdown.get()
      ? Mono.error(new AlreadyShutdownException())
      : closeBucketIgnoreShutdown(name)
    );
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
   * @return completed mono once done.
   */
  private Mono<Void> closeBucketIgnoreShutdown(final String name) {
    return Mono
      .defer(() -> {
        currentConfig.deleteBucketConfig(name);
        pushConfig();
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
          .flatMap(bucketConfig -> closeBucketIgnoreShutdown(bucketConfig.name()))
          .then(Mono.defer(this::disableAndClearGlobalConfig))
          .doOnTerminate(() -> {
            // make sure to push a final, empty config before complete to give downstream
            // consumers a chance to clean up
            pushConfig();
            configsSink.complete();
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
  public Mono<Void> refreshCollectionMap(final String bucket, final boolean force) {
    if (!collectionMap.hasBucketMap(bucket) || force) {
      return Mono.defer(() -> {
        GetCollectionManifestRequest request = new GetCollectionManifestRequest(
          core.context().environment().timeoutConfig().kvTimeout(),
          core.context(),
          BestEffortRetryStrategy.INSTANCE,
          new CollectionIdentifier(bucket, Optional.empty(), Optional.empty())
        );
        core.send(request);
        return Reactor
          .wrap(request, request.response(), true)
          .flatMap(response -> {
            if (response.status().success() && response.manifest().isPresent()) {
              parseAndStoreCollectionsManifest(bucket, response.manifest().get());
              return Mono.empty();
            } else {
              if (response.status() == ResponseStatus.UNKNOWN) {
                return Mono.error(new CollectionsNotAvailableException());
              } else {
                return Mono.error(new CouchbaseException(response.toString()));
              }
            }
          });
      });
    } else {
      return Mono.empty();
    }
  }

  /**
   * Parses a raw collections manifest and stores it in the collections map.
   *
   * @param raw the raw manifest.
   */
  private void parseAndStoreCollectionsManifest(final String bucket, final String raw) {
    try {
      CollectionsManifest manifest = Mapper.reader().forType(CollectionsManifest.class).readValue(raw);
      for (CollectionsManifestScope scope : manifest.scopes()) {
        for (CollectionsManifestCollection collection : scope.collections()) {
          long parsed = Long.parseLong(collection.uid(), 16);
          collectionMap.put(
            new CollectionIdentifier(bucket, Optional.of(scope.name()), Optional.of(collection.name())),
            UnsignedLEB128.encode(parsed)
          );
        }
      }
    } catch (Exception ex) {
      eventBus.publish(new CollectionMapDecodingFailedEvent(core.context(), ex));
    }
  }

  /**
   * Analyzes the given config and decides if to apply it (and does so if needed).
   *
   * @param newConfig the config to apply.
   */
  private void checkAndApplyConfig(final BucketConfig newConfig) {
    final String name = newConfig.name();
    final BucketConfig oldConfig = currentConfig.bucketConfig(name);

    if (newConfig.rev() > 0 && oldConfig != null && newConfig.rev() <= oldConfig.rev()) {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.OLD_OR_SAME_REVISION,
        Optional.empty(),
        Optional.empty()
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
    pushConfig();
  }

  /**
   * Analyzes the given config and decides if to apply it (and does so if needed).
   *
   * @param newConfig the config to apply.
   */
  private void checkAndApplyConfig(final GlobalConfig newConfig) {
    final GlobalConfig oldConfig = currentConfig.globalConfig();

    if (newConfig.rev() > 0 && oldConfig != null && newConfig.rev() <= oldConfig.rev()) {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.OLD_OR_SAME_REVISION,
        Optional.empty(),
        Optional.empty()
      ));
      return;
    }

    eventBus.publish(new GlobalConfigUpdatedEvent(core.context(), newConfig));
    currentConfig.setGlobalConfig(newConfig);
    checkAlternateAddress();
    updateSeedNodeList();
    pushConfig();
  }

  /**
   * Helper method to take the current config and update the seed node list with the latest topology.
   *
   * <p>If we have a global config it is used for simplicity reasons. Otherwise we iterate the configs ad collect
   * all the nodes to build the list.</p>
   */
  private void updateSeedNodeList() {
    ClusterConfig config = currentConfig;

    boolean tlsEnabled = core.context().environment().securityConfig().tlsEnabled();
    final Optional<String> alternate = core.context().alternateAddress();

    if (config.globalConfig() != null) {
      Set<SeedNode> seedNodes = config.globalConfig().portInfos().stream().map(ni -> {
        Map<ServiceType, Integer> ports = tlsEnabled ? ni.sslPorts() : ni.ports();
        return SeedNode.create(
          ni.hostname(),
          Optional.ofNullable(ports.get(ServiceType.KV)),
          Optional.ofNullable(ports.get(ServiceType.MANAGER))
        );
      }).collect(Collectors.toSet());
      if (!seedNodes.isEmpty()) {
        this.seedNodes.set(seedNodes);
      }

      return;
    }

    Set<SeedNode> seedNodes = config
      .bucketConfigs()
      .values()
      .stream()
      .flatMap(bc -> bc.nodes().stream())
      .map(ni -> {
        Map<ServiceType, Integer> ports = tlsEnabled ? ni.sslServices() : ni.services();
        return SeedNode.create(
          ni.hostname(),
          Optional.ofNullable(ports.get(ServiceType.KV)),
          Optional.ofNullable(ports.get(ServiceType.MANAGER))
        );
      }).collect(Collectors.toSet());

    if (!seedNodes.isEmpty()) {
      this.seedNodes.set(seedNodes);
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
        seedNodes().stream().map(SeedNode::address).collect(Collectors.toSet())
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
   *
   * if DEFAULT is selected, then null is returned which is equal to the "internal" or default
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
   * Pushes out a the current configuration to all config subscribers.
   */
  private void pushConfig() {
    configsSink.next(currentConfig);
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
  private Mono<Void> registerRefresher(final String bucket) {
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

  /**
   * Method for tests to get the current seed nodes.
   */
  Set<SeedNode> seedNodes() {
    return seedNodes.get();
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
