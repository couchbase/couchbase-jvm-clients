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
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.config.ConfigIgnoredEvent;
import com.couchbase.client.core.cnc.events.config.ConfigUpdatedEvent;
import com.couchbase.client.core.config.loader.KeyValueLoader;
import com.couchbase.client.core.config.loader.ClusterManagerLoader;
import com.couchbase.client.core.error.AlreadyShutdownException;
import com.couchbase.client.core.error.ConfigException;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

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
   * The number of loaders which will (at maximum) try to load a config
   * in parallel.
   */
  private static final int MAX_PARALLEL_LOADERS = 5;

  private final Core core;
  private final KeyValueLoader keyValueLoader;
  private final ClusterManagerLoader clusterManagerLoader;
  private final EventBus eventBus;

  private final DirectProcessor<ClusterConfig> configs = DirectProcessor.create();
  private final FluxSink<ClusterConfig> configsSink = configs.sink();
  private final ClusterConfig currentConfig = new ClusterConfig();

  private final AtomicBoolean shutdown = new AtomicBoolean(false);

  public DefaultConfigurationProvider(final Core core) {
    this.core = core;
    keyValueLoader = new KeyValueLoader(core);
    clusterManagerLoader = new ClusterManagerLoader(core);
    eventBus = core.context().environment().eventBus();
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
        return Flux
          .fromIterable(core.context().environment().seedNodes())
          .take(MAX_PARALLEL_LOADERS)
          .flatMap(seed -> keyValueLoader
            .load(seed.getAddress(), seed.kvPort().orElse(DEFAULT_KV_PORT), name)
            .onErrorResume(t -> clusterManagerLoader.load(
              seed.getAddress(), seed.httpPort().orElse(DEFAULT_MANAGER_PORT), name
            ))
          )
          .take(1)
          .switchIfEmpty(Mono.error(
            new ConfigException("Could not locate a single bucket configuration for bucket: " + name)
          ))
          .doOnNext(this::checkAndApplyConfig)
          .then();
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
    return Mono.defer(() -> {
      currentConfig.deleteBucketConfig(name);
      pushConfig();
      return Mono.empty();
    });
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(() -> {
      if (shutdown.compareAndSet(false, true)) {
        return Flux
          .fromIterable(currentConfig.bucketConfigs().values())
          .flatMap(bucketConfig -> closeBucketIgnoreShutdown(bucketConfig.name()))
          .doOnComplete(configsSink::complete)
          .then();
      } else {
        return Mono.error(new AlreadyShutdownException());
      }
    });
  }

  /**
   * Analyzes the given config and decides if to apply it (and does so if needed).
   *
   * @param newConfig the config to apply.
   */
  private void checkAndApplyConfig(final BucketConfig newConfig) {
    final BucketConfig oldConfig = currentConfig.bucketConfig(newConfig.name());

    if (newConfig.rev() > 0 && oldConfig != null && newConfig.rev() <= oldConfig.rev()) {
      eventBus.publish(new ConfigIgnoredEvent(
        core.context(),
        ConfigIgnoredEvent.Reason.OLD_OR_SAME_REVISION,
        Optional.empty(),
        Optional.empty()
      ));
      return;
    }

    eventBus.publish(new ConfigUpdatedEvent(core.context(), newConfig));
    currentConfig.setBucketConfig(newConfig.name(), newConfig);
    pushConfig();
  }

  /**
   * Pushes out a the current configuration to all config subscribers.
   */
  private void pushConfig() {
    configsSink.next(currentConfig);
  }

}
