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

package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.SeedNodeOutdatedException;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceState;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link BaseBucketLoader} contains all common functionality needed for the actual loader
 * implementations.
 *
 * <p>This abstract parent class basically ensures that the service and node needed to possibly
 * fetch a configuration are enabled. It might still fail in progress for whatever reason, but
 * that's why there are fallbacks in place (in the form of other loaders).</p>
 *
 * <p>Once a config is loaded, the base loader is also responsible for turning the string-based
 * config into a proper config that can be distributed throughout the system.</p>
 *
 * @since 2.0.0
 */
public abstract class BaseBucketLoader implements BucketLoader {

  /**
   * Holds the core attached to send actual commands into.
   */
  private final Core core;

  /**
   * The service type for this loader, to know what service to enable.
   */
  private final ServiceType serviceType;

  BaseBucketLoader(final Core core, final ServiceType serviceType) {
    this.core = core;
    this.serviceType = serviceType;
  }

  /**
   * To be implemented by the actual child, performs the actual fetching of a config.
   *
   * @param seed the node from where to fetch it.
   * @param bucket the name of the bucket to fetch from.
   * @return the encoded json version of the config if complete, an error otherwise.
   */
  protected abstract Mono<byte[]> discoverConfig(final NodeIdentifier seed, final String bucket);

  /**
   * Performs the config loading through multiple steps.
   *
   * <p>First, it makes sure that the service is enabled so that the following child implementation
   * can run its commands to actually fetch the config. Once the config is successfully loaded, it
   * then turns it first into a proper string and then sends it to the bucket config parser to
   * turn it into an actual config.</p>
   *
   * <p>Two things to note: $HOST is a special syntax by the cluster manager which the client needs
   * to replace with the hostname where it loaded the config from. Also, at the end we are wrapping
   * all non-config exceptions into config exceptions so that the upper level only needs to handle
   * one specific exception type.</p>
   *
   * <p>At this point, we are passing a null for server group info when the
   * service is created, since we do not have a config to check against at this point. The config provider
   * will take care of this at a later point in time, before the rest of the bootstrap happens.</p>
   *
   * @param seed the seed node to attempt loading from.
   * @param port the port to use when enabling the service.
   * @param bucket the name of the bucket.
   * @return if successful, returns a config. It fails the mono otherwise.
   */
  @Override
  public Mono<ProposedBucketConfigContext> load(final NodeIdentifier seed, final int port, final String bucket) {
    return core
      .ensureServiceAt(seed, serviceType, port, Optional.of(bucket))
      .then(ensureServiceConnected(seed, serviceType, Optional.of(bucket)))
      .then(discoverConfig(seed, bucket))
      .map(config -> new String(config, UTF_8))
      .map(config -> config.replace("$HOST", seed.address()))
      .map(config -> new ProposedBucketConfigContext(bucket, config, seed.address()))
      .onErrorResume(ex -> Mono.error(ex instanceof ConfigException
        ? ex
        : new ConfigException("Caught exception while loading config.", ex)
      ));
  }

  /**
   * Makes sure the services is connected (or failed to connect) before sending the op on its way.
   * <p>
   * It is important that we monitor the service (with its endpoints) if it eventually reaches a CONNECTED state
   * so we quickly realize if it goes into a DISCONNECT/CONNECTING reconnect loop. If we detect it, we can bail
   * out quickly and give the fallback a chance to succeed.
   *
   * @param seed the seed node to check.
   * @param serviceType the service type to verify.
   * @param bucket the bucket name, if present.
   * @return a Mono that completes if the service is ready OR will fail if it detected it is not.
   */
  private Mono<Void> ensureServiceConnected(final NodeIdentifier seed, final ServiceType serviceType,
                                            final Optional<String> bucket) {
    return Flux.defer(() -> {
      Optional<Flux<ServiceState>> states = core.serviceState(seed, serviceType, bucket);
      return states.orElseGet(() ->
        Flux.error(new SeedNodeOutdatedException("Seed Node " + seed + " for service " + serviceType
          + " not present anymore, bailing out.")
      ));
    })
    .map(ss -> {
      if (ss == ServiceState.DISCONNECTED) {
        throw new ConfigException("Seed Node " + seed + " is disconnected, bailing out.");
      }
      return ss;
    })
    .takeUntil(state -> state == ServiceState.CONNECTED || state == ServiceState.IDLE)
    .then();
  }


  /**
   * Returns the attached {@link Core} to be used by implementations.
   */
  protected Core core() {
    return core;
  }

}
