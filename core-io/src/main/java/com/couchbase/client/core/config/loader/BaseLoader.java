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
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;

/**
 * The {@link BaseLoader} contains all common functionality needed for the actual loader
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
public abstract class BaseLoader implements Loader {

  /**
   * Holds the core attached to send actual commands into.
   */
  private final Core core;

  /**
   * The service type for this loader, to know what service to enable.
   */
  private final ServiceType serviceType;

  BaseLoader(final Core core, final ServiceType serviceType) {
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
  protected abstract Mono<byte[]> discoverConfig(final NetworkAddress seed, final String bucket);

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
   * @param seed the seed node to attempt loading from.
   * @param port the port to use when enabling the service.
   * @param bucket the name of the bucket.
   * @return if successful, returns a config. It fails the mono otherwise.
   */
  @Override
  public Mono<BucketConfig> load(final NetworkAddress seed, final int port, final String bucket) {
    return core
      .ensureServiceAt(seed, serviceType, port, Optional.of(bucket))
      .then(discoverConfig(seed, bucket))
      .map(config -> new String(config, CharsetUtil.UTF_8))
      .map(config -> config.replace("$HOST", seed.address()))
      .map(config -> BucketConfigParser.parse(config, core.context().environment(), seed))
      .onErrorResume(ex -> Mono.error(ex instanceof ConfigException
        ? ex
        : new ConfigException("Caught exception while loading config.", ex)
      ));
  }

  /**
   * Returns the attached {@link Core} to be used by implementations.
   */
  protected Core core() {
    return core;
  }

}
