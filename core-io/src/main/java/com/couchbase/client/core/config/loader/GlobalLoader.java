/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.config.ConfigVersion;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.GlobalConfigNotFoundException;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CarrierGlobalConfigRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Mono;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The {@link GlobalLoader} is different to the bucket-based loaders in that it tries to fetch a cluster global
 * configuration which is not bound to a specific bucket.
 *
 * <p>Global loading is part of the initial bootstrap sequence, even before a bucket is opened at all. This
 * operation might fail on older clusters, higher level components need to deal with this accordingly.</p>
 */
public class GlobalLoader {

  private final Core core;

  public GlobalLoader(final Core core) {
    this.core = core;
  }

  /**
   * Tries to load the global configuration.
   *
   * <p>Please note that at this point, we are passing a null for server group info when the
   * service is created, since we do not have a config to check against at this point. The config provider
   * will take care of this at a later point in time, before the rest of the bootstrap happens.
   *
   * @param seed the seed node to load from.
   * @param port the port number for the KV service.
   * @return once complete a proposed global config context to update.
   */
  public Mono<ProposedGlobalConfigContext> load(final NodeIdentifier seed, final int port) {
    return core
      .ensureServiceAt(seed, ServiceType.KV, port, Optional.empty())
      .then(discoverConfig(seed))
      .map(config -> new String(config, UTF_8))
      .map(config -> config.replace("$HOST", seed.address()))
      .map(config -> new ProposedGlobalConfigContext(config, seed.address()))
      .onErrorResume(ex -> Mono.error(ex instanceof ConfigException
        ? ex
        : new ConfigException("Caught exception while loading global config.", ex)
      ));
  }

  private Mono<byte[]> discoverConfig(final NodeIdentifier seed) {
    final CoreContext ctx = core.context();

    return Mono.defer(() -> {
      CarrierGlobalConfigRequest request = new CarrierGlobalConfigRequest(
        ctx.environment().timeoutConfig().connectTimeout(),
        ctx,
        BestEffortRetryStrategy.INSTANCE,
        seed,
        ConfigVersion.ZERO
      );
      core.send(request);
      return Reactor.wrap(request, request.response(), true);
    }).map(response -> {
      if (response.status().success()) {
        return response.content();
      } else if (response.status() == ResponseStatus.UNSUPPORTED || response.status() == ResponseStatus.NO_BUCKET) {
        throw new UnsupportedConfigMechanismException();
      } else if (response.status() == ResponseStatus.NOT_FOUND) {
        throw new GlobalConfigNotFoundException();
      } else {
        throw new ConfigException("Received unexpected error status from GlobalLoader: " + response);
      }
    });
  }

}
