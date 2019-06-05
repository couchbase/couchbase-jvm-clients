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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.error.ConfigException;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * This loader is responsible for initially loading a configuration through the kv protocol.
 *
 * <p>The main and primary mechanism to bootstrap a good configuration is through the kv
 * protocol with a special command, since those connections need to be open anyways and it
 * is more efficient at large scale than the cluster manager (who is the authority).</p>
 *
 * <p>Note that this loader can fail (hence the {@link ClusterManagerLoader} as a backup), either because
 * the current seed node does not have the data service enabled or it is a memcached bucket which
 * does not support the special command.</p>
 *
 * <p>In 1.x this loader used to be called Carrier Loader (from CCCP "couchbase carrier config
 * publication"), but the new name more accurately reflects from which service it is loading it
 * rather than how.</p>
 *
 * @since 1.0.0
 */
public class KeyValueLoader extends BaseLoader {

  public KeyValueLoader(final Core core) {
    super(core, ServiceType.KV);
  }

  @Override
  protected Mono<byte[]> discoverConfig(final NodeIdentifier seed, final String bucket) {
    final CoreContext ctx = core().context();

    return Mono.defer(() -> {
      CarrierBucketConfigRequest request = new CarrierBucketConfigRequest(
        ctx.environment().timeoutConfig().kvTimeout(),
        ctx,
        new CollectionIdentifier(bucket, Optional.empty(), Optional.empty()),
        BestEffortRetryStrategy.INSTANCE,
        seed
      );
      core().send(request);
      return Reactor.wrap(request, request.response(), true);
    })
    .map(response -> {
      if (response.status().success()) {
        return response.content();
      } else if (response.status() == ResponseStatus.UNSUPPORTED) {
        throw new UnsupportedConfigMechanismException();
      } else {
        throw new ConfigException("Received error status from KeyValueLoader: " + response);
      }
    });
  }

}
