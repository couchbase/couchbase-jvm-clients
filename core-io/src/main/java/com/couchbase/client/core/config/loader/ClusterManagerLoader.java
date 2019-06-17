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
import com.couchbase.client.core.msg.manager.BucketConfigRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Mono;

/**
 * This loader is responsible for loading a config from the cluster manager.
 *
 * <p>While one might think always going to the cluster manager is the best option, there is a
 * reason why this loader is only a fallback to the {@link KeyValueLoader}. At scale, talking to
 * KV engine for a config is much more efficient than talking to the cluster manager. But there
 * are times where the carrier loader cannot do its job and then this fallback is a safe
 * alternative.</p>
 *
 * <p>Side note for folks coming from the 1.x core: since we've stopped supporting anything older
 * than 5.0.0 on the server, there is no need for the verbose fallback anymore, since every
 * supported version supports the terse http config path.</p>
 *
 * <p>In 1.x this used to be called the "HttpLoader", but the new name more accurately reflects
 * where it is getting the config from rather than how.</p>
 *
 * @since 1.0.0
 */
public class ClusterManagerLoader extends BaseLoader {

  public ClusterManagerLoader(final Core core) {
    super(core, ServiceType.MANAGER);
  }

  @Override
  protected Mono<byte[]> discoverConfig(final NodeIdentifier seed, final String bucket) {
    final CoreContext ctx = core().context();

    return Mono.defer(() -> {
      BucketConfigRequest request = new BucketConfigRequest(
        ctx.environment().timeoutConfig().managerTimeout(),
        ctx,
        BestEffortRetryStrategy.INSTANCE,
        bucket,
        ctx.environment().credentials(),
        seed
      );
      core().send(request);
      return Reactor.wrap(request, request.response(), true);
    }).map(response -> {
      if (response.status().success()) {
        return response.config();
      } else {
        throw new ConfigException("Received error status from ClusterManagerLoader: " + response);
      }
    });
  }

}
