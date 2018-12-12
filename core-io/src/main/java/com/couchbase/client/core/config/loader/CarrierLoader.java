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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.UnsupportedConfigMechanismException;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CarrierBucketConfigRequest;
import com.couchbase.client.core.service.ServiceType;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.Mono;

public class CarrierLoader extends BaseLoader {

  public CarrierLoader(final Core core) {
    super(core, ServiceType.KV);
  }

  @Override
  protected Mono<String> discoverConfig(NetworkAddress seed, String bucket) {
    final CoreContext ctx = core().context();
    final CoreEnvironment environment = ctx.environment();
    return Mono.defer(() -> {
      CarrierBucketConfigRequest request = new CarrierBucketConfigRequest(
        environment.kvTimeout(),
        ctx,
        bucket,
        environment.retryStrategy(),
        seed
      );
      core().send(request);
      return Reactor.wrap(request, request.response(), true);
    })
    .map(response -> {
      if (response.status().success()) {
        return new String(response.content(), CharsetUtil.UTF_8);
      } else if (response.status() == ResponseStatus.UNSUPPORTED) {
        throw new UnsupportedConfigMechanismException();
      } else {
        throw new CouchbaseException("Unknown status from loader: " + response);
      }
    });
  }

}
