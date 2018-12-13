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
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.msg.manager.TerseBucketConfigRequest;
import com.couchbase.client.core.msg.manager.TerseBucketConfigResponse;
import com.couchbase.client.core.service.ServiceType;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.Mono;

import java.util.function.Function;
import java.util.function.Supplier;

public class HttpLoader extends BaseLoader {

  public HttpLoader(final Core core) {
    super(core, ServiceType.MANAGER);
  }

  @Override
  protected Mono<String> discoverConfig(NetworkAddress seed, String bucket) {
    final CoreContext ctx = core().context();
    final CoreEnvironment environment = ctx.environment();

    return Mono.defer(() -> {
      // todo: fixme proper timeout
      // todo: fail fast here on the retry strategy?
      TerseBucketConfigRequest request = new TerseBucketConfigRequest(
        environment.kvTimeout(),
        ctx,
        environment.retryStrategy(),
        bucket,
        environment.credentials(),
        seed
      );
      core().send(request);
      return Reactor.wrap(request, request.response(), true);
    }).map(new Function<TerseBucketConfigResponse, String>() {
      @Override
      public String apply(TerseBucketConfigResponse response) {
        // todo: error handling
        return new String(response.config(), CharsetUtil.UTF_8);
      }
    });
  }

}
