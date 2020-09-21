/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.manager.raw;

import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.msg.manager.GenericManagerRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

import static com.couchbase.client.java.manager.raw.RawManagerOptions.rawManagerOptions;

/**
 * This class provides direct access to the various management APIs in a raw (uncommitted) form.
 * <p>
 * Please note that the results of the individual methods can vary greatly between server versions. This API
 * should only be used if you know what you ask for and it is not covered by the official, high level management
 * APIs already.
 */
@Stability.Uncommitted
public class RawManager {

  /**
   * Performs a {@link RawManagerRequest} with default options against the given cluster.
   *
   * @param cluster the cluster to query against.
   * @param request the request to dispatch.
   * @return a Mono eventually containing the response when it arrives.
   */
  public static Mono<RawManagerResponse> call(final Cluster cluster, final RawManagerRequest request) {
    return call(cluster, request, rawManagerOptions());
  }

  /**
   * Performs a {@link RawManagerRequest} with custom options against the given cluster.
   *
   * @param cluster the cluster to query against.
   * @param request the request to dispatch.
   * @param options the custom options to use.
   * @return a Mono eventually containing the response when it arrives.
   */
  public static Mono<RawManagerResponse> call(final Cluster cluster, final RawManagerRequest request,
                                              final RawManagerOptions options) {
    switch (request.serviceType()) {
      case MANAGER:
        return callManagement(cluster, request, options);
      default:
        return Mono.error(new InvalidArgumentException("Unsupported ServiceType: " + request.serviceType(), null, null));
    }
  }

  private static Mono<RawManagerResponse> callManagement(final Cluster cluster, final RawManagerRequest request,
                                                         final RawManagerOptions options) {
    final ClusterEnvironment environment = cluster.environment();
    final RawManagerOptions.Built opts = options.build();

    JsonSerializer serializer = opts.serializer() != null ? opts.serializer() : environment.jsonSerializer();
    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().managementTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    final GenericManagerRequest req = new GenericManagerRequest(
      timeout,
      cluster.core().context(),
      retryStrategy,
      () -> {
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, request.method(), request.uri());
        for (Map.Entry<String, Object> e : opts.httpHeaders().entrySet()) {
          httpRequest.headers().set(e.getKey(), e.getValue());
        }
        return httpRequest;
      },
      request.method().equals(HttpMethod.GET)
    );

    cluster.core().send(req);

    return Reactor
      .wrap(req, req.response(), true)
      .map(res -> new RawManagerResponse(request.serviceType(), serializer, res.httpStatus(), res.content()));
  }

}
