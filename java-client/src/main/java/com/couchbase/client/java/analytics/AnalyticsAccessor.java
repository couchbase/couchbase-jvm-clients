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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
import com.couchbase.client.core.topology.ClusterProdName;
import com.couchbase.client.java.codec.JsonSerializer;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * Internal helper to map the results from the analytics requests.
 *
 * @since 3.0.0
 */
public class AnalyticsAccessor {

  public static CompletableFuture<AnalyticsResult> analyticsQueryAsync(final Core core,
                                                                       final AnalyticsRequest request,
                                                                       final JsonSerializer serializer) {
    return analyticsQueryInternal(core, request)
      .flatMap(response -> response
        .rows()
        .collectList()
        .flatMap(rows -> response
          .trailer()
          .map(trailer -> new AnalyticsResult(response.header(), rows, trailer, serializer))
        )
      )
      .toFuture();
  }

  public static Mono<ReactiveAnalyticsResult> analyticsQueryReactive(final Core core, final AnalyticsRequest request, final JsonSerializer serializer) {
    return analyticsQueryInternal(core, request).map(r -> new ReactiveAnalyticsResult(r, serializer));
  }

  private static Mono<AnalyticsResponse> analyticsQueryInternal(final Core core, final AnalyticsRequest request) {
    return core.waitForClusterTopology(request.timeout())
                    .flatMap(clusterTopology -> {

                      if (clusterTopology.id() != null
                              && clusterTopology.id().prodName() != null
                              && !clusterTopology.id().prodName().startsWith(ClusterProdName.COUCHBASE_SERVER)) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("This '");
                        sb.append(clusterTopology.id().prodName());
                        sb.append("' cluster cannot be used with this SDK, which is intended for use with operational clusters");
                        if (clusterTopology.id().prodName().startsWith(ClusterProdName.ENTERPRISE_ANALYTICS)) {
                          sb.append(". For this cluster, an Enterprise Analytics SDK should be used.");
                        }
                        return Mono.error(new CouchbaseException(sb.toString()));
                      }

                      return Mono.defer(() -> {
                        core.send(request);
                        return Reactor.wrap(request, request.response(), true);
                      }).doOnNext(ignored -> request.context().logicallyComplete())
                        .doOnError(err -> request.context().logicallyComplete(err));
                    });
  }

}
