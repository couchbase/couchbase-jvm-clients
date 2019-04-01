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
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

/**
 * Internal helper to map the results from the analytics requests.
 *
 * @since 3.0.0
 */
public class AnalyticsAccessor {

  public static CompletableFuture<AnalyticsResult> analyticsQueryAsync(final Core core,
                                                                       final AnalyticsRequest request) {
    core.send(request);
    return request.response().thenApply(AnalyticsResult::new);
  }

  public static Mono<ReactiveAnalyticsResult> analyticsQueryReactive(final Core core,
                                                                     final AnalyticsRequest request) {
    core.send(request);
    return Reactor
      .wrap(request, request.response(), true)
      .map(ReactiveAnalyticsResult::new);
  }

}
