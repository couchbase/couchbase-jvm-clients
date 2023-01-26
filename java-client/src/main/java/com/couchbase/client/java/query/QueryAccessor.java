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

package com.couchbase.client.java.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.msg.query.CoreQueryAccessor;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.codec.JsonSerializer;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Converts requests and responses for N1QL queries.
 * <p>
 * Note that this accessor also transparently deals with prepared statements and the associated query
 * cache.
 * <p>
 * Also, this class has internal functionality and is not intended to be called from the user directly.
 */
@Stability.Internal
public class QueryAccessor {

  private final CoreQueryAccessor coreQueryAccessor;

  public QueryAccessor(final Core core) {
    this.coreQueryAccessor = new CoreQueryAccessor(core);
  }

  /**
   * Performs a N1QL query and returns the result as a future.
   * <p>
   * Note that compared to the reactive method, this one collects the rows into a list and makes sure
   * everything is part of the result. If you need backpressure, go with reactive.
   *
   * @param request the request to perform.
   * @param options query options to use.
   * @return the future once the result is complete.
   */
  public CompletableFuture<QueryResult> queryAsync(final QueryRequest request, final QueryOptions.Built options,
                                                   final JsonSerializer serializer) {
    return coreQueryAccessor.query(request, options.adhoc())
        .flatMap(response -> response
            .rows()
            .collectList()
            .flatMap(rows -> response
                .trailer()
                .map(trailer -> (QueryResult) new QueryResultHttp(response.header(), rows, trailer, serializer))
            )
        )
        .toFuture();
  }

  /**
   * Performs a N1QL query and returns the result as a Mono.
   *
   * @param request the request to perform.
   * @param options query options to use.
   * @return the mono once the result is complete.
   */
  public Mono<ReactiveQueryResult> queryReactive(final QueryRequest request, final QueryOptions.Built options,
                                                 final JsonSerializer serializer) {
    return coreQueryAccessor.query(request, options.adhoc())
        .map(r -> new ReactiveQueryResultHttp(r, serializer));
  }

  /**
   * Used by the transactions library, this provides some binary interface protection against
   * QueryRequest/TargetedQueryRequest changing.
   */
  @Stability.Internal
  @SuppressWarnings("unused")
  public static QueryRequest targetedQueryRequest(String statement,
                                                  byte[] queryBytes,
                                                  String clientContextId,
                                                  @Nullable NodeIdentifier target,
                                                  boolean readonly,
                                                  RetryStrategy retryStrategy,
                                                  Duration timeout,
                                                  RequestSpan parentSpan,
                                                  Core core) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedQueryErrorContext(statement));

    return new QueryRequest(timeout, core.context(), retryStrategy, core.context().authenticator(), statement,
        queryBytes, readonly, clientContextId, parentSpan, null, null, target);
  }

}
