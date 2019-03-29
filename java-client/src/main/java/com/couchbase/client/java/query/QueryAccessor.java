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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static java.nio.charset.StandardCharsets.UTF_8;

public class QueryAccessor {

    public static CompletableFuture<QueryResult> map(CompletableFuture<QueryResponse> in) {
        return Mono.fromFuture(in)

                .flatMap(response -> response.rows().collectList()

                        .flatMap(rows -> response.trailer()

                                .map(trailer -> {
                                    QueryMeta meta = new QueryMeta(response, trailer);

                                    return new QueryResult(rows.stream(), meta);
                                })
                        )
                )

                .toFuture();

    }

    public static CompletableFuture<QueryResult> queryAsync(final Core core, final Query query,
                                                            final QueryOptions.BuiltQueryOptions options,
                                                            final ClusterEnvironment environment) {

        return map(queryInternal(core, query, options, environment));
    }

    public static CompletableFuture<ReactiveQueryResult> queryReactive(final Core core, final Query query,
                                                                       final QueryOptions.BuiltQueryOptions options,
                                                                       final ClusterEnvironment environment) {
        return Mono.fromFuture(queryInternal(core, query, options, environment))

                .map(v -> {
                    Mono<QueryMeta> meta = v.trailer()
                            .map(trailer -> new QueryMeta(v, trailer));

                    return new ReactiveQueryResult(v.rows(), meta);
                })

                .toFuture();

    }

    private static CompletableFuture<QueryResponse> queryInternal(final Core core, final Query query,
                                                                  final QueryOptions.BuiltQueryOptions opts,
                                                                  final ClusterEnvironment environment) {
        Duration timeout = opts.timeout().orElse(environment.timeoutConfig().queryTimeout());
        RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
        JsonObject queryJson = query.getQueryJson(opts.parameters());
        opts.getN1qlParams(queryJson);
        QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy,
                environment.credentials(), queryJson.toString().getBytes(UTF_8));
        core.send(request);
        return request.response();
    }
}
