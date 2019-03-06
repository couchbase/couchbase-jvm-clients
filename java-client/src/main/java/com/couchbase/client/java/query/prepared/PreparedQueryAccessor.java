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

package com.couchbase.client.java.query.prepared;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import com.couchbase.client.core.Core;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;

/**
 * Prepared queries helper for sending the requests to core
 *
 * @since 3.0.0
 */
public class PreparedQueryAccessor {

    public static CompletableFuture<AsyncQueryResult> queryAsync(final Core core, final Query query,
                                                                 final QueryOptions.BuiltQueryOptions options, final Supplier<ClusterEnvironment> environment,
                                                                 final LFUCache<String, PreparedQuery> preparedCache) {
        return queryInternal(core, query, options, environment, preparedCache).thenApply(AsyncQueryResult::new);
    }

    public static CompletableFuture<ReactiveQueryResult> queryReactive(final Core core, final Query query,
                                                                       final QueryOptions.BuiltQueryOptions options,
                                                                       final Supplier<ClusterEnvironment> environment,
                                                                       final LFUCache<String, PreparedQuery> preparedCache) {
        return queryInternal(core, query, options, environment, preparedCache).thenApply(ReactiveQueryResult::new);
    }

    public static CompletableFuture<QueryResponse> queryInternal(final Core core, final Query query,
                                                                 final QueryOptions.BuiltQueryOptions opts,
                                                                 final Supplier<ClusterEnvironment> environment,
                                                                 final LFUCache<String, PreparedQuery> preparedCache) {
        Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().queryTimeout());
        RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());

        if (preparedCache.contains(query.statement())) {
            PreparedQuery prepared = preparedCache.get(query.statement());
            JsonObject queryJson = prepared.getQueryJson(opts.parameters());
            opts.getN1qlParams(queryJson);
            QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy, environment.get().credentials(),
                    queryJson.toString().getBytes(CharsetUtil.UTF_8));
            core.send(request);
            return request.response()
                    .thenApply((r) -> {
                        if (r.status() != ResponseStatus.SUCCESS) {
                            AsyncQueryResult result = new AsyncQueryResult(r);
                            result.errors().thenApply(errors -> {
                                if (errors.size() != 0) {
                                    if (canRetryPrepared(errors.get(0).getInt("code"))) {
                                        preparedCache.remove(query.statement());
                                        return queryInternal(core, query, opts, environment, preparedCache);
                                    }
                                }
                                return errors;
                            });
                        }
                        return r;
                    });
        } else {
            JsonObject queryJson = query.getQueryJson(null);
            opts.getN1qlParams(queryJson);
            QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy, environment.get().credentials(),
                    queryJson.toString().getBytes(CharsetUtil.UTF_8));
            core.send(request);
            return request
                    .response()
                    .thenApply(AsyncQueryResult::new)
                    .thenCompose(AsyncQueryResult::rows)
                    .thenApply(l -> {
                        PreparedQuery prepared = PreparedQuery.fromJsonObject(l.get(0));
                        preparedCache.put(query.statement(), prepared);
                        return prepared;
                    }).thenCompose(r -> queryInternal(core, query, opts, environment, preparedCache));
        }
    }

    private static boolean canRetryPrepared(int code) {
        return code == PreparedErrorCodes.NO_SUCH_PREPARED_STATEMENT.code() ||
                code == PreparedErrorCodes.INDEX_NOT_FOUND.code() ||
                code == PreparedErrorCodes.ENCODED_PLAN_MISMATCH.code() ||
                code == PreparedErrorCodes.ENCODED_PLAN_DECODE_ERROR.code();
    }
}