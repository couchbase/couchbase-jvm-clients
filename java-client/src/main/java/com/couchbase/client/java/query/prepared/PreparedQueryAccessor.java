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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.QueryEndpoint;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;

/**
 * Prepared queries helper for sending the requests to core
 *
 * @since 3.0.0
 */
public class PreparedQueryAccessor {

	public static CompletableFuture<AsyncQueryResult> queryAsync(final Core core, final Query query, final QueryOptions options,
																															 final Supplier<ClusterEnvironment> environment,
																															 final LFUCache<String, PreparedQuery> preparedCache) {
		return queryInternal(core, query, options, environment, preparedCache).thenApply(AsyncQueryResult::new);
	}

	public static CompletableFuture<ReactiveQueryResult> queryReactive(final Core core, final Query query, final QueryOptions options,
																																		 final Supplier<ClusterEnvironment> environment,
																																		 final LFUCache<String, PreparedQuery> preparedCache) {
		return queryInternal(core, query, options, environment, preparedCache).thenApply(ReactiveQueryResult::new);
	}

	private static CompletableFuture<QueryResponse> queryInternal(final Core core, final Query query, final QueryOptions options,
																																final Supplier<ClusterEnvironment> environment,
																																final LFUCache<String, PreparedQuery> preparedCache) {
		QueryOptions.BuiltQueryOptions opts = options.build();
		Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().queryTimeout());
		RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());
		QueryRequest request;

		//TODO: handle prepared retries
		if (preparedCache.contains(query.statement())) {
			PreparedQuery prepared = preparedCache.get(query.statement());
			request = new QueryRequest(timeout, core.context(), retryStrategy, environment.get().credentials(), prepared.encode());
			core.send(request);
			return request
							.response();
		} else {
			request = new QueryRequest(timeout, core.context(), retryStrategy, environment.get().credentials(), query.encode());
			core.send(request);
			return request
							.response()
							.thenApply(AsyncQueryResult::new)
							.thenCompose(r -> r.rows(PreparedQuery.class))
							.thenApply(l -> {
								PreparedQuery prepared = l.get(0);
								preparedCache.put(query.statement(), prepared);
								return prepared;
							}).thenCompose(r -> queryInternal(core, query, options, environment, preparedCache));
		}
	}

	@Stability.Internal
	public static CompletableFuture<QueryResponse> queryEndpoint(final QueryEndpoint endpoint, final Core core, final Query query, final QueryOptions options,
																																final Supplier<ClusterEnvironment> environment,
																																final LFUCache<String, PreparedQuery> preparedCache) {
		QueryOptions.BuiltQueryOptions opts = options.build();
		Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().queryTimeout());
		RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());
		QueryRequest request;

		//TODO: handle prepared retries
		if (preparedCache.contains(query.statement())) {
			PreparedQuery prepared = preparedCache.get(query.statement());
			request = new QueryRequest(timeout, core.context(), retryStrategy, environment.get().credentials(), prepared.encode());
			endpoint.send(request);
			return request
							.response();
		} else {
			request = new QueryRequest(timeout, core.context(), retryStrategy, environment.get().credentials(), query.encode());
			endpoint.send(request);
			return request
							.response()
							.thenApply(AsyncQueryResult::new)
							.thenCompose(r -> r.rows())
							.thenApply(l -> {
								PreparedQuery prepared = PreparedQuery.fromJsonObject(l.get(0));
								preparedCache.put(query.statement(), prepared);
								return prepared;
							}).thenCompose(r -> queryEndpoint(endpoint, core, query, options, environment, preparedCache));
		}
	}
}