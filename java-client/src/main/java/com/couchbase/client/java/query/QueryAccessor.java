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
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import io.netty.util.CharsetUtil;

public class QueryAccessor {

	public static CompletableFuture<AsyncQueryResult> queryAsync(final Core core, final Query query,
																 final QueryOptions.BuiltQueryOptions options,
																 final Supplier<ClusterEnvironment> environment) {
		return queryInternal(core, query, options, environment).thenApply(AsyncQueryResult::new);
	}

	public static CompletableFuture<ReactiveQueryResult> queryReactive(final Core core, final Query query,
																	   final QueryOptions.BuiltQueryOptions options,
																	   final Supplier<ClusterEnvironment> environment) {
		return queryInternal(core, query, options, environment).thenApply(ReactiveQueryResult::new);
	}

	private static CompletableFuture<QueryResponse> queryInternal(final Core core, final Query query,
																  final QueryOptions.BuiltQueryOptions opts,
																  final Supplier<ClusterEnvironment> environment) {
		Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().queryTimeout());
		RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());
		JsonObject queryJson = query.getQueryJson(opts.parameters());
		opts.getN1qlParams(queryJson);
		QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy,
				environment.get().credentials(), queryJson.toString().getBytes(CharsetUtil.UTF_8));
		core.send(request);
		return request.response();
	}
}