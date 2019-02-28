package com.couchbase.client.java.query;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;

public class QueryAccessor {

	public static CompletableFuture<AsyncQueryResult> queryAsync(final Core core, final Query query, final QueryOptions options,
																															 final Supplier<ClusterEnvironment> environment) {
		return queryInternal(core, query, options, environment).thenApply(AsyncQueryResult::new);
	}

	public static CompletableFuture<ReactiveQueryResult> queryReactive(final Core core, final Query query, final QueryOptions options,
																																		 final Supplier<ClusterEnvironment> environment) {
		return queryInternal(core, query, options, environment).thenApply(ReactiveQueryResult::new);
	}

	private static CompletableFuture<QueryResponse> queryInternal(final Core core, final Query query, final QueryOptions options,
														final Supplier<ClusterEnvironment> environment) {
		QueryOptions.BuiltQueryOptions opts = options.build();
		Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().queryTimeout());
		RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());
		QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy,
						environment.get().credentials(), query.encode(opts.parameters()));
		core.send(request);
		return request.response();
	}
}
