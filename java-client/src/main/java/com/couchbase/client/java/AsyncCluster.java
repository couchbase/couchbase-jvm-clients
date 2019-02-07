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

package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.OwnedSupplier;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AsyncAnalyticsResult;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryRow;
import com.couchbase.client.java.search.AsyncSearchResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.EmitterProcessor;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class AsyncCluster {

  private final Supplier<ClusterEnvironment> environment;
  private final Core core;


  public static AsyncCluster connect(final String username, final String password) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(username, password)));
  }

  public static AsyncCluster connect(final Credentials credentials) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(credentials)));
  }

  public static AsyncCluster connect(final String connectionString, final String username, final String password) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(connectionString, username, password)));
  }

  public static AsyncCluster connect(final String connectionString, final Credentials credentials) {
    return new AsyncCluster(new OwnedSupplier<>(ClusterEnvironment.create(connectionString, credentials)));
  }

  public static AsyncCluster connect(final ClusterEnvironment environment) {
    return new AsyncCluster(() -> environment);
  }

  AsyncCluster(final Supplier<ClusterEnvironment> environment) {
    this.environment = environment;
    this.core = Core.create(environment.get());
  }

  public Supplier<ClusterEnvironment> environment() {
    return environment;
  }

  public Core core() {
    return core;
  }

  public CompletableFuture<AsyncQueryResult> query(final String statement, final Consumer<QueryRow> consumer) {
    return query(statement, consumer, QueryOptions.DEFAULT);
  }

  public CompletableFuture<AsyncQueryResult> query(final String statement,
                                                   final Consumer<QueryRow> consumer,
                                                   final QueryOptions options) {
    notNullOrEmpty(statement, "Statement");
    notNull(options, "QueryOptions");
    QueryOptions.BuiltQueryOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().queryTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());

    // FIXME: proper jackson encoding with options
    byte[] query = ("{\"statement\":\""+statement+"\"}").getBytes(CharsetUtil.UTF_8);

    // TODO: I assume cancellation needs to be done THROUGH THE request cancellation
    // mechanism to be consistent?

    AsyncQueryResult result = new AsyncQueryResult(consumer);
    QueryRequest request = new QueryRequest(
      timeout,
      core.context(),
      retryStrategy,
      environment.get().credentials(),
      query,
      result
    );
    core.send(request);
    return request.response().thenApply(r -> {
      result.result(r);
      return result;
    });
  }

  public CompletableFuture<AsyncAnalyticsResult> analyticsQuery(final String statement) {
    return analyticsQuery(statement, AnalyticsOptions.DEFAULT);
  }

  public CompletableFuture<AsyncAnalyticsResult> analyticsQuery(final String statement,
                                                           final AnalyticsOptions options) {
    notNullOrEmpty(statement, "Statement");
    notNull(options, "AnalyticsOptions");
    return null;
  }

  public CompletableFuture<AsyncSearchResult> searchQuery(final SearchQuery query) {
    return searchQuery(query, SearchOptions.DEFAULT);
  }

  public CompletableFuture<AsyncSearchResult> searchQuery(final SearchQuery query, final SearchOptions options) {
    notNull(query, "SearchQuery");
    notNull(options, "SearchOptions");

    return null;
  }

  public CompletableFuture<AsyncBucket> bucket(final String name) {
    return core
      .openBucket(name)
      .thenReturn(new AsyncBucket(name, core, environment.get()))
      .toFuture();
  }

  public CompletableFuture<Void> shutdown() {
    if (environment instanceof OwnedSupplier) {
      // TODO: fixme
      return environment.get().shutdownAsync(Duration.ofSeconds(1)).toFuture();
    } else {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      cf.complete(null);
      return cf;
    }
  }

}
