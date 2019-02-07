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

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.OwnedSupplier;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.search.ReactiveSearchResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class ReactiveCluster {

  private final AsyncCluster asyncCluster;

  public static ReactiveCluster connect(final String username, final String password) {
    return new ReactiveCluster(new OwnedSupplier<>(ClusterEnvironment.create(username, password)));
  }

  public static ReactiveCluster connect(final Credentials credentials) {
    return new ReactiveCluster(new OwnedSupplier<>(ClusterEnvironment.create(credentials)));
  }

  public static ReactiveCluster connect(final String connectionString, final String username, final String password) {
    return new ReactiveCluster(new OwnedSupplier<>(ClusterEnvironment.create(connectionString, username, password)));
  }

  public static ReactiveCluster connect(final String connectionString, final Credentials credentials) {
    return new ReactiveCluster(new OwnedSupplier<>(ClusterEnvironment.create(connectionString, credentials)));
  }

  public static ReactiveCluster connect(final ClusterEnvironment environment) {
    return new ReactiveCluster(() -> environment);
  }

  private ReactiveCluster(final Supplier<ClusterEnvironment> environment) {
    this(new AsyncCluster(environment));
  }

  ReactiveCluster(final AsyncCluster asyncCluster) {
    this.asyncCluster = asyncCluster;
  }

  public AsyncCluster async() {
    return asyncCluster;
  }

  public Mono<ReactiveQueryResult> query(final String statement) {
    return query(statement, QueryOptions.DEFAULT);
  }

  public Mono<ReactiveQueryResult> query(final String statement, final QueryOptions options) {
    notNullOrEmpty(statement, "Statement");
    notNull(options, "QueryOptions");

    /*return Mono.defer(new Supplier<Mono<? extends ReactiveQueryResult>>() {
      @Override
      public Mono<? extends ReactiveQueryResult> get() {
        return null;
      }
    });*/


    QueryOptions.BuiltQueryOptions opts = options.build();
    Duration timeout = opts.timeout().orElse(asyncCluster.environment().get().timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(asyncCluster.environment().get().retryStrategy());

    // FIXME: proper jackson encoding with options
    byte[] query = ("{\"statement\":\""+statement+"\"}").getBytes(CharsetUtil.UTF_8);

    // TODO: I assume cancellation needs to be done THROUGH THE request cancellation
    // mechanism to be consistent?

    EmitterProcessor<QueryResponse.QueryEvent> processor = EmitterProcessor.create();
    QueryRequest request = new QueryRequest(
      timeout,
      asyncCluster.core().context(),
      retryStrategy,
      asyncCluster.environment().get().credentials(),
      query,
      new QueryResponse.QueryEventSubscriber() {
        @Override
        public void onNext(QueryResponse.QueryEvent row) {
          processor.onNext(row);
        }

        @Override
        public void onComplete() {
          processor.onComplete();
        }
      }
    );

    asyncCluster.core().send(request);

    // TODO
    return null;
  }

  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement) {
    return analyticsQuery(statement, AnalyticsOptions.DEFAULT);
  }

  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement, final AnalyticsOptions options) {
    notNullOrEmpty(statement, "Statement");
    notNull(options, "AnalyticsOptions");

    return null;
  }

  public Mono<ReactiveSearchResult> searchQuery(final SearchQuery query) {
    return searchQuery(query, SearchOptions.DEFAULT);
  }

  public Mono<ReactiveSearchResult> searchQuery(final SearchQuery query, final SearchOptions options) {
    notNull(query, "SearchQuery");
    notNull(options, "SearchOptions");

    return null;
  }

  public Mono<ReactiveBucket> bucket(String name) {
    return Mono.fromFuture(asyncCluster.bucket(name)).map(ReactiveBucket::new);
  }

  public Mono<Void> shutdown() {
    return null;
  }

}
