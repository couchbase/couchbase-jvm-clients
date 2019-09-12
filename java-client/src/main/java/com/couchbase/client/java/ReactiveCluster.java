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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.OwnedSupplier;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.analytics.ReactiveAnalyticsIndexManager;
import com.couchbase.client.java.manager.bucket.ReactiveBucketManager;
import com.couchbase.client.java.manager.query.AsyncQueryIndexManager;
import com.couchbase.client.java.manager.query.ReactiveQueryIndexManager;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.search.SearchAccessor;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.java.search.SearchOptions.searchOptions;

public class ReactiveCluster {

  static final QueryOptions DEFAULT_QUERY_OPTIONS = queryOptions();
  static final SearchOptions DEFAULT_SEARCH_OPTIONS = searchOptions();
  static final AnalyticsOptions DEFAULT_ANALYTICS_OPTIONS = analyticsOptions();

  /**
   * Holds the underlying async cluster reference.
   */
  private final AsyncCluster asyncCluster;

  /**
   * Connect to a Couchbase cluster with a username and a password as credentials.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param username the name of the user with appropriate permissions on the cluster.
   * @param password the password of the user with appropriate permissions on the cluster.
   * @return if properly connected, returns a {@link ReactiveCluster}.
   */
  public static Mono<ReactiveCluster> connect(final String connectionString, final String username,
                                        final String password) {
    return connect(connectionString, PasswordAuthenticator.create(username, password));
  }

  /**
   * Connect to a Couchbase cluster with custom {@link Authenticator}.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param authenticator custom credentials used when connecting to the cluster.
   * @return if properly connected, returns a {@link ReactiveCluster}.
   */
  public static Mono<ReactiveCluster> connect(final String connectionString,
                                        final Authenticator authenticator) {
    return Mono.defer(() -> {
      ReactiveCluster cluster = new ReactiveCluster(new OwnedSupplier<>(
        ClusterEnvironment.create(connectionString, authenticator)
      ));
      return cluster.asyncCluster.performGlobalConnect().then(Mono.just(cluster));
    });
  }

  /**
   * Connect to a Couchbase cluster with a custom {@link ClusterEnvironment}.
   *
   * @param environment the custom environment with its properties used to connect to the cluster.
   * @return if properly connected, returns a {@link ReactiveCluster}.
   */
  public static Mono<ReactiveCluster> connect(final ClusterEnvironment environment) {
    return Mono.defer(() -> {
      ReactiveCluster cluster = new ReactiveCluster(() -> environment);
      return cluster.asyncCluster.performGlobalConnect().then(Mono.just(cluster));
    });
  }

  /**
   * Creates a new cluster from a {@link ClusterEnvironment}.
   *
   * @param environment the environment to use for this cluster.
   */
  private ReactiveCluster(final Supplier<ClusterEnvironment> environment) {
    this(new AsyncCluster(environment));
  }

  /**
   * Creates a {@link ReactiveCluster} from an {@link AsyncCluster}.
   *
   * @param asyncCluster the underlying async cluster.
   */
  ReactiveCluster(final AsyncCluster asyncCluster) {
    this.asyncCluster = asyncCluster;
  }


  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return asyncCluster.core();
  }

  /**
   * Provides access to the bucket management services.
   */
  @Stability.Volatile
  public ReactiveBucketManager buckets() {
    return new ReactiveBucketManager(async().buckets());
  }

  /**
   * Provides access to the Analytics index management services.
   */
  @Stability.Volatile
  public ReactiveAnalyticsIndexManager analyticsIndexes() {
    return new ReactiveAnalyticsIndexManager(async());
  }

  /**
   * Provides access to the N1QL index management services.
   */
  @Stability.Volatile
  public ReactiveQueryIndexManager queryIndexes() {
    return new ReactiveQueryIndexManager(new AsyncQueryIndexManager(async()));
  }

  /**
   * Provides access to the underlying {@link AsyncCluster}.
   */
  public AsyncCluster async() {
    return asyncCluster;
  }

  /**
   * Provides access to the configured {@link ClusterEnvironment} for this cluster.
   */
  public ClusterEnvironment environment() {
    return asyncCluster.environment();
  }

  /**
   * Performs a N1QL query with default {@link QueryOptions}.
   *
   * @param statement the N1QL query statement as a raw string.
   * @return the {@link ReactiveQueryResult} once the response arrives successfully.
   */
  public Mono<ReactiveQueryResult> query(final String statement) {
    return this.query(statement, DEFAULT_QUERY_OPTIONS);
  }

  /**
   * Performs a N1QL query with custom {@link QueryOptions}.
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link ReactiveQueryResult} once the response arrives successfully.
   */
  public Mono<ReactiveQueryResult> query(final String statement, final QueryOptions options) {
    final QueryOptions.Built opts = options.build();
    return asyncCluster.queryAccessor().queryReactive(
      asyncCluster.queryRequest(statement, opts),
      opts
    );
  }

  /**
   * Performs an Analytics query with default {@link AnalyticsOptions}.
   *
   * @param statement the Analytics query statement as a raw string.
   * @return the {@link ReactiveAnalyticsResult} once the response arrives successfully.
   */
  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement) {
    return analyticsQuery(statement, DEFAULT_ANALYTICS_OPTIONS);
  }


  /**
   * Performs an Analytics query with custom {@link AnalyticsOptions}.
   *
   * @param statement the Analytics query statement as a raw string.
   * @param options the custom options for this analytics query.
   * @return the {@link ReactiveAnalyticsResult} once the response arrives successfully.
   */
  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement,
                                                      final AnalyticsOptions options) {
    return AnalyticsAccessor.analyticsQueryReactive(
      asyncCluster.core(),
      asyncCluster.analyticsRequest(statement, options)
    );
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link SearchRequest} once the response arrives successfully, inside a {@link Mono}
   */
  public Mono<ReactiveSearchResult> searchQuery(SearchQuery query) {
    return searchQuery(query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchRequest} once the response arrives successfully, inside a {@link Mono}
   */
  public Mono<ReactiveSearchResult> searchQuery(final SearchQuery query, final SearchOptions options) {
    return SearchAccessor.searchQueryReactive(asyncCluster.core(), asyncCluster.searchRequest(query, options));
  }

  /**
   * Opens a {@link ReactiveBucket} with the given name.
   *
   * @param name the name of the bucket to open.
   * @return a {@link ReactiveBucket} once opened.
   */
  public Mono<ReactiveBucket> bucket(final String name) {
    return Mono
      .fromFuture(asyncCluster.bucket(name))
      .map(ReactiveBucket::new);
  }

  /**
   * Performs a non-reversible shutdown of this {@link ReactiveCluster}.
   */
  public Mono<Void> shutdown() {
    return shutdown(environment().timeoutConfig().disconnectTimeout());
  }

  /**
   * Performs a non-reversible shutdown of this {@link ReactiveCluster}.
   *
   * @param timeout overriding the default disconnect timeout if needed.
   */
  public Mono<Void> shutdown(final Duration timeout) {
    return asyncCluster.shutdownInternal(timeout);
  }

}
