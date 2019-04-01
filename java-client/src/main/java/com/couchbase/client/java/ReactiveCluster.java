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

import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.env.OwnedSupplier;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.search.SearchAccessor;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

public class ReactiveCluster {

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
  public static ReactiveCluster connect(final String connectionString, final String username,
                                        final String password) {
    return new ReactiveCluster(new OwnedSupplier<>(
      ClusterEnvironment.create(connectionString, username, password)
    ));
  }

  /**
   * Connect to a Couchbase cluster with custom {@link Credentials}.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param credentials custom credentials used when connecting to the cluster.
   * @return if properly connected, returns a {@link ReactiveCluster}.
   */
  public static ReactiveCluster connect(final String connectionString,
                                        final Credentials credentials) {
    return new ReactiveCluster(new OwnedSupplier<>(
      ClusterEnvironment.create(connectionString, credentials)
    ));
  }

  /**
   * Connect to a Couchbase cluster with a custom {@link ClusterEnvironment}.
   *
   * @param environment the custom environment with its properties used to connect to the cluster.
   * @return if properly connected, returns a {@link ReactiveCluster}.
   */
  public static ReactiveCluster connect(final ClusterEnvironment environment) {
    return new ReactiveCluster(() -> environment);
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
    return this.query(statement, QueryOptions.DEFAULT);
  }

  /**
   * Performs a N1QL query with custom {@link QueryOptions}.
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link ReactiveQueryResult} once the response arrives successfully.
   */
  public Mono<ReactiveQueryResult> query(final String statement, final QueryOptions options) {
    return QueryAccessor.queryReactive(
      asyncCluster.core(),
      asyncCluster.queryRequest(statement, options)
    );
  }

  /**
   * Performs an Analytics query with default {@link AnalyticsOptions}.
   *
   * @param statement the Analytics query statement as a raw string.
   * @return the {@link ReactiveAnalyticsResult} once the response arrives successfully.
   */
  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement) {
    return analyticsQuery(statement, AnalyticsOptions.DEFAULT);
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
    return searchQuery(query, SearchOptions.DEFAULT);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchRequest} once the response arrives successfully, inside a {@link Mono}
   */
  public Mono<ReactiveSearchResult> searchQuery(SearchQuery query, SearchOptions options) {
    SearchRequest request = SearchAccessor.searchRequest(query, options, async().core().context(), environment());
    return SearchAccessor.searchQueryReactive(async().core(), request);
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
    return Mono.fromFuture(asyncCluster.shutdown());
  }

}
