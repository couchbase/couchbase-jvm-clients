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
import com.couchbase.client.core.diag.DiagnosticsResult;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.RequestTimeoutException;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.diagnostics.DiagnosticsOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.analytics.AnalyticsIndexManager;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.query.QueryIndexManager;
import com.couchbase.client.java.manager.search.SearchIndexManager;
import com.couchbase.client.java.manager.user.UserManager;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;

import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;

import static com.couchbase.client.java.AsyncCluster.extractClusterEnvironment;
import static com.couchbase.client.java.AsyncCluster.seedNodesFromConnectionString;
import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_ANALYTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_DIAGNOSTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_QUERY_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_SEARCH_OPTIONS;

/**
 * The {@link Cluster} is the main entry point when connecting to a Couchbase cluster.
 */
public class Cluster {

  /**
   * Holds the underlying async cluster reference.
   */
  private final AsyncCluster asyncCluster;

  /**
   * Holds the adjacent reactive cluster reference.
   */
  private final ReactiveCluster reactiveCluster;

  /**
   * Holds the index manager.
   */
  private final SearchIndexManager searchIndexManager;


  private final UserManager userManager;

  private final BucketManager bucketManager;

  private final QueryIndexManager queryIndexManager;

  private final AnalyticsIndexManager analyticsIndexManager;

  /**
   * Connect to a Couchbase cluster with a username and a password as credentials.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param username the name of the user with appropriate permissions on the cluster.
   * @param password the password of the user with appropriate permissions on the cluster.
   * @return if properly connected, returns a {@link Cluster}.
   */
  public static Cluster connect(final String connectionString, final String username, final String password) {
    return connect(connectionString, clusterOptions(PasswordAuthenticator.create(username, password)));
  }

  /**
   * Connect to a Couchbase cluster with custom {@link Authenticator}.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param options custom options when creating the cluster.
   * @return if properly connected, returns a {@link Cluster}.
   */
  public static Cluster connect(final String connectionString, final ClusterOptions options) {
    ClusterOptions.Built opts = options.build();
    Supplier<ClusterEnvironment> environmentSupplier = extractClusterEnvironment(connectionString, opts);

    Set<SeedNode> seedNodes;
    if (opts.seedNodes() != null && !opts.seedNodes().isEmpty()) {
      seedNodes = opts.seedNodes();
    } else {
      seedNodes = seedNodesFromConnectionString(connectionString, environmentSupplier.get());
    }
    return new Cluster(environmentSupplier, opts.authenticator(), seedNodes);
  }

  /**
   * Creates a new cluster from a {@link ClusterEnvironment}.
   *
   * @param environment the environment to use for this cluster.
   */
  private Cluster(final Supplier<ClusterEnvironment> environment, final Authenticator authenticator, Set<SeedNode> seedNodes) {
    this.asyncCluster = new AsyncCluster(environment, authenticator, seedNodes);
    this.reactiveCluster = new ReactiveCluster(asyncCluster);
    this.searchIndexManager = new SearchIndexManager(asyncCluster.searchIndexes());
    this.userManager = new UserManager(asyncCluster.users());
    this.bucketManager = new BucketManager(asyncCluster.buckets());
    this.queryIndexManager = new QueryIndexManager(asyncCluster.queryIndexes());
    this.analyticsIndexManager = new AnalyticsIndexManager(this);
  }

  /**
   * Provides access to the underlying {@link AsyncCluster}.
   */
  public AsyncCluster async() {
    return asyncCluster;
  }

  /**
   * Provides access to the related {@link ReactiveCluster}.
   */
  public ReactiveCluster reactive() {
    return reactiveCluster;
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
   * Provides access to the user management services.
   */
  @Stability.Volatile
  public UserManager users() {
    return userManager;
  }

  /**
   * Provides access to the bucket management services.
   */
  @Stability.Volatile
  public BucketManager buckets() {
    return bucketManager;
  }

  /**
   * Provides access to the Analytics index management services.
   */
  @Stability.Volatile
  public AnalyticsIndexManager analyticsIndexes() {
    return analyticsIndexManager;
  }

  /**
   * Provides access to the N1QL index management services.
   */
  @Stability.Volatile
  public QueryIndexManager queryIndexes() {
    return queryIndexManager;
  }

  /**
   * Provides access to the Full Text Search index management services.
   */
  @Stability.Volatile
  public SearchIndexManager searchIndexes() {
    return searchIndexManager;
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
   * @return the {@link QueryResult} once the response arrives successfully.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public QueryResult query(final String statement) {
    return query(statement, DEFAULT_QUERY_OPTIONS);
  }

  /**
   * Performs a N1QL query with custom {@link QueryOptions}.
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link QueryResult} once the response arrives successfully.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public QueryResult query(final String statement, final QueryOptions options) {
    return block(async().query(statement, options));
  }

  /**
   * Performs an analytics query with default {@link AnalyticsOptions}.
   *
   * @param statement the query statement as a raw string.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public AnalyticsResult analyticsQuery(final String statement) {
    return analyticsQuery(statement, DEFAULT_ANALYTICS_OPTIONS);
  }

  /**
   * Performs an analytics query with custom {@link AnalyticsOptions}.
   *
   * @param statement the query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public AnalyticsResult analyticsQuery(final String statement, final AnalyticsOptions options) {
    return block(async().analyticsQuery(statement, options));
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link SearchRequest} once the response arrives successfully.
   */
  public SearchResult searchQuery(final String indexName, final SearchQuery query) {
    return searchQuery(indexName, query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchRequest} once the response arrives successfully.
   */
  public SearchResult searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    return block(asyncCluster.searchQuery(indexName, query, options));
  }

  /**
   * Opens a {@link Bucket} with the given name.
   *
   * @param bucketName the name of the bucket to open.
   * @return a {@link Bucket} once opened.
   */
  public Bucket bucket(final String bucketName) {
    return new Bucket(asyncCluster.bucket(bucketName));
  }

  /**
   * Performs a non-reversible disconnect of this {@link Cluster}.
   */
  public void disconnect() {
    block(asyncCluster.disconnect());
  }

  /**
   * Performs a non-reversible disconnect of this {@link Cluster}.
   *
   * @param timeout overriding the default disconnect timeout if needed.
   */
  public void disconnect(final Duration timeout) {
    block(asyncCluster.disconnect(timeout));
  }

  /**
   * Returns a {@link DiagnosticsResult}, reflecting the SDK's current view of all its existing connections to the
   * cluster.
   *
   * @param options options on the generation of the report
   * @return a {@link DiagnosticsResult}
   */
  @Stability.Volatile
  public DiagnosticsResult diagnostics(DiagnosticsOptions options) {
    return block(asyncCluster.diagnostics(options));
  }

  /**
   * Returns a {@link DiagnosticsResult}, reflecting the SDK's current view of all its existing connections to the
   * cluster.
   *
   * @return a {@link DiagnosticsResult}
   */
  @Stability.Volatile
  public DiagnosticsResult diagnostics() {
    return block(asyncCluster.diagnostics(DEFAULT_DIAGNOSTICS_OPTIONS));
  }
}

