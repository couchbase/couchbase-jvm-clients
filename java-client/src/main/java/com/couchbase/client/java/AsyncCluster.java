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
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AsyncAnalyticsResult;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.SimpleQuery;
import com.couchbase.client.java.query.prepared.PreparedQueryAccessor;
import com.couchbase.client.java.search.AsyncSearchResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class AsyncCluster {

  private final Supplier<ClusterEnvironment> environment;
  private final Core core;
  private final Cluster cluster;

  public static AsyncCluster connect(final String username, final String password) {
    return Cluster.connect(ClusterEnvironment.create(username, password)).async();
  }

  public static AsyncCluster connect(final Credentials credentials) {
    return Cluster.connect(ClusterEnvironment.create(credentials)).async();
  }

  public static AsyncCluster connect(final String connectionString, final String username, final String password) {
    return Cluster.connect(ClusterEnvironment.create(connectionString, username, password)).async();
  }

  public static AsyncCluster connect(final String connectionString, final Credentials credentials) {
    return Cluster.connect(ClusterEnvironment.create(connectionString, credentials)).async();
  }

  public static AsyncCluster connect(final ClusterEnvironment environment) {
    return Cluster.connect(environment).async();
  }

  AsyncCluster(final Supplier<ClusterEnvironment> environment, final Cluster cluster) {
    this.environment = environment;
    this.core = Core.create(environment.get());
    this.cluster = cluster;
  }

  public Supplier<ClusterEnvironment> environment() {
    return environment;
  }

  public Core core() {
    return core;
  }

  public Cluster cluster() {
    return this.cluster;
  }

  public CompletableFuture<AsyncQueryResult> query(final String statement) {
    return query(statement, QueryOptions.DEFAULT);
  }

  public CompletableFuture<AsyncQueryResult> query(final String statement, final QueryOptions options) {
    notNullOrEmpty(statement, "Statement");
    notNull(options, "QueryOptions");

    Query query = SimpleQuery.create(statement);
    return query.prepared()
      ? PreparedQueryAccessor.queryAsync(core, query, options, environment, this.cluster().getPreparedQueryCache())
      : QueryAccessor.queryAsync(core, query, options, environment);
  }

  /*
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
*/
  public CompletableFuture<AsyncBucket> bucket(final String name) {
    return core
      .openBucket(name)
      .thenReturn(new AsyncBucket(name, core, environment.get()))
      .toFuture();
  }

  public CompletableFuture<Void> shutdown() {
    if (environment instanceof OwnedSupplier) {
      return environment.get().shutdownAsync(environment.get().timeoutConfig().disconnectTimeout());
    } else {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      cf.complete(null);
      return cf;
    }
  }
}