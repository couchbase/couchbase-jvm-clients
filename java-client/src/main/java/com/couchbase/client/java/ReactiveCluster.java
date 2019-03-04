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
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.query.SimpleQuery;
import com.couchbase.client.java.query.prepared.PreparedQueryAccessor;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class ReactiveCluster {

  private final AsyncCluster asyncCluster;

  public static ReactiveCluster connect(final String username, final String password) {
    return Cluster.connect(ClusterEnvironment.create(username, password)).reactive();
  }

  public static ReactiveCluster connect(final Credentials credentials) {
    return Cluster.connect(ClusterEnvironment.create(credentials)).reactive();
  }

  public static ReactiveCluster connect(final String connectionString, final String username, final String password) {
    return Cluster.connect(ClusterEnvironment.create(connectionString, username, password)).reactive();
  }

  public static ReactiveCluster connect(final String connectionString, final Credentials credentials) {
    return Cluster.connect(ClusterEnvironment.create(connectionString, credentials)).reactive();
  }

  public static ReactiveCluster connect(final ClusterEnvironment environment) {
    return Cluster.connect(environment).reactive();
  }

  ReactiveCluster(final AsyncCluster asyncCluster) {
    this.asyncCluster = asyncCluster;
  }

  public AsyncCluster async() {
    return asyncCluster;
  }

  public Mono<ReactiveQueryResult> query(final String statement) {
    return this.query(statement, QueryOptions.DEFAULT);
  }

  public Mono<ReactiveQueryResult> query(final String statement, final QueryOptions options) {
    notNullOrEmpty(statement, "Statement");
    notNull(options, "QueryOptions");

    QueryOptions.BuiltQueryOptions builtOpts = options.build();

    if (builtOpts.isPrepared()) {
      return Mono.defer(() -> Mono.fromFuture(() ->
              PreparedQueryAccessor.queryReactive(async().core(), SimpleQuery.createPrepared(statement), builtOpts, async().environment(),
                      async().cluster().getPreparedQueryCache())));
    } else {
      return Mono.defer(() -> Mono.fromFuture(() ->
              QueryAccessor.queryReactive(async().core(), SimpleQuery.create(statement), builtOpts, async().environment())));
    }
  }

  /*
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
  */

  public Mono<ReactiveBucket> bucket(String name) {
    return Mono.fromFuture(asyncCluster.bucket(name)).map(ReactiveBucket::new);
  }

  public Mono<Void> shutdown() {
    return null;
  }

}
