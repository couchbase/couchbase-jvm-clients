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
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryAccessorProtostellar;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_ANALYTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_QUERY_OPTIONS;

/**
 * The scope identifies a group of collections and allows high application
 * density as a result.
 *
 * <p>If no scope is explicitly provided, the default scope is used.</p>
 *
 * @since 3.0.0
 */
public class Scope {

  /**
   * The underlying async scope which actually performs the actions.
   */
  private final AsyncScope asyncScope;

  /**
   * Holds the adjacent reactive scope.
   */
  private final ReactiveScope reactiveScope;

  /**
   * Stores already opened collections for reuse.
   */
  private final Map<String, Collection> collectionCache = new ConcurrentHashMap<>();

  /**
   * Creates a new {@link Scope}.
   *
   * @param asyncScope the underlying async scope.
   */
  Scope(final AsyncScope asyncScope) {
    this.asyncScope = asyncScope;
    this.reactiveScope = new ReactiveScope(asyncScope);
  }

  /**
   * The name of the scope.
   *
   * @return the name of the scope.
   */
  public String name() {
    return asyncScope.name();
  }


  /**
   * The name of the bucket this scope is attached to.
   */
  public String bucketName() {
    return asyncScope.bucketName();
  }

  /**
   * Returns the underlying async scope.
   */
  public AsyncScope async() {
    return asyncScope;
  }

  /**
   * Provides access to the related {@link ReactiveScope}.
   */
  public ReactiveScope reactive() {
    return reactiveScope;
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return asyncScope.core();
  }

  /**
   * Provides access to the configured {@link ClusterEnvironment} for this scope.
   */
  public ClusterEnvironment environment() {
    return asyncScope.environment();
  }

  /**
   * Opens the default collection for this scope.
   *
   * @return the default collection once opened.
   */
  Collection defaultCollection() {
    return collectionCache.computeIfAbsent(
      CollectionIdentifier.DEFAULT_COLLECTION,
      n -> new Collection(asyncScope.defaultCollection())
    );
  }

  /**
   * Opens a collection for this scope with an explicit name.
   *
   * @param collectionName the collection name.
   * @return the requested collection if successful.
   */
  public Collection collection(final String collectionName) {
    return collectionCache.computeIfAbsent(collectionName, n -> new Collection(asyncScope.collection(n)));
  }

  /**
   * Performs a N1QL query with default {@link QueryOptions} in a Scope
   *
   * @param statement the N1QL query statement.
   * @return the {@link QueryResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public QueryResult query(final String statement) {
    return query(statement, DEFAULT_QUERY_OPTIONS);
  }

  /**
   * Performs a N1QL query with custom {@link QueryOptions} in a Scope
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link QueryResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public QueryResult query(final String statement, final QueryOptions options) {
    if (core().isProtostellar()) {
      QueryOptions.Built opts = options.build();
      JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
      return QueryAccessorProtostellar.blocking(core(), opts,
        QueryAccessorProtostellar.request(core(), statement, options.build(), environment(), bucketName(), name()),
        serializer);
    }
    else {
      return block(async().query(statement, options));
    }
  }

  /**
   * Performs an Analytics query with default {@link AnalyticsOptions} on a scope
   *
   * @param statement the Analytics query statement as a raw string.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public AnalyticsResult analyticsQuery(final String statement) {
    return analyticsQuery(statement, DEFAULT_ANALYTICS_OPTIONS);
  }

  /**
   * Performs an Analytics query with custom {@link AnalyticsOptions} on a scope
   *
   * @param statement the Analytics query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public AnalyticsResult analyticsQuery(final String statement, final AnalyticsOptions options) {
    return block(async().analyticsQuery(statement, options));
  }
}
