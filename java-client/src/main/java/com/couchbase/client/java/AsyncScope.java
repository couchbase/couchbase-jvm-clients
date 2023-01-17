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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryAccessorProtostellar;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.transactions.internal.ErrorUtil;
import com.couchbase.client.java.transactions.internal.SingleQueryTransactions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.util.Golang.encodeDurationToMs;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
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
public class AsyncScope {

  /**
   * Holds the inner core reference to pass on.
   */
  private final Core core;

  /**
   * The name of the bucket at which this scope belongs.
   */
  private final String bucketName;

  /**
   * The actual name of this scope.
   */
  private final String scopeName;

  /**
   * The attached environment to pass on and use.
   */
  private final ClusterEnvironment environment;

  /**
   * for executing queries
   */
  private final QueryAccessor queryAccessor;
  /**
   * Stores already opened collections for reuse.
   */
  private final Map<String, AsyncCollection> collectionCache = new ConcurrentHashMap<>();

  /**
   * Creates a new {@link AsyncScope}.
   *
   * @param scopeName the name of the scope.
   * @param bucketName the name of the bucket.
   * @param core the attached core.
   * @param environment the attached environment.
   */
  AsyncScope(final String scopeName, final String bucketName, final Core core,
             final ClusterEnvironment environment) {
    this.scopeName = scopeName;
    this.bucketName = bucketName;
    this.core = core;
    this.environment = environment;
    this.queryAccessor = new QueryAccessor(core);
  }

  /**
   * The name of the scope.
   */
  public String name() {
    return scopeName;
  }

  /**
   * The name of the bucket this scope is attached to.
   */
  public String bucketName() {
    return bucketName;
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return core;
  }

  /**
   * Provides access to the configured {@link ClusterEnvironment} for this scope.
   */
  public ClusterEnvironment environment() {
    return environment;

  }
  /**
   * Provides access to the underlying {@link QueryAccessor}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  QueryAccessor queryAccessor() {
    return queryAccessor;
  }

  /**
   * Opens the default collection for this scope.
   *
   * <p>Note that this method is package private because it is called from the bucket class only!</p>
   *
   * @return the default collection once opened.
   */
  AsyncCollection defaultCollection() {
    return maybeCreateAsyncCollection(
      CollectionIdentifier.DEFAULT_COLLECTION,
      !scopeName.equals(CollectionIdentifier.DEFAULT_SCOPE)
    );
  }

  /**
   * Opens a collection for this scope with an explicit name.
   *
   * @param collectionName the collection name.
   * @return the requested collection if successful.
   */
  public AsyncCollection collection(final String collectionName) {
    boolean defaultScopeAndCollection = collectionName.equals(CollectionIdentifier.DEFAULT_COLLECTION)
        && scopeName.equals(CollectionIdentifier.DEFAULT_SCOPE);
    return maybeCreateAsyncCollection(collectionName, !defaultScopeAndCollection);
  }

  /**
   * Helper method to maybe create a new collection or load it from the cache.
   *
   * @param collectionName the name of the collection.
   * @param refreshMap if the collection map should be refreshed on the config provider.
   * @return a collection, either from the cache or a freshly populated one.
   */
  private AsyncCollection maybeCreateAsyncCollection(final String collectionName, final boolean refreshMap) {
    return collectionCache.computeIfAbsent(collectionName, name -> {
      if (refreshMap) {
        core
          .configurationProvider()
          .refreshCollectionId(new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(name)));
      }
      return new AsyncCollection(name, scopeName, bucketName, core, environment);
    });
  }

  /**
   * Performs a N1QL query with default {@link QueryOptions} in a Scope
   *
   * @param statement the N1QL query statement.
   * @return the {@link QueryResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<QueryResult> query(final String statement) {
    return query(statement, DEFAULT_QUERY_OPTIONS);
  }

  /**
   * Performs a N1QL query with custom {@link QueryOptions} in a Scope.
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link QueryResult} once the response arrives successfully.
   */
  public CompletableFuture<QueryResult> query(final String statement, final QueryOptions options) {
    notNull(options, "QueryOptions", () -> new ReducedQueryErrorContext(statement));
    final QueryOptions.Built opts = options.build();
    if (opts.asTransaction()) {
      return SingleQueryTransactions.singleQueryTransactionBuffered(core, environment, statement, bucketName, scopeName, opts)
              .onErrorResume(ErrorUtil::convertTransactionFailedInternal)
              .toFuture();
    }
    else {
      JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
      if (core.isProtostellar()) {
        return QueryAccessorProtostellar.async(core, opts,
          QueryAccessorProtostellar.request(core(), statement, opts, environment(), bucketName, scopeName),
          serializer);
      }
      else {
        return queryAccessor.queryAsync(queryRequest(bucketName(), scopeName, statement, opts, core, environment()), opts,
          serializer);
      }
    }
  }

  /**
   * Helper method to construct the query request. ( copied from Cluster )
   *
   * @param statement the statement of the query.
   * @param options the options.
   * @return the constructed query request.
   */
   static QueryRequest queryRequest(final String bucketName, final String scopeName, final String statement,
                                    final QueryOptions.Built options, final Core core, final ClusterEnvironment environment) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedQueryErrorContext(statement));
    Duration timeout = options.timeout().orElse(environment.timeoutConfig().queryTimeout());
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment.retryStrategy());

    final JsonObject query = JsonObject.create();
    query.put("statement", statement);
    query.put("timeout", encodeDurationToMs(timeout));
    String queryContext = QueryRequest.queryContext(bucketName, scopeName);
    query.put("query_context", queryContext);
    options.injectParams(query);
    final byte[] queryBytes = query.toString().getBytes(StandardCharsets.UTF_8);
    final String clientContextId = query.getString("client_context_id");
    final RequestSpan span = environment.requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_QUERY,
        options.parentSpan().orElse(null));

    QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy, core.context().authenticator(),
        statement, queryBytes, options.readonly(), clientContextId, span, bucketName, scopeName, null);
    request.context().clientContext(options.clientContext());
    return request;
  }

  /**
   * Performs an Analytics query with default {@link AnalyticsOptions}.
   *
   * @param statement the Analytics query statement as a raw string.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   */
  public CompletableFuture<AnalyticsResult> analyticsQuery(final String statement) {
    return analyticsQuery(statement, DEFAULT_ANALYTICS_OPTIONS);
  }

  /**
   * Performs an Analytics query with custom {@link AnalyticsOptions}.
   *
   * @param statement the Analytics query statement as a raw string.
   * @param options the custom options for this analytics query.
   * @return the {@link AnalyticsResult} once the response arrives successfully.
   */
  public CompletableFuture<AnalyticsResult> analyticsQuery(final String statement, final AnalyticsOptions options) {
    notNull(options, "AnalyticsOptions", () -> new ReducedAnalyticsErrorContext(statement));
    AnalyticsOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return AnalyticsAccessor.analyticsQueryAsync(core, analyticsRequest(statement, opts), serializer);
  }

  /**
   * Helper method to craft an analytics request.
   *
   * @param statement the statement to use.
   * @param opts the built analytics options.
   * @return the created analytics request.
   */
  AnalyticsRequest analyticsRequest(final String statement, final AnalyticsOptions.Built opts) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedAnalyticsErrorContext(statement));
    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().analyticsTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    JsonObject query = JsonObject.create();
    query.put("statement", statement);
    query.put("timeout", encodeDurationToMs(timeout));
    query.put("query_context", AnalyticsRequest.queryContext(bucketName, scopeName));
    opts.injectParams(query);

    final byte[] queryBytes = query.toString().getBytes(StandardCharsets.UTF_8);
    final String clientContextId = query.getString("client_context_id");
    final RequestSpan span = environment()
        .requestTracer()
        .requestSpan(TracingIdentifiers.SPAN_REQUEST_ANALYTICS, opts.parentSpan().orElse(null));
    AnalyticsRequest request = new AnalyticsRequest(timeout, core.context(), retryStrategy, core.context().authenticator(),
        queryBytes, opts.priority(), opts.readonly(), clientContextId, statement, span, bucketName, scopeName
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

}
