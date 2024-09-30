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
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.search.CoreSearchOps;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.queries.CoreSearchRequest;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.error.context.ReducedSearchErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.PreventsGarbageCollection;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.eventing.AsyncScopeEventingFunctionManager;
import com.couchbase.client.java.manager.search.AsyncScopeSearchIndexManager;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.vector.VectorSearch;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.util.Golang.encodeDurationToMs;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_ANALYTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_QUERY_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_SEARCH_OPTIONS;
import static java.util.Objects.requireNonNull;

/**
 * The scope identifies a group of collections and allows high application
 * density as a result.
 *
 * <p>If no scope is explicitly provided, the default scope is used.</p>
 *
 * @since 3.0.0
 */
public class AsyncScope {

  private final CoreCouchbaseOps couchbaseOps;

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
   * For executing queries.
   */
  final CoreQueryOps queryOps;
  final CoreQueryContext queryContext;

  /**
   * Stores already opened collections for reuse.
   */
  private final Map<String, AsyncCollection> collectionCache = new ConcurrentHashMap<>();

  /**
   * Strategy for performing search operations.
   */
  final CoreSearchOps searchOps;

  @PreventsGarbageCollection
  private final AsyncCluster cluster;

  AsyncScope(
    final String scopeName,
    final String bucketName,
    final CoreCouchbaseOps couchbaseOps,
    final ClusterEnvironment environment,
    final AsyncCluster cluster
  ) {
    this.scopeName = requireNonNull(scopeName);
    this.bucketName = requireNonNull(bucketName);
    this.couchbaseOps = requireNonNull(couchbaseOps);
    this.environment = requireNonNull(environment);
    this.queryOps = couchbaseOps.queryOps();
    queryContext = CoreQueryContext.of(bucketName, scopeName);
    this.searchOps = couchbaseOps.searchOps(new CoreBucketAndScope(bucketName, name()));
    this.cluster = requireNonNull(cluster);
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
   *
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return couchbaseOps.asCore();
  }

  /**
   * Provides access to the configured {@link ClusterEnvironment} for this scope.
   */
  public ClusterEnvironment environment() {
    return environment;

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
      CoreKeyspace keyspace = new CoreKeyspace(bucketName, scopeName, collectionName);
      if (refreshMap) {
        if (couchbaseOps instanceof Core) {
          ((Core) couchbaseOps)
            .configurationProvider()
            .refreshCollectionId(keyspace.toCollectionIdentifier());
        }
      }
      return new AsyncCollection(keyspace, couchbaseOps, environment, cluster);
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
    JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return queryOps.queryAsync(statement, opts, queryContext, null, QueryAccessor::convertCoreQueryError)
      .thenApply(r -> new QueryResult(r, serializer));
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
    return AnalyticsAccessor.analyticsQueryAsync(core(), analyticsRequest(statement, opts), serializer);
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
    final RequestSpan span = couchbaseOps.coreResources()
        .requestTracer()
        .requestSpan(TracingIdentifiers.SPAN_REQUEST_ANALYTICS, opts.parentSpan().orElse(null));
    AnalyticsRequest request = new AnalyticsRequest(timeout, core().context(), retryStrategy, core().context().authenticator(),
        queryBytes, opts.priority(), opts.readonly(), clientContextId, statement, span, bucketName, scopeName
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Performs a request against the Full Text Search (FTS) service, with default {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link AsyncCluster} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link CompletableFuture}
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @SinceCouchbase("7.6")
  public CompletableFuture<SearchResult> search(final String indexName, final SearchRequest searchRequest) {
    return search(indexName, searchRequest, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a request against the Full Text Search (FTS) service, with custom {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link AsyncCluster} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link CompletableFuture}
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @SinceCouchbase("7.6")
  public CompletableFuture<SearchResult> search(final String indexName, final SearchRequest searchRequest, final SearchOptions options) {
    notNull(searchRequest, "SearchRequest", () -> new ReducedSearchErrorContext(indexName, null));
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, null));
    CoreSearchRequest coreRequest = searchRequest.toCore();
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return searchOps.searchAsync(indexName, coreRequest, opts)
      .thenApply(r -> new SearchResult(r, serializer));
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link AsyncCluster} instead.
   * <p>
   * New users should consider the newer {@link #search(String, SearchRequest)} interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
   * and/or can also be used to perform a {@link VectorSearch}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link CompletableFuture}
   */
  public CompletableFuture<SearchResult> searchQuery(final String indexName, final SearchQuery query) {
    return searchQuery(indexName, query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link AsyncCluster} instead.
   * <p>
   * New users should consider the newer {@link #search(String, SearchRequest)} interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
   * and/or can also be used to perform a {@link VectorSearch}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link CompletableFuture}
   */
  @Stability.Volatile
  public CompletableFuture<SearchResult> searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    notNull(query, "SearchQuery", () -> new ReducedSearchErrorContext(indexName, null));
    CoreSearchQuery coreQuery = query.toCore();
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, coreQuery));
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return searchOps.searchQueryAsync(indexName, coreQuery, opts)
            .thenApply(r -> new SearchResult(r, serializer));
  }

  /**
   * Allows managed scope FTS indexes.
   */
  @SinceCouchbase("7.6")
  public AsyncScopeSearchIndexManager searchIndexes() {
    return new AsyncScopeSearchIndexManager(couchbaseOps, this, cluster);
  }

  /**
   * Provides access to the eventing function management services for functions in this scope.
   */
  @Stability.Volatile
  @SinceCouchbase("7.1")
  public AsyncScopeEventingFunctionManager eventingFunctions() {
    return new AsyncScopeEventingFunctionManager(core(), this.cluster, new CoreBucketAndScope(bucketName, name()));
  }
}
