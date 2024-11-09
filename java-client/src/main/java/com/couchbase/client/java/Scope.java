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
import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.eventing.ScopeEventingFunctionManager;
import com.couchbase.client.java.manager.search.ScopeSearchIndexManager;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryMetaData;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryRow;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.vector.VectorSearch;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_ANALYTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_QUERY_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_SEARCH_OPTIONS;

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
    notNull(options, "QueryOptions", () -> new ReducedQueryErrorContext(statement));
    final QueryOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ?  environment().jsonSerializer() : opts.serializer();
    return new QueryResult(async().queryOps.queryBlocking(statement, opts, asyncScope.queryContext, null, QueryAccessor::convertCoreQueryError), serializer);
  }

  /**
   * Executes a SQL++ query statement in this scope using default options,
   * (no query parameters, etc.), and passes result rows to the given
   * {@code rowAction} callback, one by one as they arrive from the server.
   * <p>
   * The callback action is guaranteed to execute in the same thread
   * (or virtual thread) that called this method. If the callback throws
   * an exception, the query is cancelled and the exception is re-thrown
   * by this method.
   * <p>
   * If the calling thread is interrupted, this method throws
   * {@link CancellationException} and sets the thread's interrupted flag.
   *
   * @param statement The SQL++ statement to execute.
   * @return Query metadata.
   * @throws CancellationException if the calling thread is interrupted.
   * @throws TimeoutException if the query does not complete before the timeout expires.
   * @throws RuntimeException if the row action callback throws an exception.
   */
  @Stability.Volatile
  public QueryMetaData queryStreaming(String statement, Consumer<QueryRow> rowAction) {
    return queryStreaming(statement, DEFAULT_QUERY_OPTIONS, rowAction);
  }

  /**
   * Executes a SQL++ query statement in this scope using the specified options,
   * (query parameters, etc.), and passes result rows to the given
   * {@code rowAction} callback, one by one as they arrive from the server.
   * <p>
   * The callback action is guaranteed to execute in the same thread
   * (or virtual thread) that called this method. If the callback throws
   * an exception, the query is cancelled and the exception is re-thrown
   * by this method.
   * <p>
   * If the calling thread is interrupted, this method throws
   * {@link CancellationException} and sets the thread's interrupted flag.
   *
   * @param statement The SQL++ statement to execute.
   * @param options Custom query options.
   * @return Query metadata.
   * @throws CancellationException if the calling thread is interrupted.
   * @throws TimeoutException if the query does not complete before the timeout expires.
   * @throws RuntimeException if the row action callback throws an exception.
   */
  @Stability.Volatile
  public QueryMetaData queryStreaming(String statement, QueryOptions options, Consumer<QueryRow> rowAction) {
    notNull(options, "QueryOptions", () -> new ReducedQueryErrorContext(statement));
    final QueryOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();

    CoreQueryMetaData coreMetadata = async().queryOps.queryBlockingStreaming(
      statement,
      opts,
      asyncScope.queryContext,
      null,
      QueryAccessor::convertCoreQueryError,
      coreRow -> rowAction.accept(new QueryRow(coreRow.data(), serializer))
    );

    return new QueryMetaData(coreMetadata);
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

  /**
   * Performs a request against the Full Text Search (FTS) service, with default {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link Cluster} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @SinceCouchbase("7.6")
  public SearchResult search(final String indexName, final SearchRequest searchRequest) {
    return search(indexName, searchRequest, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a request against the Full Text Search (FTS) service, with custom {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link Cluster} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @SinceCouchbase("7.6")
  public SearchResult search(final String indexName, final SearchRequest searchRequest, final SearchOptions options) {
    return block(async().search(indexName, searchRequest, options));
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link Cluster} instead.
   * <p>
   * New users should consider the newer {@link #search(String, SearchRequest)} interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
   * and/or can also be used to perform a {@link VectorSearch}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link SearchResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public SearchResult searchQuery(final String indexName, final SearchQuery query) {
    return searchQuery(indexName, query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   * <p>
   * This method is for scoped FTS indexes.  For global indexes, use {@link Cluster} instead.
   * <p>
   * New users should consider the newer {@link #search(String, SearchRequest)} interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
   * and/or can also be used to perform a {@link VectorSearch}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchResult} once the response arrives successfully.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public SearchResult searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    return block(async().searchQuery(indexName, query, options));
  }

  /**
   * Allows management of scope FTS indexes.
   */
  @SinceCouchbase("7.6")
  public ScopeSearchIndexManager searchIndexes() {
    return new ScopeSearchIndexManager(asyncScope.searchIndexes());
  }

  /**
   * Provides access to the eventing function management services for functions in this scope.
   */
  @Stability.Volatile
  @SinceCouchbase("7.1")
  public ScopeEventingFunctionManager eventingFunctions() {
    return new ScopeEventingFunctionManager(asyncScope.eventingFunctions());
  }
}
