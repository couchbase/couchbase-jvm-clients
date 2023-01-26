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
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.context.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.error.context.ReducedSearchErrorContext;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.diagnostics.DiagnosticsOptions;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.http.ReactiveCouchbaseHttpClient;
import com.couchbase.client.java.manager.analytics.ReactiveAnalyticsIndexManager;
import com.couchbase.client.java.manager.bucket.ReactiveBucketManager;
import com.couchbase.client.java.manager.eventing.ReactiveEventingFunctionManager;
import com.couchbase.client.java.manager.query.AsyncQueryIndexManager;
import com.couchbase.client.java.manager.query.ReactiveQueryIndexManager;
import com.couchbase.client.java.manager.search.ReactiveSearchIndexManager;
import com.couchbase.client.java.manager.user.ReactiveUserManager;
import com.couchbase.client.java.query.QueryAccessorProtostellar;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.client.java.search.SearchAccessor;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.ReactiveSearchResult;
import com.couchbase.client.java.transactions.ReactiveTransactions;
import com.couchbase.client.java.transactions.Transactions;
import com.couchbase.client.java.transactions.internal.ErrorUtil;
import com.couchbase.client.java.transactions.internal.SingleQueryTransactions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.AsyncCluster.extractClusterEnvironment;
import static com.couchbase.client.java.AsyncCluster.seedNodesFromConnectionString;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static com.couchbase.client.java.diagnostics.DiagnosticsOptions.diagnosticsOptions;
import static com.couchbase.client.java.diagnostics.PingOptions.pingOptions;
import static com.couchbase.client.java.diagnostics.WaitUntilReadyOptions.waitUntilReadyOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.java.search.SearchOptions.searchOptions;

/**
 * The {@link Cluster} is the main entry point when connecting to a Couchbase cluster using the reactive APIs.
 * <p>
 * Most likely you want to start out by using the {@link #connect(String, String, String)} entry point. For more
 * advanced options you want to use the {@link #connect(String, ClusterOptions)} method. The entry point that allows
 * overriding the seed nodes ({@link #connect(Set, ClusterOptions)} is only needed if you run a couchbase cluster
 * at non-standard ports.
 * <p>
 * When the application shuts down (or the SDK is not needed anymore), you are required to call {@link #disconnect()}.
 * If you omit this step, the application will terminate (all spawned threads are daemon threads) but any operations
 * or work in-flight will not be able to complete and lead to undesired side-effects. Note that disconnect will also
 * shutdown all associated {@link Bucket buckets}.
 * <p>
 * Cluster-level operations like {@link #query(String)} will not work unless at leas one bucket is opened against a
 * pre 6.5 cluster. If you are using 6.5 or later, you can run cluster-level queries without opening a bucket. All
 * of these operations are lazy, so the SDK will bootstrap in the background and service queries as quickly as possible.
 * This also means that the first operations might be a bit slower until all sockets are opened in the background and
 * the configuration is loaded. If you want to wait explicitly, you can utilize the {@link #waitUntilReady(Duration)}
 * method before performing your first query.
 * <p>
 * The SDK will only work against Couchbase Server 5.0 and later, because RBAC (role-based access control) is a first
 * class concept since 3.0 and therefore required.
 */
public class ReactiveCluster {

  static final QueryOptions DEFAULT_QUERY_OPTIONS = queryOptions();
  static final SearchOptions DEFAULT_SEARCH_OPTIONS = searchOptions();
  static final AnalyticsOptions DEFAULT_ANALYTICS_OPTIONS = analyticsOptions();
  static final DiagnosticsOptions DEFAULT_DIAGNOSTICS_OPTIONS = diagnosticsOptions();
  static final WaitUntilReadyOptions DEFAULT_WAIT_UNTIL_READY_OPTIONS = waitUntilReadyOptions();
  static final PingOptions DEFAULT_PING_OPTIONS = pingOptions();

  /**
   * Holds the underlying async cluster reference.
   */
  private final AsyncCluster asyncCluster;

  /**
   * Stores already opened buckets for reuse.
   */
  private final Map<String, ReactiveBucket> bucketCache = new ConcurrentHashMap<>();

  /**
   * Connect to a Couchbase cluster with a username and a password as credentials.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param username the name of the user with appropriate permissions on the cluster.
   * @param password the password of the user with appropriate permissions on the cluster.
   * @return the instantiated {@link ReactiveCluster}.
   */
  public static ReactiveCluster connect(final String connectionString, final String username, final String password) {
    return connect(connectionString, clusterOptions(PasswordAuthenticator.create(username, password)));
  }

  /**
   * Connect to a Couchbase cluster with custom {@link Authenticator}.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param options custom options when creating the cluster.
   * @return the instantiated {@link ReactiveCluster}.
   */
  public static ReactiveCluster connect(final String connectionString, final ClusterOptions options) {
    notNullOrEmpty(connectionString, "ConnectionString");
    notNull(options, "ClusterOptions");
    final ClusterOptions.Built opts = options.build();
    final Supplier<ClusterEnvironment> environmentSupplier = extractClusterEnvironment(connectionString, opts);
    return new ReactiveCluster(
      environmentSupplier,
      opts.authenticator(),
      seedNodesFromConnectionString(connectionString, environmentSupplier.get()),
      connectionString
    );
  }

  /**
   * Connect to a Couchbase cluster with a list of seed nodes and custom options.
   * <p>
   * Please note that you likely only want to use this method if you need to pass in custom ports for specific
   * seed nodes during bootstrap. Otherwise we recommend relying ont he simpler {@link #connect(String, ClusterOptions)}
   * method instead.
   *
   * @param seedNodes the seed nodes used to connect to the cluster.
   * @param options custom options when creating the cluster.
   * @return the instantiated {@link ReactiveCluster}.
   */
  public static ReactiveCluster connect(final Set<SeedNode> seedNodes, final ClusterOptions options) {
    notNullOrEmpty(seedNodes, "SeedNodes");
    notNull(options, "ClusterOptions");

    final ClusterOptions.Built opts = options.build();
    return new ReactiveCluster(extractClusterEnvironment(asConnectionString(seedNodes), opts), opts.authenticator(),
      seedNodes, null);
  }


  /**
   * Creates a new cluster from a {@link ClusterEnvironment}.
   *
   * @param environment the environment to use for this cluster.
   */
  private ReactiveCluster(final Supplier<ClusterEnvironment> environment, final Authenticator authenticator,
                          final Set<SeedNode> seedNodes, final String connectionString) {
    this(new AsyncCluster(environment, authenticator, seedNodes, connectionString));
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
   * Returns a specialized HTTP client for making requests to the Couchbase Server REST API.
   */
  @Stability.Volatile
  public ReactiveCouchbaseHttpClient httpClient() {
    return new ReactiveCouchbaseHttpClient(asyncCluster.httpClient());
  }

  /**
   * Provides access to the user management services.
   */
  public ReactiveUserManager users() {
    return new ReactiveUserManager(asyncCluster.users());
  }

  /**
   * Provides access to the bucket management services.
   */
  public ReactiveBucketManager buckets() {
    return new ReactiveBucketManager(async().buckets());
  }

  /**
   * Provides access to the Analytics index management services.
   */
  public ReactiveAnalyticsIndexManager analyticsIndexes() {
    return new ReactiveAnalyticsIndexManager(async());
  }

  /**
   * Provides access to the search index management services.
   */
  public ReactiveSearchIndexManager searchIndexes() {
    return new ReactiveSearchIndexManager(async().searchIndexes());
  }

  /**
   * Provides access to the N1QL index management services.
   */
  public ReactiveQueryIndexManager queryIndexes() {
    return new ReactiveQueryIndexManager(new AsyncQueryIndexManager(async()));
  }

  /**
   * Provides access to the eventing function management services.
   */
  @Stability.Uncommitted
  public ReactiveEventingFunctionManager eventingFunctions() {
    return new ReactiveEventingFunctionManager(async().eventingFunctions());
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
    notNull(options, "QueryOptions", () -> new ReducedQueryErrorContext(statement));
    final QueryOptions.Built opts = options.build();
    if (opts.asTransaction()) {
      return SingleQueryTransactions.singleQueryTransactionStreaming(core(), environment(), statement, null, null, opts, ErrorUtil::convertTransactionFailedSingleQuery);
    }
    else {
      JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
      return Mono.defer(() -> {
        if (core().isProtostellar()) {
          return QueryAccessorProtostellar.reactive(
            core(),
            opts,
            QueryAccessorProtostellar.request(core(), statement, opts, environment(), null, null),
            serializer
          );
        }
        else {
          return asyncCluster.queryAccessor().queryReactive(
            asyncCluster.queryRequest(statement, opts),
            opts,
            serializer
          );
        }
      });
    }
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
  public Mono<ReactiveAnalyticsResult> analyticsQuery(final String statement, final AnalyticsOptions options) {
    notNull(options, "AnalyticsOptions", () -> new ReducedAnalyticsErrorContext(statement));
    AnalyticsOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
    return Mono.defer(() -> {
      return AnalyticsAccessor.analyticsQueryReactive(
        asyncCluster.core(),
        asyncCluster.analyticsRequest(statement, opts),
        serializer
      );
    });
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link SearchRequest} once the response arrives successfully, inside a {@link Mono}
   */
  public Mono<ReactiveSearchResult> searchQuery(final String indexName, SearchQuery query) {
    return searchQuery(indexName, query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchRequest} once the response arrives successfully, inside a {@link Mono}
   */
  public Mono<ReactiveSearchResult> searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    notNull(query, "SearchQuery", () -> new ReducedSearchErrorContext(indexName, null));
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, query.export().toMap()));
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
    return Mono.defer(() -> {
      return SearchAccessor.searchQueryReactive(asyncCluster.core(), asyncCluster.searchRequest(indexName, query, opts), serializer);
    });
  }

  /**
   * Opens a {@link ReactiveBucket} with the given name.
   *
   * @param bucketName the name of the bucket to open.
   * @return a {@link ReactiveBucket} once opened.
   */
  public ReactiveBucket bucket(final String bucketName) {
    return bucketCache.computeIfAbsent(bucketName, n -> new ReactiveBucket(asyncCluster.bucket(n)));
  }

  /**
   * Performs a non-reversible disconnect of this {@link ReactiveCluster}.
   * <p>
   * If this method is used, the default disconnect timeout on the environment is used. Please use the companion
   * overload ({@link #disconnect(Duration)} if you want to provide a custom duration.
   * <p>
   * If a custom {@link ClusterEnvironment} has been passed in during connect, it is <strong>VERY</strong> important to
   * shut it down after calling this method. This will prevent any in-flight tasks to be stopped prematurely.
   */
  public Mono<Void> disconnect() {
    return disconnect(environment().timeoutConfig().disconnectTimeout());
  }

  /**
   * Performs a non-reversible disconnect of this {@link ReactiveCluster}.
   * <p>
   * If a custom {@link ClusterEnvironment} has been passed in during connect, it is <strong>VERY</strong> important to
   * shut it down after calling this method. This will prevent any in-flight tasks to be stopped prematurely.
   *
   * @param timeout overriding the default disconnect timeout if needed.
   */
  public Mono<Void> disconnect(final Duration timeout) {
    return asyncCluster.disconnectInternal(timeout);
  }

  /**
   * Runs a diagnostic report on the current state of the cluster from the SDKs point of view.
   * <p>
   * Please note that it does not perform any I/O to do this, it will only use the current known state of the cluster
   * to assemble the report (so, if for example no N1QL query has been run the socket pool might be empty and as
   * result not show up in the report).
   *
   * @return the {@link DiagnosticsResult} once complete.
   */
  public Mono<DiagnosticsResult> diagnostics() {
    return diagnostics(DEFAULT_DIAGNOSTICS_OPTIONS);
  }

  /**
   * Runs a diagnostic report with custom options on the current state of the cluster from the SDKs point of view.
   * <p>
   * Please note that it does not perform any I/O to do this, it will only use the current known state of the cluster
   * to assemble the report (so, if for example no N1QL query has been run the socket pool might be empty and as
   * result not show up in the report).
   *
   * @param options options that allow to customize the report.
   * @return the {@link DiagnosticsResult} once complete.
   */
  public Mono<DiagnosticsResult> diagnostics(final DiagnosticsOptions options) {
    return Mono.defer(() -> Mono.fromFuture(asyncCluster.diagnostics(options)));
  }

  /**
   * Performs application-level ping requests against services in the couchbase cluster.
   * <p>
   * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
   * not wish to perform I/O, consider using the {@link #diagnostics()} instead. You can also combine the functionality
   * of both APIs as needed, which is {@link #waitUntilReady(Duration)} is doing in its implementation as well.
   *
   * @return the {@link PingResult} once complete.
   */
  public Mono<PingResult> ping() {
    return ping(DEFAULT_PING_OPTIONS);
  }

  /**
   * Performs application-level ping requests with custom options against services in the couchbase cluster.
   * <p>
   * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
   * not wish to perform I/O, consider using the {@link #diagnostics(DiagnosticsOptions)} instead. You can also combine
   * the functionality of both APIs as needed, which is {@link #waitUntilReady(Duration)} is doing in its
   * implementation as well.
   *
   * @return the {@link PingResult} once complete.
   */
  public Mono<PingResult> ping(final PingOptions options) {
    return Mono.defer(() -> Mono.fromFuture(asyncCluster.ping(options)));
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
   * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on.
   *
   * @param timeout the maximum time to wait until readiness.
   * @return a mono that completes either once ready or timeout.
   */
  public Mono<Void> waitUntilReady(final Duration timeout) {
    return waitUntilReady(timeout, DEFAULT_WAIT_UNTIL_READY_OPTIONS);
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online" by default, or the timeout is reached. Since the
   * SDK is bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on. You can tune the properties through {@link WaitUntilReadyOptions}.
   *
   * @param timeout the maximum time to wait until readiness.
   * @param options the options to customize the readiness waiting.
   * @return a mono that completes either once ready or timeout.
   */
  public Mono<Void> waitUntilReady(final Duration timeout, final WaitUntilReadyOptions options) {
    return Mono.defer(() -> Mono.fromFuture(asyncCluster.waitUntilReady(timeout, options)));
  }

  /**
   * Allows access to transactions.
   *
   * @return the {@link Transactions} interface.
   */
  @Stability.Uncommitted
  public ReactiveTransactions transactions() {
    return new ReactiveTransactions(core(), environment().jsonSerializer());
  }
}
