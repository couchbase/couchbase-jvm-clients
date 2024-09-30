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
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.search.CoreSearchOps;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.queries.CoreSearchRequest;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.HealthPinger;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.OwnedOrExternal;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.error.context.ReducedQueryErrorContext;
import com.couchbase.client.core.error.context.ReducedSearchErrorContext;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.ClusterCleanupTask;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.client.core.util.Jdk8Cleaner;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.diagnostics.DiagnosticsOptions;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.http.AsyncCouchbaseHttpClient;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.analytics.AsyncAnalyticsIndexManager;
import com.couchbase.client.java.manager.bucket.AsyncBucketManager;
import com.couchbase.client.java.manager.eventing.AsyncEventingFunctionManager;
import com.couchbase.client.java.manager.query.AsyncQueryIndexManager;
import com.couchbase.client.java.manager.search.AsyncSearchIndexManager;
import com.couchbase.client.java.manager.user.AsyncUserManager;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.vector.VectorSearch;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.couchbase.client.core.env.OwnedOrExternal.external;
import static com.couchbase.client.core.env.OwnedOrExternal.owned;
import static com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString;
import static com.couchbase.client.core.util.ConnectionStringUtil.checkConnectionString;
import static com.couchbase.client.core.util.Golang.encodeDurationToMs;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_ANALYTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_DIAGNOSTICS_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_PING_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_QUERY_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_SEARCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCluster.DEFAULT_WAIT_UNTIL_READY_OPTIONS;

/**
 * The {@link AsyncCluster} is the main entry point when connecting to a Couchbase cluster using the async API.
 * <p>
 * Note that most of the time you want to use the blocking {@link Cluster} or the powerful
 * reactive {@link ReactiveCluster} API instead. Use this API if you know what you are doing and
 * you want to build low-level, even faster APIs on top.
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
public class AsyncCluster {

  /**
   * Holds the environment that gets used throughout the lifetime.
   */
  private final OwnedOrExternal<ClusterEnvironment> environment;

  private final CoreCouchbaseOps couchbaseOps;

  private final Authenticator authenticator;

  /**
   * Stores already opened buckets for reuse.
   */
  private final Map<String, AsyncBucket> bucketCache = new ConcurrentHashMap<>();

  /**
   * Strategy for performing query operations.
   */
  final CoreQueryOps queryOps;

  /**
   * Strategy for performing search operations.
   */
  final CoreSearchOps searchOps;

  /**
   * Connect to a Couchbase cluster with a username and a password as authentication credentials.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param username the name of the user with appropriate permissions on the cluster.
   * @param password the password of the user with appropriate permissions on the cluster.
   * @return the instantiated {@link AsyncCluster}.
   */
  public static AsyncCluster connect(final String connectionString, final String username, final String password) {
    return connect(connectionString, clusterOptions(PasswordAuthenticator.create(username, password)));
  }

  /**
   * Connect to a Couchbase cluster with a connection string and custom options.
   *
   * @param connectionString connection string used to locate the Couchbase cluster.
   * @param options custom options when creating the cluster.
   * @return the instantiated {@link AsyncCluster}.
   */
  public static AsyncCluster connect(final String connectionString, final ClusterOptions options) {
    notNullOrEmpty(connectionString, "ConnectionString");
    notNull(options, "ClusterOptions");

    final ClusterOptions.Built opts = options.build();
    final ConnectionString connStr = ConnectionString.create(connectionString);
    final OwnedOrExternal<ClusterEnvironment> environment = extractClusterEnvironment(connStr, opts);
    return new AsyncCluster(
      environment,
      opts.authenticator(),
      connStr
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
   * @return the instantiated {@link AsyncCluster}.
   */
  public static AsyncCluster connect(final Set<SeedNode> seedNodes, final ClusterOptions options) {
    return connect(asConnectionString(seedNodes).original(), options);
  }

  /**
   * Helper method to extract the cluster environment from the connection string and options.
   *
   * @param connectionString the connection string which is used to populate settings into it.
   * @param opts the cluster options.
   * @return the cluster environment, created if not passed in or the one supplied from the user.
   */
  static OwnedOrExternal<ClusterEnvironment> extractClusterEnvironment(final ConnectionString connectionString, final ClusterOptions.Built opts) {
    OwnedOrExternal<ClusterEnvironment> env;
    if (opts.environment() == null) {
      ClusterEnvironment.Builder builder = ClusterEnvironment.builder();
      if (opts.environmentCustomizer() != null) {
        opts.environmentCustomizer().accept(builder);
      }
      builder.load(new ConnectionStringPropertyLoader(connectionString));
      builder.loadSystemProperties();
      env = owned(builder.build());
    } else {
      env = external(opts.environment());
    }

    checkConnectionString(env.get(), env.isOwned(), connectionString);

    return env;
  }

  /**
   * Creates a new cluster from a {@link ClusterEnvironment}.
   *
   * @param environment the environment to use for this cluster.
   */
  AsyncCluster(
    final OwnedOrExternal<ClusterEnvironment> environment,
    final Authenticator authenticator,
    final ConnectionString connectionString
  ) {
    this.environment = environment;
    this.couchbaseOps = CoreCouchbaseOps.create(environment.get(), authenticator, connectionString);
    this.authenticator = authenticator;
    this.queryOps = couchbaseOps.queryOps();
    this.searchOps = couchbaseOps.searchOps(null);

    if (couchbaseOps instanceof Core) {
      ((Core) couchbaseOps).initGlobalConfig();
    }

    Duration autoDisconnectTimeout = environment().timeoutConfig().disconnectTimeout()
      .plus(Duration.ofSeconds(15)); // Give it more time than a user-initiated timeout, since we *really* want it to succeed.

    Jdk8Cleaner.registerWithOneShotCleaner(this, new ClusterCleanupTask(
      disconnectInternal(autoDisconnectTimeout),
      environment().eventBus(),
      disconnected
    ));
  }

  /**
   * Provides access to the configured {@link ClusterEnvironment} for this cluster.
   */
  public ClusterEnvironment environment() {
    return environment.get();
  }

  /**
   * Provides access to the underlying {@link Core}.
   *
   * <p>This is advanced API, use with care!</p>
   */
  @Stability.Volatile
  public Core core() {
    return couchbaseOps.asCore();
  }

  @Stability.Internal
  public CoreCouchbaseOps couchbaseOps() {
    return couchbaseOps;
  }

  /**
   * Returns a specialized HTTP client for making requests to the Couchbase Server REST API.
   */
  @Stability.Volatile
  public AsyncCouchbaseHttpClient httpClient() {
    return new AsyncCouchbaseHttpClient(this);
  }

  /**
   * Provides access to the user management services.
   */
  public AsyncUserManager users() {
    return new AsyncUserManager(core(), this);
  }

  /**
   * Provides access to the bucket management services.
   */
  public AsyncBucketManager buckets() {
    return new AsyncBucketManager(couchbaseOps, this);
  }

  /**
   * Provides access to the Analytics index management services.
   */
  public AsyncAnalyticsIndexManager analyticsIndexes() {
    return new AsyncAnalyticsIndexManager(this);
  }

  /**
   * Provides access to the N1QL index management services.
   */
  public AsyncQueryIndexManager queryIndexes() {
    return new AsyncQueryIndexManager(couchbaseOps().queryOps(), couchbaseOps().coreResources().requestTracer(), this);
  }

  /**
   * Provides access to the Full Text Search index management services.
   */
  public AsyncSearchIndexManager searchIndexes() {
    return new AsyncSearchIndexManager(couchbaseOps, this);
  }

  /**
   * Provides access to the eventing function management services for functions in the admin scope.
   */
  @Stability.Uncommitted
  public AsyncEventingFunctionManager eventingFunctions() {
    return new AsyncEventingFunctionManager(core(), this, null);
  }

  /**
   * Performs a N1QL query with default {@link QueryOptions}.
   *
   * @param statement the N1QL query statement as a raw string.
   * @return the {@link QueryResult} once the response arrives successfully.
   */
  public CompletableFuture<QueryResult> query(final String statement) {
    return query(statement, DEFAULT_QUERY_OPTIONS);
  }

  /**
   * Performs a N1QL query with custom {@link QueryOptions}.
   *
   * @param statement the N1QL query statement as a raw string.
   * @param options the custom options for this query.
   * @return the {@link QueryResult} once the response arrives successfully.
   */
  public CompletableFuture<QueryResult> query(final String statement, final QueryOptions options) {
    notNull(options, "QueryOptions", () -> new ReducedQueryErrorContext(statement));
    final QueryOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.get().jsonSerializer() : opts.serializer();
    return queryOps.queryAsync(statement, opts, null, null, QueryAccessor::convertCoreQueryError)
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
    JsonSerializer serializer = opts.serializer() == null ? environment.get().jsonSerializer() : opts.serializer();
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
    Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().analyticsTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());

    JsonObject query = JsonObject.create();
    query.put("statement", statement);
    query.put("timeout", encodeDurationToMs(timeout));
    opts.injectParams(query);

    final byte[] queryBytes = query.toString().getBytes(StandardCharsets.UTF_8);
    final String clientContextId = query.getString("client_context_id");
    final RequestSpan span = couchbaseOps()
      .coreResources()
      .requestTracer()
      .requestSpan(TracingIdentifiers.SPAN_REQUEST_ANALYTICS, opts.parentSpan().orElse(null));
    AnalyticsRequest request = new AnalyticsRequest(timeout, core().context(), retryStrategy, authenticator,
      queryBytes, opts.priority(), opts.readonly(), clientContextId, statement, span, null, null
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Performs a request against the Full Text Search (FTS) service, with default {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for global FTS indexes.  For scoped indexes, use {@link AsyncScope} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link CompletableFuture}
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<SearchResult> search(final String indexName, final SearchRequest searchRequest) {
    return search(indexName, searchRequest, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a request against the Full Text Search (FTS) service, with custom {@link SearchOptions}.
   * <p>
   * This can be used to perform a traditional FTS query, and/or a vector search.
   * <p>
   * This method is for global FTS indexes.  For scoped indexes, use {@link AsyncScope} instead.
   *
   * @param searchRequest the request, in the form of a {@link SearchRequest}
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link CompletableFuture}
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<SearchResult> search(final String indexName, final SearchRequest searchRequest, final SearchOptions options) {
    notNull(searchRequest, "SearchRequest", () -> new ReducedSearchErrorContext(indexName, null));
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, null));
    CoreSearchRequest coreRequest = searchRequest.toCore();
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.get().jsonSerializer() : opts.serializer();
    return searchOps.searchAsync(indexName, coreRequest, opts)
      .thenApply(r -> new SearchResult(r, serializer));
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   * <p>
   * This method is for global FTS indexes.  For scoped indexes, use {@link AsyncScope} instead.
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
   * New users should consider the newer {@link #search(String, SearchRequest)} interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
   * and/or can also be used to perform a {@link VectorSearch}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchResult} once the response arrives successfully, inside a {@link CompletableFuture}
   */
  public CompletableFuture<SearchResult> searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    notNull(query, "SearchQuery", () -> new ReducedSearchErrorContext(indexName, null));
    CoreSearchQuery coreQuery = query.toCore();
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, coreQuery));
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.get().jsonSerializer() : opts.serializer();
    return searchOps.searchQueryAsync(indexName, coreQuery, opts)
            .thenApply(r -> new SearchResult(r, serializer));
  }

  /**
   * Opens a {@link AsyncBucket} with the given name.
   *
   * @param bucketName the name of the bucket to open.
   * @return a {@link AsyncBucket} once opened.
   */
  public AsyncBucket bucket(final String bucketName) {
    notNullOrEmpty(bucketName, "Name");
    return bucketCache.computeIfAbsent(bucketName, n -> {
      if (couchbaseOps instanceof Core) {
        ((Core) couchbaseOps).openBucket(n);
      }
      return new AsyncBucket(n, couchbaseOps, environment.get(), this);
    });
  }

  /**
   * Performs a non-reversible disconnect of this {@link AsyncCluster}.
   * <p>
   * If this method is used, the default disconnect timeout on the environment is used. Please use the companion
   * overload ({@link #disconnect(Duration)} if you want to provide a custom duration.
   * <p>
   * If a custom {@link ClusterEnvironment} has been passed in during connect, it is <strong>VERY</strong> important to
   * shut it down after calling this method. This will prevent any in-flight tasks to be stopped prematurely.
   */
  public CompletableFuture<Void> disconnect() {
    return disconnect(environment.get().timeoutConfig().disconnectTimeout());
  }

  /**
   * Performs a non-reversible disconnect of this {@link AsyncCluster}.
   * <p>
   * If a custom {@link ClusterEnvironment} has been passed in during connect, it is <strong>VERY</strong> important to
   * shut it down after calling this method. This will prevent any in-flight tasks to be stopped prematurely.
   *
   * @param timeout overriding the default disconnect timeout if needed.
   */
  public CompletableFuture<Void> disconnect(final Duration timeout) {
    return disconnectInternal(timeout).toFuture();
  }

  /**
   * Remembers whether {@link #disconnectInternal} was called.
   */
  private final AtomicBoolean disconnected = new AtomicBoolean();

  /**
   * Can be called from other cluster instances so that code is not duplicated.
   *
   * @param timeout the timeout for the environment to shut down if owned.
   * @return a mono once complete.
   */
  Mono<Void> disconnectInternal(final Duration timeout) {
    return disconnectInternal(
      disconnected,
      timeout,
      couchbaseOps,
      environment.get(),
      environment.isOwned()
    );
  }

  /**
   * Central place for cluster/core disconnect logic.
   * <p>
   * This method is static and does not reference the cluster,
   * so it can be called after the cluster becomes unreachable.
   */
  static Mono<Void> disconnectInternal(
    final AtomicBoolean disconnected,
    final Duration timeout,
    final CoreCouchbaseOps couchbaseOps,
    final CoreEnvironment environment,
    final boolean ownsEnvironment
    ) {
    return couchbaseOps.shutdown(timeout)
      .then(ownsEnvironment ? environment.shutdownReactive(timeout) : Mono.empty())
      .then(Mono.fromRunnable(() -> disconnected.set(true)));
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
  public CompletableFuture<DiagnosticsResult> diagnostics() {
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
  public CompletableFuture<DiagnosticsResult> diagnostics(final DiagnosticsOptions options) {
    notNull(options, "DiagnosticsOptions");
    final DiagnosticsOptions.Built opts = options.build();

    return Mono.defer(() -> Mono.just(new DiagnosticsResult(
      core().diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
      core().context().environment().userAgent().formattedShort(),
      opts.reportId().orElse(UUID.randomUUID().toString())
    ))).toFuture();
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
  public CompletableFuture<PingResult> ping() {
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
  public CompletableFuture<PingResult> ping(final PingOptions options) {
    notNull(options, "PingOptions");
    final PingOptions.Built opts = options.build();
    return HealthPinger.ping(
      core(),
      opts.timeout(),
      opts.retryStrategy().orElse(environment.get().retryStrategy()),
      opts.serviceTypes(),
      opts.reportId(),
      Optional.empty()
    ).toFuture();
  }

  /**
   * Waits until the desired {@link ClusterState} is reached.
   * <p>
   * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
   * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
   * and usable before moving on.
   *
   * @param timeout the maximum time to wait until readiness.
   * @return a completable future that completes either once ready or timeout.
   */
  public CompletableFuture<Void> waitUntilReady(final Duration timeout) {
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
   * @return a completable future that completes either once ready or timeout.
   */
  public CompletableFuture<Void> waitUntilReady(final Duration timeout, final WaitUntilReadyOptions options) {
    notNull(options, "WaitUntilReadyOptions");
    final WaitUntilReadyOptions.Built opts = options.build();
    return couchbaseOps.waitUntilReady(opts.serviceTypes(), timeout, opts.desiredState(), null);
  }

}
