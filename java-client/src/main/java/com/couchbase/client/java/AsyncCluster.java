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
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.diagnostics.DiagnosticsResult;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.diagnostics.HealthPinger;
import com.couchbase.client.core.diagnostics.PingResult;
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.core.env.OwnedSupplier;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.ReducedAnalyticsErrorContext;
import com.couchbase.client.core.error.ReducedQueryErrorContext;
import com.couchbase.client.core.error.ReducedSearchErrorContext;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.search.SearchRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConnectionStringUtil;
import com.couchbase.client.java.analytics.AnalyticsAccessor;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.diagnostics.DiagnosticsOptions;
import com.couchbase.client.java.diagnostics.PingOptions;
import com.couchbase.client.java.diagnostics.WaitUntilReadyOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.analytics.AsyncAnalyticsIndexManager;
import com.couchbase.client.java.manager.bucket.AsyncBucketManager;
import com.couchbase.client.java.manager.query.AsyncQueryIndexManager;
import com.couchbase.client.java.manager.search.AsyncSearchIndexManager;
import com.couchbase.client.java.manager.user.AsyncUserManager;
import com.couchbase.client.java.query.QueryAccessor;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchAccessor;
import com.couchbase.client.java.search.SearchOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import static reactor.core.publisher.Mono.fromFuture;

/**
 * The {@link AsyncCluster} is the main entry point when connecting to a Couchbase cluster.
 *
 * <p>Note that most of the time you want to use the blocking {@link Cluster} or the powerful
 * reactive {@link ReactiveCluster} API instead. Use this API if you know what you are doing and
 * you want to build low-level, even faster APIs on top.</p>
 */
public class AsyncCluster {

  /**
   * Holds the supplied environment that gets used throughout the lifetime.
   */
  private final Supplier<ClusterEnvironment> environment;

  /**
   * Holds the internal core reference.
   */
  private final Core core;

  private final AsyncSearchIndexManager searchIndexManager;

  private final QueryAccessor queryAccessor;

  private final AsyncUserManager userManager;

  private final AsyncBucketManager bucketManager;

  private final AsyncQueryIndexManager queryIndexManager;

  private final AsyncAnalyticsIndexManager analyticsIndexManager;

  private final Authenticator authenticator;

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
    final Supplier<ClusterEnvironment> environmentSupplier = extractClusterEnvironment(connectionString, opts);
    return new AsyncCluster(
      environmentSupplier,
      opts.authenticator(),
      seedNodesFromConnectionString(connectionString, environmentSupplier.get())
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
    notNullOrEmpty(seedNodes, "SeedNodes");
    notNull(options, "ClusterOptions");

    final ClusterOptions.Built opts = options.build();
    return new AsyncCluster(extractClusterEnvironment(null, opts), opts.authenticator(), seedNodes);
  }

  /**
   * Helper method to extract the cluster environment from the connection string and options.
   *
   * @param connectionString the connection string which is used to populate settings into it.
   * @param opts the cluster options.
   * @return the cluster environment, created if not passed in or the one supplied from the user.
   */
  static Supplier<ClusterEnvironment> extractClusterEnvironment(final String connectionString,
                                                                final ClusterOptions.Built opts) {
    Supplier<ClusterEnvironment> envSupplier;
    if (opts.environment() == null) {
      ClusterEnvironment.Builder builder = ClusterEnvironment.builder();
      if (connectionString != null) {
        builder.load(new ConnectionStringPropertyLoader(connectionString));
      }
      envSupplier = new OwnedSupplier<>(builder.build());
    } else {
      envSupplier = opts::environment;
    }
    return envSupplier;
  }

  /**
   * Extracts the relevant seed nodes from the connection string.
   *
   * @param cs the connection string where it should be extracted from.
   * @param environment the environment to load certain properties that influence how it is loaded.
   * @return a set of seed nodes once extracted.
   */
  static Set<SeedNode> seedNodesFromConnectionString(final String cs, final ClusterEnvironment environment) {
    return ConnectionStringUtil.seedNodesFromConnectionString(cs, environment.ioConfig().dnsSrvEnabled());
  }

  /**
   * Creates a new cluster from a {@link ClusterEnvironment}.
   *
   * @param environment the environment to use for this cluster.
   */
  AsyncCluster(final Supplier<ClusterEnvironment> environment, final Authenticator authenticator, Set<SeedNode> seedNodes) {
    this.environment = environment;
    this.core = Core.create(environment.get(), authenticator, seedNodes);
    this.searchIndexManager = new AsyncSearchIndexManager(core);
    this.queryAccessor = new QueryAccessor(core);
    this.userManager = new AsyncUserManager(core);
    this.bucketManager = new AsyncBucketManager(core);
    this.queryIndexManager = new AsyncQueryIndexManager(this);
    this.analyticsIndexManager = new AsyncAnalyticsIndexManager(this);
    this.authenticator = authenticator;

    core.initGlobalConfig();
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
    return core;
  }

  /**
   * Provides access to the user management services.
   */
  @Stability.Volatile
  public AsyncUserManager users() {
    return userManager;
  }

  /**
   * Provides access to the bucket management services.
   */
  @Stability.Volatile
  public AsyncBucketManager buckets() {
    return bucketManager;
  }

  /**
   * Provides access to the Analytics index management services.
   */
  @Stability.Volatile
  public AsyncAnalyticsIndexManager analyticsIndexes() {
    return analyticsIndexManager;
  }

  /**
   * Provides access to the N1QL index management services.
   */
  @Stability.Volatile
  public AsyncQueryIndexManager queryIndexes() {
    return queryIndexManager;
  }

  /**
   * Provides access to the Full Text Search index management services.
   */
  @Stability.Volatile
  public AsyncSearchIndexManager searchIndexes() {
    return searchIndexManager;
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
    return queryAccessor.queryAsync(queryRequest(statement, opts), opts, serializer);
  }

  /**
   * Helper method to construct the query request.
   *
   * @param statement the statement of the query.
   * @param options the options.
   * @return the constructed query request.
   */
  QueryRequest queryRequest(final String statement, final QueryOptions.Built options) {
    notNullOrEmpty(statement, "Statement", () -> new ReducedQueryErrorContext(statement));
    Duration timeout = options.timeout().orElse(environment.get().timeoutConfig().queryTimeout());
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment.get().retryStrategy());

    final JsonObject query = JsonObject.create();
    query.put("statement", statement);
    query.put("timeout", encodeDurationToMs(timeout));
    options.injectParams(query);

    final byte[] queryBytes = query.toString().getBytes(StandardCharsets.UTF_8);
    final String clientContextId = query.getString("client_context_id");
    final InternalSpan span = environment()
      .requestTracer()
      .internalSpan(QueryRequest.OPERATION_NAME, options.parentSpan().orElse(null));

    QueryRequest request = new QueryRequest(timeout, core.context(), retryStrategy, authenticator, statement,
     queryBytes, options.readonly(), clientContextId, span);
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
    JsonSerializer serializer = opts.serializer() == null ? environment.get().jsonSerializer() : opts.serializer();
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
    Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().analyticsTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());

    JsonObject query = JsonObject.empty();
    query.put("statement", statement);
    query.put("timeout", encodeDurationToMs(timeout));
    opts.injectParams(query);

    final byte[] queryBytes = query.toString().getBytes(StandardCharsets.UTF_8);
    final String clientContextId = query.getString("client_context_id");
    final InternalSpan span = environment()
      .requestTracer()
      .internalSpan(AnalyticsRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    AnalyticsRequest request = new AnalyticsRequest(timeout, core.context(), retryStrategy, authenticator,
        queryBytes, opts.priority(), opts.readonly(), clientContextId, statement, span
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Performs a Full Text Search (FTS) query with default {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @return the {@link SearchRequest} once the response arrives successfully, inside a {@link CompletableFuture}
   */
  public CompletableFuture<SearchResult> searchQuery(final String indexName, final SearchQuery query) {
    return searchQuery(indexName, query, DEFAULT_SEARCH_OPTIONS);
  }

  /**
   * Performs a Full Text Search (FTS) query with custom {@link SearchOptions}.
   *
   * @param query the query, in the form of a {@link SearchQuery}
   * @param options the custom options for this query.
   * @return the {@link SearchRequest} once the response arrives successfully, inside a {@link CompletableFuture}
   */
  public CompletableFuture<SearchResult> searchQuery(final String indexName, final SearchQuery query, final SearchOptions options) {
    notNull(query, "SearchQuery", () -> new ReducedSearchErrorContext(indexName, null));
    notNull(options, "SearchOptions", () -> new ReducedSearchErrorContext(indexName, query.export().toMap()));
    SearchOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment.get().jsonSerializer() : opts.serializer();
    return SearchAccessor.searchQueryAsync(core, searchRequest(indexName, query, opts), serializer);
  }

  SearchRequest searchRequest(final String indexName, final SearchQuery query, final SearchOptions.Built opts) {
    notNullOrEmpty(indexName, "IndexName", () -> new ReducedSearchErrorContext(indexName, query.export().toMap()));
    JsonObject params = query.export();
    opts.injectParams(indexName, params);
    byte[] bytes = params.toString().getBytes(StandardCharsets.UTF_8);

    Duration timeout = opts.timeout().orElse(environment.get().timeoutConfig().searchTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.get().retryStrategy());

    final InternalSpan span = environment()
      .requestTracer()
      .internalSpan(SearchRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    SearchRequest request = new SearchRequest(timeout, core.context(), retryStrategy, authenticator, indexName, bytes, span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Opens a {@link AsyncBucket} with the given name.
   *
   * @param bucketName the name of the bucket to open.
   * @return a {@link AsyncBucket} once opened.
   */
  public AsyncBucket bucket(final String bucketName) {
    notNullOrEmpty(bucketName, "Name");
    core.openBucket(bucketName);
    return new AsyncBucket(bucketName, core, environment.get());
  }

  /**
   * Performs a non-reversible disconnect of this {@link AsyncCluster}.
   */
  public CompletableFuture<Void> disconnect() {
    return disconnect(environment.get().timeoutConfig().disconnectTimeout());
  }

  /**
   * Performs a non-reversible disconnect of this {@link AsyncCluster}.
   *
   * @param timeout overriding the default disconnect timeout if needed.
   */
  public CompletableFuture<Void> disconnect(final Duration timeout) {
    return disconnectInternal(timeout).toFuture();
  }

  /**
   * Can be called from other cluster instances so that code is not duplicated.
   *
   * @param timeout the timeout for the environment to shut down if owned.
   * @return a mono once complete.
   */
  Mono<Void> disconnectInternal(final Duration timeout) {
    return core.shutdown(timeout).flatMap(ignore -> {
      if (environment instanceof OwnedSupplier) {
        return environment.get().shutdownReactive(timeout);
      } else {
        return Mono.empty();
      }
    });
  }

  /**
   * Provides access to the internal query accessor.
   */
  @Stability.Internal
  QueryAccessor queryAccessor() {
    return queryAccessor;
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
      core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
      core.context().environment().userAgent().formattedShort(),
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
      core,
      opts.timeout(),
      opts.retryStrategy().orElse(environment.get().retryStrategy()),
      opts.serviceTypes(),
      opts.reportId(),
      true
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
    return WaitUntilReadyHelper.waitUntilReady(core, opts.serviceTypes(), timeout, opts.desiredState(), true);
  }

}
