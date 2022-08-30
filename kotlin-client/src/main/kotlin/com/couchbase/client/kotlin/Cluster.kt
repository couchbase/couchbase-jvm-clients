/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.core.diagnostics.EndpointDiagnostics
import com.couchbase.client.core.diagnostics.HealthPinger
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.env.CertificateAuthenticator
import com.couchbase.client.core.env.ConnectionStringPropertyLoader
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.env.SeedNode
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.search.SearchRequest
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionString.Scheme.COUCHBASES
import com.couchbase.client.core.util.ConnectionStringUtil
import com.couchbase.client.core.util.ConnectionStringUtil.checkConnectionString
import com.couchbase.client.kotlin.Cluster.Companion.connect
import com.couchbase.client.kotlin.analytics.AnalyticsFlowItem
import com.couchbase.client.kotlin.analytics.AnalyticsParameters
import com.couchbase.client.kotlin.analytics.AnalyticsPriority
import com.couchbase.client.kotlin.analytics.AnalyticsScanConsistency
import com.couchbase.client.kotlin.analytics.internal.AnalyticsExecutor
import com.couchbase.client.kotlin.annotations.UncommittedCouchbaseApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.diagnostics.DiagnosticsResult
import com.couchbase.client.kotlin.diagnostics.PingResult
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.env.dsl.ClusterEnvironmentConfigBlock
import com.couchbase.client.kotlin.env.env
import com.couchbase.client.kotlin.http.CouchbaseHttpClient
import com.couchbase.client.kotlin.internal.await
import com.couchbase.client.kotlin.internal.putIfNotEmpty
import com.couchbase.client.kotlin.internal.putIfNotNull
import com.couchbase.client.kotlin.internal.putIfTrue
import com.couchbase.client.kotlin.manager.bucket.BucketManager
import com.couchbase.client.kotlin.manager.query.QueryIndexManager
import com.couchbase.client.kotlin.manager.user.UserManager
import com.couchbase.client.kotlin.query.QueryFlowItem
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryProfile
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.query.QueryScanConsistency
import com.couchbase.client.kotlin.query.internal.QueryExecutor
import com.couchbase.client.kotlin.search.Direction.DESCENDING
import com.couchbase.client.kotlin.search.Highlight
import com.couchbase.client.kotlin.search.Score
import com.couchbase.client.kotlin.search.SearchFacet
import com.couchbase.client.kotlin.search.SearchFlowItem
import com.couchbase.client.kotlin.search.SearchMetadata
import com.couchbase.client.kotlin.search.SearchPage
import com.couchbase.client.kotlin.search.SearchQuery
import com.couchbase.client.kotlin.search.SearchResult
import com.couchbase.client.kotlin.search.SearchRow
import com.couchbase.client.kotlin.search.SearchScanConsistency
import com.couchbase.client.kotlin.search.SearchSort
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * Main entry point for interacting with a Couchbase Server cluster.
 *
 * Start by calling one of the [Cluster.connect] factory methods.
 * The sample code linked below should help get you off the ground quickly.
 *
 * IMPORTANT: If you are connecting to a version of Couchbase Server prior
 * to 6.5, you must open at least one bucket before doing cluster-level
 * operations like [query].
 *
 * The [connect] and [bucket] methods return immediately; the actual work
 * of opening sockets and loading bucket configuration happens asynchronously
 * in the background. Your first requests after connecting might take longer
 * than usual, since they need to wait for this background work to finish.
 * If you want to wait explicitly instead, call the [waitUntilReady] method
 * before issuing your first request.
 *
 * When your application is ready to shut down, be sure to call [disconnect].
 * This gives any in-flight requests a chance to complete, avoiding undesired
 * side-effects. Disconnecting the cluster also disconnects all [Bucket] and
 * [Collection] objects acquired from the cluster.
 *
 * If you are sharing a [ClusterEnvironment] between clusters, be sure to
 * shut down the environment by calling [ClusterEnvironment.shutdownSuspend]
 * (or one of its "shutdown*" cousins) after disconnecting the clusters.
 *
 * @sample com.couchbase.client.kotlin.samples.quickstart
 * @sample com.couchbase.client.kotlin.samples.configureTlsUsingDsl
 * @sample com.couchbase.client.kotlin.samples.configureTlsUsingBuilder
 */
public class Cluster internal constructor(
    environment: ClusterEnvironment,
    private val ownsEnvironment: Boolean,
    private val authenticator: Authenticator,
    seedNodes: Set<SeedNode>,
    connectionString: String,
) {
    internal val core: Core = Core.create(environment, authenticator, seedNodes, connectionString)

    internal val env: ClusterEnvironment
        get() = core.env

    private val bucketCache = ConcurrentHashMap<String, Bucket>()

    private val queryExecutor = QueryExecutor(core)
    private val analyticsExecutor = AnalyticsExecutor(core)

    init {
        core.initGlobalConfig()
    }

    /**
     * Waits until SDK bootstrap is complete and the desired [ClusterState]
     * is observed.
     *
     * Calling this method is optional. Without it, operations performed
     * before the cluster is ready may take longer than usual, since
     * the SDK bootstrap needs to complete first.
     *
     * @param timeout the maximum time to wait for readiness.
     * @param services the service types to wait for, or empty set to wait for all services.
     * @throws UnambiguousTimeoutException if not ready before timeout
     */
    public suspend fun waitUntilReady(
        timeout: Duration,
        services: Set<ServiceType> = emptySet(),
        desiredState: ClusterState = ClusterState.ONLINE,
    ): Cluster {
        WaitUntilReadyHelper.waitUntilReady(core, services, timeout.toJavaDuration(), desiredState, Optional.empty())
            .await()
        return this
    }

    /**
     * Opens the [Bucket] with the given name.
     *
     * @see Bucket.waitUntilReady
     */
    public fun bucket(name: String): Bucket = bucketCache.computeIfAbsent(name) { key ->
        core.openBucket(key)
        Bucket(key, this, core)
    }

    /**
     * An HTTP client for the Couchbase REST API.
     *
     * This is an "escape hatch" to use in areas where the Kotlin SDK's
     * management APIs do not provide full coverage of the REST API.
     *
     * @sample com.couchbase.client.kotlin.samples.httpClientGetBucketStats
     * @sample com.couchbase.client.kotlin.samples.httpClientGetWithQueryParameters
     * @sample com.couchbase.client.kotlin.samples.httpClientPostWithFormData
     * @sample com.couchbase.client.kotlin.samples.httpClientPostWithJsonBody
     */
    @Stability.Volatile
    public val httpClient: CouchbaseHttpClient = CouchbaseHttpClient(this)

    /**
     * A manager for administering buckets (create, update, drop, flush, list, etc.)
     */
    public val buckets: BucketManager = BucketManager(core)

    /**
     * A manager for administering users (create, update, drop, etc.)
     */
    public val users: UserManager = UserManager(core, httpClient)

    /**
     * A manager for administering N1QL indexes
     */
    public val queryIndexes: QueryIndexManager = QueryIndexManager(this)

    /**
     * Pings the Couchbase cluster's global services.
     * (To ping bucket-level services like KV as well, use [Bucket.ping] instead.)
     *
     * This operation performs I/O against services and endpoints to assess their health.
     * If you do not wish to perform I/O, consider using [diagnostics] instead.
     *
     * @param services The services to ping, or an empty set to ping all services (the default).
     * @param reportId An arbitrary ID to assign to the report.
     */
    public suspend fun ping(
        common: CommonOptions = CommonOptions.Default,
        services: Set<ServiceType> = emptySet(),
        reportId: String = UUID.randomUUID().toString(),
    ): PingResult = PingResult(
        HealthPinger.ping(
            core,
            Optional.ofNullable(common.timeout?.toJavaDuration()),
            common.retryStrategy,
            services,
            Optional.of(reportId),
            Optional.empty(),
        ).awaitSingle()
    )

    /**
     * Generates a diagnostic report on the current state of the cluster from the SDKs point of view.
     *
     * This operation does not perform any I/O. It uses only the last known state of the cluster
     * to assemble the report. For example, if no N1QL query has been executed, the Query service's
     * socket pool might be empty, and as result not show up in the report.
     *
     * If you wish to actively assess the health of the cluster by performing I/O,
     * consider using [ping] instead.
     *
     * @param reportId An arbitrary ID to assign to the report.
     */
    public fun diagnostics(
        reportId: String = UUID.randomUUID().toString(),
    ): DiagnosticsResult = DiagnosticsResult(
        com.couchbase.client.core.diagnostics.DiagnosticsResult(
            core.diagnostics().collect(Collectors.groupingBy(EndpointDiagnostics::type)),
            core.context().environment().userAgent().formattedShort(),
            reportId,
        )
    )

    /**
     * Returns a Flow which can be collected to execute a Full-Text Search query
     * and process the results.
     *
     * The returned Flow is cold, meaning the query is not executed unless
     * the Flow is collected. If you collect the flow multiple times,
     * the query is executed each time.
     *
     * The extension function `Flow<SearchFlowItem>.execute()` may be used
     * when the results are known to fit in memory. It simply collects the flow
     * into a [SearchResult].
     *
     * For larger query results, prefer the streaming version which takes a
     * lambda to invoke when each row is received from the server:
     * `Flow<SearchFlowItem>.execute { row -> ... }`.
     *
     * @param indexName Index to search.
     *
     * @param query Condition a document must match in order to be included in the search results.
     *
     * @param page Specifies which rows of the search result to return.
     *
     * @param limit Number of rows to return (page size).
     *
     * @param sort Specifies how the results should be sorted.
     * For tiered sort (sort by X then by Y) see [SearchSort.then].
     *
     * @param fields Stored fields to include in the result rows, or `listOf("*")`
     * to include all stored fields.
     *
     * @param facets Specifies the facets to include in the search results.
     * A facet counts the number of documents in the full, unpaginated search results
     * that meet certain criteria. Facet results may be retrieved from either
     * [SearchResult] or [SearchMetadata], whichever is more convenient.
     *
     * @param highlight Specifies whether to include fragments of text
     * with the matching search term highlighted. If highlighting is requested,
     * the result also includes location information for matched terms.
     *
     * @param includeLocations If true, triggers the inclusion of location information
     * for matched terms. If highlighting is requested, locations are always included
     * and this parameter has no effect.
     *
     * @param score The scoring algorithm to use.
     *
     * @param explain If true, [SearchRow.explanation] holds the bytes of a JSON Object
     * describing how the score was calculated.
     *
     * @param collections If not empty, only search within the named collections.
     * Requires an index defined on a non-default scope containing the collections.
     *
     * @param consistency Specifies whether to wait for the index to catch up with the
     * latest versions of certain documents. The default (unbounded) is fast, but means
     * the results might not reflect the latest document mutations.
     *
     * @param serializer Default serializer to use for [SearchRow.fieldsAs].
     * If not specified, defaults to the cluster environment's default serializer.
     *
     * @param raw The keys and values in this map are added to the query specification JSON.
     * This is an "escape hatch" for sending arguments supported by Couchbase Server, but not
     * by this version of the SDK.
     *
     * @sample com.couchbase.client.kotlin.samples.searchQuerySimple
     * @sample com.couchbase.client.kotlin.samples.checkSearchQueryResultForPartialFailure
     * @sample com.couchbase.client.kotlin.samples.searchQueryWithFacets
     */
    public fun searchQuery(
        indexName: String,
        query: SearchQuery,
        common: CommonOptions = CommonOptions.Default,
        page: SearchPage = SearchPage.startAt(offset = 0),
        limit: Int? = null,
        sort: SearchSort = SearchSort.byScore(DESCENDING),
        fields: List<String> = emptyList(),
        facets: List<SearchFacet> = emptyList(),
        highlight: Highlight = Highlight.none(),
        includeLocations: Boolean = false,
        score: Score = Score.default(),
        explain: Boolean = false,
        @SinceCouchbase("7.0") collections: List<String> = emptyList(),
        consistency: SearchScanConsistency = SearchScanConsistency.notBounded(),
        serializer: JsonSerializer? = null,
        raw: Map<String, Any?> = emptyMap(),
    ): Flow<SearchFlowItem> {

        // use interface type so less capable serializers don't freak out
        val rootJson: MutableMap<String, Any?> = HashMap<String, Any?>()

        rootJson["query"] = query.toMap()

        highlight.inject(rootJson)
        score.inject(rootJson)
        sort.inject(rootJson)

        val facetMap = facets.groupBy { it.name }.mapValues { (name, facets) ->
            require(facets.size == 1) {
                "Facet names must be unique within a request, but found found multiple facts named '$name'." +
                        " Specify a unique name when creating the facet using the optional 'name' parameter." +
                        " Facets with the duplicate name: $facets"
            }
            facets.single().toMap()
        }

        with(rootJson) {
            putIfNotNull("size", limit)
            putAll(page.map)
            putIfTrue("explain", explain)
            putIfTrue("includeLocations", includeLocations)
            putIfNotEmpty("fields", fields)
            putIfNotEmpty("facets", facetMap)
            putIfNotEmpty("collections", collections)
        }

        val timeout = with(core.env) { common.actualSearchTimeout() }

        val control = mutableMapOf<String, Any?>("timeout" to timeout.toMillis())
        rootJson["ctl"] = control
        consistency.inject(indexName, control)

        rootJson.putAll(raw)

        val actualSerializer = serializer ?: core.env.jsonSerializer
        val queryBytes = Mapper.encodeAsBytes(rootJson)

        return flow {
            val request = with(core.env) {
                SearchRequest(
                    timeout,
                    core.context(),
                    common.actualRetryStrategy(),
                    core.context().authenticator(),
                    indexName,
                    queryBytes,
                    common.actualSpan(TracingIdentifiers.SPAN_REQUEST_SEARCH),
                )
            }
            request.context().clientContext(common.clientContext)

            try {
                core.send(request)

                val response = request.response().await()

                emitAll(response.rows().asFlow()
                    .map { SearchRow.from(it.data(), actualSerializer) })

                emitAll(response.trailer().asFlow()
                    .map { SearchMetadata(response.header(), it) })

            } finally {
                request.logicallyComplete()
            }
        }
    }

    /**
     * Returns a Flow which may be collected to execute a cluster-level
     * N1QL query and process the results.
     *
     * The returned Flow is cold, meaning the query is not executed unless
     * the Flow is collected. If you collect the flow multiple times,
     * the query is executed each time.
     *
     * The extension function `Flow<QueryFlowItem>.execute()` may be used
     * when the results are known to fit in memory. It simply collects the flow
     * into a [QueryResult].
     *
     * For larger query results, prefer the streaming version which takes a
     * lambda to invoke when each row is received from the server:
     * `Flow<QueryFlowItem>.execute { row -> ... }`.
     *
     * @param statement the N1QL statement to execute.
     *
     * @param common options common to all requests.
     *
     * @param parameters parameters to the N1QL statement.
     *
     * @param preserveExpiry pass true if you want the query engine to preserve
     * existing expiration times for any documents modified by this query.
     * *Requires Couchbase Server 7.1 or later.*
     *
     * @param serializer the serializer to use for converting parameters to JSON,
     * and the default serializer for parsing [QueryRow] content.
     * Defaults to the serializer configured on the cluster environment.
     *
     * @param consistency required if you want to read your own writes.
     * Values other than [QueryScanConsistency.notBounded]
     * tell the server to wait for the indexer to catch up with a certain
     * state of the K/V service before executing the query.
     *
     * @param readonly pass true if the N1QL statement does not modify documents.
     * This allows the client to retry the query if necessary.
     *
     * @param adhoc pass false if this is a commonly used query that should be
     * turned into a prepared statement for faster execution.
     *
     * @param flexIndex pass true to use a full-text index instead of a query index.
     *
     * @param metrics pass true to include metrics in the response (access via
     * [QueryMetadata.metrics]). Relatively inexpensive, and may be enabled
     * in production with minimal impact.
     *
     * @param profile specifies how much profiling information to include in
     * the response (access via [QueryMetadata.profile]). Profiling is
     * relatively expensive, and can impact the performance of the server
     * query engine. Not recommended for use in production, unless you're
     * diagnosing a specific issue.  Note this is an Enterprise Edition feature.
     * On Community Edition the parameter will be accepted, but no profiling
     * information returned.
     *
     * @param maxParallelism Specifies the maximum parallelism for the query.
     *
     * @param scanCap Maximum buffered channel size between the indexer client
     * and the query service for index scans. This parameter controls when to use
     * scan backfill. Use 0 or a negative number to disable. Smaller values
     * reduce GC, while larger values reduce indexer backfill.
     *
     * @param pipelineBatch Controls the number of items execution operators
     * can batch for Fetch from the KV.
     *
     * @param pipelineCap Maximum number of items each execution operator
     * can buffer between various operators.
     *
     * @param clientContextId an arbitrary string that identifies this query
     * for diagnostic purposes.
     *
     * @param raw an "escape hatch" for passing arbitrary query options that
     * aren't otherwise exposed by this method.
     *
     * @sample com.couchbase.client.kotlin.samples.bufferedQuery
     * @sample com.couchbase.client.kotlin.samples.streamingQuery
     * @sample com.couchbase.client.kotlin.samples.singleValueQueryAnonymous
     * @sample com.couchbase.client.kotlin.samples.singleValueQueryNamed
     */
    public fun query(
        statement: String,
        common: CommonOptions = CommonOptions.Default,
        parameters: QueryParameters = QueryParameters.None,
        @SinceCouchbase("7.1") preserveExpiry: Boolean = false,

        serializer: JsonSerializer? = null,

        consistency: QueryScanConsistency = QueryScanConsistency.notBounded(),
        readonly: Boolean = false,
        adhoc: Boolean = true,
        flexIndex: Boolean = false,

        metrics: Boolean = false,
        profile: QueryProfile = QueryProfile.OFF,

        maxParallelism: Int? = null,
        scanCap: Int? = null,
        pipelineBatch: Int? = null,
        pipelineCap: Int? = null,

        clientContextId: String? = UUID.randomUUID().toString(),
        raw: Map<String, Any?> = emptyMap(),

        ): Flow<QueryFlowItem> {

        return queryExecutor.query(
            statement,
            common,
            parameters,
            preserveExpiry,
            serializer,
            consistency,
            readonly,
            adhoc,
            flexIndex,
            metrics,
            profile,
            maxParallelism,
            scanCap,
            pipelineBatch,
            pipelineCap,
            clientContextId,
            raw,
        )
    }

    public fun analyticsQuery(
        statement: String,
        common: CommonOptions = CommonOptions.Default,
        parameters: AnalyticsParameters = AnalyticsParameters.None,

        serializer: JsonSerializer? = null,

        consistency: AnalyticsScanConsistency = AnalyticsScanConsistency.notBounded(),
        @SinceCouchbase("6.5") readonly: Boolean = false,
        priority: AnalyticsPriority = AnalyticsPriority.normal(),

        clientContextId: String? = UUID.randomUUID().toString(),
        raw: Map<String, Any?> = emptyMap(),

        ): Flow<AnalyticsFlowItem> {

        return analyticsExecutor.query(
            statement,
            common,
            parameters,
            serializer,
            consistency,
            readonly,
            priority,
            clientContextId,
            raw
        )
    }

    /**
     * Gives any in-flight requests a chance to complete, then disposes
     * of resources allocated to the cluster.
     *
     * Call this method when your application is ready to shut down
     * or when you are done using the cluster.
     */
    public suspend fun disconnect(
        timeout: Duration = core.context().environment().timeoutConfig().disconnectTimeout().toKotlinDuration(),
    ) {
        core.shutdown(timeout.toJavaDuration()).await()
        if (ownsEnvironment) {
            core.context().environment().shutdownReactive(timeout.toJavaDuration()).await()
        }
    }

    public companion object {
        /**
         * The number of connected Cluster instances that may exist at the same time.
         * Calling [Cluster.connect] after this limit is reached will either fail or log a warning,
         * depending on the value of [failIfInstanceLimitReached].
         */
        @UncommittedCouchbaseApi
        public var maxAllowedInstances : Int
            get() = Core.getMaxAllowedInstances()
            set(value) = Core.maxAllowedInstances(value)

        /**
         * True means exceeding [maxAllowedInstances] is a fatal error, false means just log a warning.
         */
        @UncommittedCouchbaseApi
        public var failIfInstanceLimitReached : Boolean
            get() = Core.getFailIfInstanceLimitReached()
            set(value) = Core.failIfInstanceLimitReached(value)

        /**
         * Connects to a Couchbase cluster, authenticating with username and password.
         */
        public fun connect(
            connectionString: String,
            username: String,
            password: String,
            envConfigBlock: ClusterEnvironmentConfigBlock,
        ): Cluster = connect(connectionString, username, password, ClusterEnvironment.builder(envConfigBlock))

        /**
         * Connects to a Couchbase cluster, authenticating with username and password.
         *
         * Accepts a traditional [ClusterEnvironment.Builder] so you can opt out
         * of using the cluster environment config DSL.
         *
         * @param envBuilder Configuration for the new cluster.
         */
        public fun connect(
            connectionString: String,
            username: String,
            password: String,
            envBuilder: ClusterEnvironment.Builder = ClusterEnvironment.builder(),
        ): Cluster = connect(connectionString, PasswordAuthenticator.create(username, password), envBuilder)

        /**
         * Connects to a Couchbase cluster using an alternate authentication
         * strategy like [CertificateAuthenticator].
         */
        public fun connect(
            connectionString: String,
            authenticator: Authenticator,
            envConfigBlock: ClusterEnvironmentConfigBlock,
        ): Cluster = connect(connectionString, authenticator, ClusterEnvironment.builder(envConfigBlock))

        /**
         * Connects to a Couchbase cluster using an alternate authentication
         * strategy like [CertificateAuthenticator].
         *
         * Accepts a traditional [ClusterEnvironment.Builder] so you can opt out
         * of using the cluster environment config DSL.
         *
         * @param envBuilder Configuration for the new cluster.
         */
        public fun connect(
            connectionString: String,
            authenticator: Authenticator,
            envBuilder: ClusterEnvironment.Builder = ClusterEnvironment.builder(),
        ): Cluster {
            envBuilder.load(ConnectionStringPropertyLoader(connectionString))
            return doConnect(connectionString, authenticator, envBuilder.build(), ownsEnv = true)
        }

        /**
         * Connects to a Couchbase cluster, authenticating with username and password.
         *
         * Use this method if you are connecting to multiple clusters and want to
         * share the same [ClusterEnvironment] for improved performance.
         *
         * IMPORTANT: You are responsible for shutting down the environment
         * after all the clusters sharing it have disconnected.
         *
         * @see ClusterEnvironment.builder
         */
        public fun connectUsingSharedEnvironment(
            connectionString: String,
            username: String,
            password: String,
            env: ClusterEnvironment,
        ): Cluster = connectUsingSharedEnvironment(
            connectionString, PasswordAuthenticator.create(username, password), env
        )

        /**
         * Connects to a Couchbase cluster using an alternate authentication
         * strategy like [CertificateAuthenticator].
         *
         * Use this method if you are connecting to multiple clusters and want to
         * share the same [ClusterEnvironment] for improved performance.
         *
         * IMPORTANT: You are responsible for shutting down the environment
         * after all the clusters sharing it have disconnected.
         *
         * @see ClusterEnvironment.builder
         */
        public fun connectUsingSharedEnvironment(
            connectionString: String,
            authenticator: Authenticator,
            env: ClusterEnvironment,
        ): Cluster {
            ConnectionString.create(connectionString).apply {
                require(scheme() != COUCHBASES || env.securityConfig().tlsEnabled()) {
                    "Connection string scheme indicates a secure connection," +
                            " but the shared cluster environment was not configured for TLS."
                }
                require(params().isEmpty()) {
                    "Can't use a shared cluster environment with a connection string that has parameters."
                }
            }

            return doConnect(connectionString, authenticator, env, ownsEnv = false)
        }

        private fun doConnect(
            connectionString: String,
            authenticator: Authenticator,
            env: ClusterEnvironment,
            ownsEnv: Boolean,
        ): Cluster {
            checkConnectionString(env, ownsEnv, ConnectionString.create(connectionString))

            val seedNodes = ConnectionStringUtil.seedNodesFromConnectionString(
                connectionString,
                env.ioConfig().dnsSrvEnabled(),
                env.securityConfig().tlsEnabled(),
                env.eventBus(),
            )
            return Cluster(env, ownsEnv, authenticator, seedNodes, connectionString)
        }
    }
}
