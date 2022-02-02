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
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.env.CertificateAuthenticator
import com.couchbase.client.core.env.ConnectionStringPropertyLoader
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.env.SeedNode
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionString.Scheme.COUCHBASES
import com.couchbase.client.core.util.ConnectionStringUtil
import com.couchbase.client.kotlin.Cluster.Companion.connect
import com.couchbase.client.kotlin.analytics.AnalyticsFlowItem
import com.couchbase.client.kotlin.analytics.AnalyticsParameters
import com.couchbase.client.kotlin.analytics.AnalyticsPriority
import com.couchbase.client.kotlin.analytics.AnalyticsScanConsistency
import com.couchbase.client.kotlin.analytics.internal.AnalyticsExecutor
import com.couchbase.client.kotlin.annotations.SinceCouchbase
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.env.dsl.ClusterEnvironmentConfigBlock
import com.couchbase.client.kotlin.env.env
import com.couchbase.client.kotlin.internal.await
import com.couchbase.client.kotlin.manager.http.CouchbaseHttpClient
import com.couchbase.client.kotlin.query.QueryFlowItem
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryProfile
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.query.QueryScanConsistency
import com.couchbase.client.kotlin.query.internal.QueryExecutor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import java.util.*
import java.util.concurrent.ConcurrentHashMap
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
) {
    internal val core: Core = Core.create(environment, authenticator, seedNodes)

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
    public val httpClient : CouchbaseHttpClient = CouchbaseHttpClient(this)

    /**
     * Returns a Flow which may be collected to execute a cluster-level
     * N1QL query and process the results.
     *
     * The returned Flow is cold, meaning the query is not executed unless
     * the Flow is collected. If you collect the flow multiple times,
     * the query is executed each time.
     *
     * The extension function `Flow<QueryFlowItem>.execute()` may be used when
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
     * diagnosing a specific issue.
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
    @OptIn(VolatileCouchbaseApi::class)
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
        readonly: Boolean = false,
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
         * Connects to a Couchbase cluster, authenticating with username and password.
         */
        public fun connect(
            connectionString: String,
            username: String,
            password: String,
            envConfigBlock: ClusterEnvironmentConfigBlock
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
            envBuilder: ClusterEnvironment.Builder = ClusterEnvironment.builder()
        ): Cluster = connect(connectionString, PasswordAuthenticator.create(username, password), envBuilder)

        /**
         * Connects to a Couchbase cluster using an alternate authentication
         * strategy like [CertificateAuthenticator].
         */
        public fun connect(
            connectionString: String,
            authenticator: Authenticator,
            envConfigBlock: ClusterEnvironmentConfigBlock
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
            envBuilder: ClusterEnvironment.Builder = ClusterEnvironment.builder()
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
            val seedNodes = ConnectionStringUtil.seedNodesFromConnectionString(
                connectionString,
                env.ioConfig().dnsSrvEnabled(),
                env.securityConfig().tlsEnabled(),
                env.eventBus(),
            )
            return Cluster(env, ownsEnv, authenticator, seedNodes)
        }
    }
}
