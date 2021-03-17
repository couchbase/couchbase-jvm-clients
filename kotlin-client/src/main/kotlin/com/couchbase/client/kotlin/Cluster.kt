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
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.env.CertificateAuthenticator
import com.couchbase.client.core.env.ConnectionStringPropertyLoader
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.env.SeedNode
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionString.Scheme.COUCHBASES
import com.couchbase.client.core.util.ConnectionStringUtil
import com.couchbase.client.core.util.Golang
import com.couchbase.client.kotlin.Cluster.Companion.connect
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.env.dsl.ClusterEnvironmentConfigBlock
import com.couchbase.client.kotlin.env.env
import com.couchbase.client.kotlin.internal.await
import com.couchbase.client.kotlin.query.QueryDiagnostics
import com.couchbase.client.kotlin.query.QueryFlowItem
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.query.QueryScanConsistency
import com.couchbase.client.kotlin.query.QueryTuning
import com.couchbase.client.kotlin.samples.bufferedQuery
import com.couchbase.client.kotlin.samples.singleValueQueryAnonymous
import com.couchbase.client.kotlin.samples.singleValueQueryNamed
import com.couchbase.client.kotlin.samples.streamingQuery
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap

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
 * @sample quickstart
 * @sample configureTlsUsingDsl
 * @sample configureTlsUsingBuilder
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
        WaitUntilReadyHelper.waitUntilReady(core, services, timeout, desiredState, Optional.empty()).await()
        return this
    }

    /**
     * Opens the [Bucket] with the given name.
     *
     * @see Bucket.waitUntilReady
     */
    public fun bucket(name: String): Bucket = bucketCache.computeIfAbsent(name) { key ->
        core.openBucket(key)
        Bucket(key, core)
    }

    /**
     * Returns a Flow which may be collected to execute the query and process
     * the results.
     *
     * The returned Flow is cold, meaning the query is not executed unless
     * the Flow is collected. If you collect the flow multiple times,
     * the query is executed each time.
     *
     * The extension function `Flow<QueryFlowItem>.execute()` may be used when
     * when the results are known to fit in memory. It simply collects the flow
     * into a [QueryResult].
     *
     * For larger query results, prefer the streaming version:
     * `Flow<QueryFlowItem>.execute { row -> ... }`.
     *
     * @param serializer the serializer to use for converting parameters to JSON,
     * and the default serializer for parsing [QueryRow] content.
     * Defaults to the serializer configured on the cluster environment.
     *
     * @sample bufferedQuery
     * @sample streamingQuery
     * @sample singleValueQueryAnonymous
     * @sample singleValueQueryNamed
     */
    public fun query(
        statement: String,
        common: CommonOptions = CommonOptions.Default,
        parameters: QueryParameters = QueryParameters.None,
        readonly: Boolean = false,
        adhoc: Boolean = true,

        serializer: JsonSerializer? = null,
        raw: Map<String, Any?> = emptyMap(),

        consistency: QueryScanConsistency = QueryScanConsistency.NotBounded,
        diagnostics: QueryDiagnostics = QueryDiagnostics.Default,
        tuning: QueryTuning = QueryTuning.Default,

        clientContextId: String? = UUID.randomUUID().toString(),

        ): Flow<QueryFlowItem> {

        if (!adhoc) TODO("adhoc=false not yet implemented")

        val timeout = common.actualQueryTimeout()

        // use interface type so less capable serializers don't freak out
        val queryJson: MutableMap<String, Any?> = HashMap<String, Any?>()

        queryJson["statement"] = statement
        queryJson["timeout"] = Golang.encodeDurationToMs(timeout)
        clientContextId?.let { queryJson["client_context_id"] = it }
        if (readonly) {
            queryJson["readonly"] = true
        }

        consistency.inject(queryJson)
        diagnostics.inject(queryJson)
        tuning.inject(queryJson)
        parameters.inject(queryJson)

        queryJson.putAll(raw)

        val actualSerializer = serializer ?: env.jsonSerializer
        val queryBytes = actualSerializer.serialize(queryJson, typeRef())

        return flow {
            val bucketName: String? = null
            val scopeName: String? = null

            val request = QueryRequest(
                timeout,
                core.context(),
                common.actualRetryStrategy(),
                authenticator,
                statement,
                queryBytes,
                readonly,
                clientContextId,
                common.actualSpan(TracingIdentifiers.SPAN_REQUEST_QUERY),
                bucketName,
                scopeName,
            )

            request.context().clientContext(common.clientContext)

            try {
                core.send(request)

                val response = request.response().await()

                emitAll(response.rows().asFlow()
                    .map { QueryRow(it.data(), actualSerializer) })

                emitAll(response.trailer().asFlow()
                    .map { QueryMetadata(response.header(), it) })

            } finally {
                request.endSpan()
            }
        }
    }

    private fun CommonOptions.actualQueryTimeout() = timeout ?: env.timeoutConfig().queryTimeout()
    private fun CommonOptions.actualRetryStrategy() = retryStrategy ?: env.retryStrategy()
    private fun CommonOptions.actualSpan(name: String) = env.requestTracer().requestSpan(name, parentSpan)

    /**
     * Gives any in-flight requests a chance to complete, then disposes
     * of resources allocated to the cluster.
     *
     * Call this method when your application is ready to shut down
     * or when you are done using the cluster.
     */
    public suspend fun disconnect(
        timeout: Duration = core.context().environment().timeoutConfig().disconnectTimeout()
    ) {
        core.shutdown(timeout).await()
        if (ownsEnvironment) {
            core.context().environment().shutdownReactive(timeout).await()
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

