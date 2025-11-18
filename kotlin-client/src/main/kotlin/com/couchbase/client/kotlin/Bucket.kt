/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin

import com.couchbase.client.core.Core
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.config.BucketConfig
import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.core.diagnostics.HealthPinger
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.error.ViewNotFoundException
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.core.msg.view.ViewRequest
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.BucketConfigUtil
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.kotlin.annotations.UncommittedCouchbaseApi
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.diagnostics.PingResult
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.couchbase.client.kotlin.manager.collection.CollectionManager
import com.couchbase.client.kotlin.manager.view.ViewIndexManager
import com.couchbase.client.kotlin.view.DesignDocumentNamespace
import com.couchbase.client.kotlin.view.DesignDocumentNamespace.DEVELOPMENT
import com.couchbase.client.kotlin.view.DesignDocumentNamespace.PRODUCTION
import com.couchbase.client.kotlin.view.ViewErrorMode
import com.couchbase.client.kotlin.view.ViewFlowItem
import com.couchbase.client.kotlin.view.ViewGroupLevel
import com.couchbase.client.kotlin.view.ViewMetadata
import com.couchbase.client.kotlin.view.ViewRow
import com.couchbase.client.kotlin.view.ViewScanConsistency
import com.couchbase.client.kotlin.view.ViewSelection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import java.util.Optional
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.DeprecationLevel.WARNING
import kotlin.time.Duration
import kotlin.time.toJavaDuration

public class Bucket internal constructor(
    public val name: String,
    public val cluster: Cluster,
    internal val couchbaseOps: CoreCouchbaseOps,
) {
    internal val env = cluster.env

    internal val core: Core
        get() = couchbaseOps.asCore()

    private val scopeCache = ConcurrentHashMap<String, Scope>()

    @Suppress("DeprecatedCallableAddReplaceWith")
    @Deprecated(level = WARNING, message = viewsDeprecationMessage)
    public val viewIndexes: ViewIndexManager
        get() = ViewIndexManager(this)

    /**
     * A manager for administering Scopes and Collections within this bucket.
     */
    @UncommittedCouchbaseApi
    public val collections: CollectionManager = CollectionManager(this)

    /**
     * Returns the default collection in this bucket's default scope.
     */
    public fun defaultCollection(): Collection = collection(DEFAULT_COLLECTION)

    /**
     * Returns the requested collection in the bucket's default scope.
     */
    public fun collection(name: String): Collection = defaultScope().collection(name)

    /**
     * Returns this bucket's default scope.
     */
    public fun defaultScope(): Scope = scope(DEFAULT_SCOPE)

    /**
     * Returns the requested scope.
     */
    public fun scope(name: String): Scope =
        scopeCache.computeIfAbsent(name) { Scope(name, this) }

    /**
     * Queries a view on the bucket.
     *
     * @throws ViewNotFoundException if the view or design document is not found on the server.
     *
     * @sample com.couchbase.client.kotlin.samples.bufferedViewQuery
     * @sample com.couchbase.client.kotlin.samples.streamingViewQuery
     */
    @Deprecated(level = WARNING, message = viewsDeprecationMessage)
    public fun viewQuery(
        designDocument: String,
        viewName: String,
        common: CommonOptions = CommonOptions.Default,
        serializer: JsonSerializer? = null,
        namespace: DesignDocumentNamespace = PRODUCTION,
        scanConsistency: ViewScanConsistency = ViewScanConsistency.updateAfter(),
        selection: ViewSelection = ViewSelection.range(),
        skip: Int = 0,
        limit: Int? = null,
        reduce: Boolean = true, // set to false to disable reduce function. If view has no reduce function, has no effect.
        group: ViewGroupLevel = ViewGroupLevel.none(),
        onError: ViewErrorMode = ViewErrorMode.CONTINUE,
        debug: Boolean = false,
        raw: Map<String, String>? = null,
    ): Flow<ViewFlowItem> {
        val actualSerializer = serializer ?: env.jsonSerializer

        val params = UrlQueryStringBuilder.createForUrlSafeNames()

        scanConsistency.inject(params)
        selection.inject(params)

        val keys = selection.keys()
        val postBody: ByteArray? = when {
            keys.isEmpty() -> null
            keys.size == 1 -> {
                params.set("key", actualSerializer.serialize(keys.first(), typeRef()).toStringUtf8())
                null
            }
            else -> actualSerializer.serialize(mapOf("keys" to keys), typeRef())
        }

        if (!reduce) params.set("reduce", reduce)
        group.inject(params)

        if (onError != ViewErrorMode.CONTINUE) params.set("on_error", onError.encoded)
        if (debug) params.set("debug", debug)

        fun addNonNegative(value: Int?, name: String) = value?.let {
            require(it >= 0) { "'$name' must be non-negative but got $it" }
            params.add(name, it)
        }
        if (skip != 0) addNonNegative(skip, "skip")
        addNonNegative(limit, "limit")

        raw?.let { it.forEach { (key, value) -> params.set(key, value) } }

        return flow {
            val request = with(env) {
                ViewRequest(
                    common.actualViewTimeout(),
                    core.context(),
                    common.actualRetryStrategy(),
                    core.context().authenticator(),
                    this@Bucket.name,
                    designDocument,
                    viewName,
                    params.build(),
                    postBody.toOptional(),
                    namespace == DEVELOPMENT,
                    common.actualSpan(TracingIdentifiers.SPAN_REQUEST_VIEWS)
                )
            }
            request.context().clientContext(common.clientContext)

            try {
                core.send(request)

                val response = request.response().await()

                emitAll(response.rows().asFlow()
                    .map { ViewRow(it.data(), actualSerializer) })

                emitAll(response.trailer().asFlow()
                    .map { ViewMetadata(response.header(), it) })

            } finally {
                request.logicallyComplete()
            }
        }
    }

    /**
     * Waits until SDK bootstrap is complete and the desired [ClusterState]
     * is observed.
     *
     * Calling this method is optional. Without it, operations performed
     * before the bucket is ready may take longer than usual, since
     * the SDK needs to load the bucket config from the server first.
     *
     * @param timeout the maximum time to wait for readiness.
     * @param services the service types to wait for, or empty set to wait for all services.
     * @throws UnambiguousTimeoutException if not ready before timeout
     */
    public suspend fun waitUntilReady(
        timeout: Duration,
        services: Set<ServiceType> = emptySet(),
        desiredState: ClusterState = ClusterState.ONLINE,
    ): Bucket {
        couchbaseOps.waitUntilReady(services, timeout.toJavaDuration(), desiredState, name)
            .await()
        return this
    }

    /**
     * Pings services in the Couchbase cluster.
     * <p>
     * This operation performs I/O against services and endpoints to assess their health.
     * If you do not wish to perform I/O, consider using [Cluster.diagnostics] instead.
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
            Optional.of(name),
        ).awaitSingle()
    )

    @VolatileCouchbaseApi
    public suspend fun config(timeout: Duration): BucketConfig =
        core.clusterConfig().bucketConfig(name)
            ?: BucketConfigUtil.waitForBucketConfig(core, name, timeout.toJavaDuration()).asFlow().single()
}

private const val viewsDeprecationMessage = "Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version." +
        " Views are not compatible with the Magma storage engine." +
        " Instead of views, use indexes and queries using the Index Service (GSI) and the Query Service (SQL++)."
