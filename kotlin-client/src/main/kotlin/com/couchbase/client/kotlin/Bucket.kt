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
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.config.BucketConfig
import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.error.ViewNotFoundException
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.core.msg.view.ViewRequest
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.BucketConfigUtil
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.couchbase.client.kotlin.samples.bufferedViewQuery
import com.couchbase.client.kotlin.samples.streamingViewQuery
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
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

public class Bucket internal constructor(
    public val name: String,
    public val cluster: Cluster,
    internal val core: Core,
) {
    internal val env = core.context().environment() as ClusterEnvironment

    private val scopeCache = ConcurrentHashMap<String, Scope>()

    /**
     * Returns the default collection in this bucket's default scope.
     */
    public fun defaultCollection(): Collection = defaultScope().defaultCollection()

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
     * @sample bufferedViewQuery
     * @sample streamingViewQuery
     */
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
            val request = ViewRequest(
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
        WaitUntilReadyHelper.waitUntilReady(core, services, timeout, desiredState, name.toOptional()).await()
        return this
    }

    @VolatileCouchbaseApi
    public suspend fun config(timeout: Duration): BucketConfig =
        core.clusterConfig().bucketConfig(name)
            ?: BucketConfigUtil.waitForBucketConfig(core, name, timeout).asFlow().single()


    internal fun CommonOptions.actualViewTimeout(): Duration = timeout ?: env.timeoutConfig().viewTimeout()
    internal fun CommonOptions.actualRetryStrategy() = retryStrategy ?: env.retryStrategy()
    internal fun CommonOptions.actualSpan(name: String) = env.requestTracer().requestSpan(name, parentSpan)
}
