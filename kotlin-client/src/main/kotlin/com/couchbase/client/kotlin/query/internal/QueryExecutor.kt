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

package com.couchbase.client.kotlin.query.internal

import com.couchbase.client.core.Core
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.msg.query.QueryRequest
import com.couchbase.client.core.msg.query.QueryRequest.queryContext
import com.couchbase.client.core.util.Golang
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.Scope
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.env
import com.couchbase.client.kotlin.logicallyComplete
import com.couchbase.client.kotlin.query.QueryFlowItem
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryProfile
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.query.QueryScanConsistency
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow

internal class QueryExecutor(
    private val core: Core,
    private val scope: Scope? = null,
) {
    private val bucketName = scope?.bucket?.name
    private val scopeName = scope?.name
    private val queryContext = scope?.let { queryContext(scope.bucket.name, scope.name) }

    @OptIn(VolatileCouchbaseApi::class)
    fun query(
        statement: String,
        common: CommonOptions,
        parameters: QueryParameters,

        serializer: JsonSerializer?,

        consistency: QueryScanConsistency,
        readonly: Boolean,
        adhoc: Boolean,
        flexIndex: Boolean,

        metrics: Boolean,
        @VolatileCouchbaseApi profile: QueryProfile,

        maxParallelism: Int?,
        scanCap: Int?,
        pipelineBatch: Int?,
        pipelineCap: Int?,

        clientContextId: String?,
        raw: Map<String, Any?>,
    ): Flow<QueryFlowItem> {

        if (!adhoc) TODO("adhoc=false not yet implemented")

        val timeout = with(core.env) { common.actualQueryTimeout() }

        // use interface type so less capable serializers don't freak out
        val queryJson: MutableMap<String, Any?> = HashMap<String, Any?>()

        queryJson["statement"] = statement
        queryJson["timeout"] = Golang.encodeDurationToMs(timeout)

        if (readonly) queryJson["readonly"] = true
        if (flexIndex) queryJson["use_fts"] = true
        if (!metrics) queryJson["metrics"] = false

        if (profile !== QueryProfile.OFF) queryJson["profile"] = profile.toString()

        clientContextId?.let { queryJson["client_context_id"] = it }
        maxParallelism?.let { queryJson["max_parallelism"] = it.toString() }
        pipelineCap?.let { queryJson["pipeline_cap"] = it.toString() }
        pipelineBatch?.let { queryJson["pipeline_batch"] = it.toString() }
        scanCap?.let { queryJson["scan_cap"] = it.toString() }

        consistency.inject(queryJson)
        parameters.inject(queryJson)

        queryContext?.let { queryJson["query_context"] = it }

        queryJson.putAll(raw)

        val actualSerializer = serializer ?: core.env.jsonSerializer
        val queryBytes = actualSerializer.serialize(queryJson, typeRef())

        return flow {
            val request = with(core.env) {
                QueryRequest(
                    timeout,
                    core.context(),
                    common.actualRetryStrategy(),
                    core.context().authenticator(),
                    statement,
                    queryBytes,
                    readonly,
                    clientContextId,
                    common.actualSpan(TracingIdentifiers.SPAN_REQUEST_QUERY),
                    bucketName,
                    scopeName,
                    null,
                )
            }
            request.context().clientContext(common.clientContext)

            try {
                core.send(request)

                val response = request.response().await()

                emitAll(response.rows().asFlow()
                    .map { QueryRow(it.data(), actualSerializer) })

                emitAll(response.trailer().asFlow()
                    .map { QueryMetadata(response.header(), it) })

            } finally {
                request.logicallyComplete()
            }
        }
    }
}
