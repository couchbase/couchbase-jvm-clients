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

import com.couchbase.client.core.api.query.CoreQueryContext
import com.couchbase.client.core.api.query.CoreQueryOps
import com.couchbase.client.core.api.query.CoreQueryOptions
import com.couchbase.client.core.api.query.CoreQueryProfile
import com.couchbase.client.core.api.query.CoreQueryScanConsistency
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
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
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
import kotlin.time.toJavaDuration

internal class QueryExecutor(
    private val queryOps: CoreQueryOps,
    private val queryContext: CoreQueryContext?,
    private val defaultSerializer: JsonSerializer,
) {
    fun query(
        statement: String,
        common: CommonOptions,
        parameters: QueryParameters,
        preserveExpiry: Boolean,

        serializer: JsonSerializer?,

        consistency: QueryScanConsistency,
        readonly: Boolean,
        adhoc: Boolean,
        flexIndex: Boolean,

        metrics: Boolean,
        profile: QueryProfile,

        maxParallelism: Int?,
        scanCap: Int?,
        pipelineBatch: Int?,
        pipelineCap: Int?,

        clientContextId: String?,
        raw: Map<String, Any?>,
        useReplica: Boolean?,
    ): Flow<QueryFlowItem> {
        val actualSerializer = serializer ?: defaultSerializer

        val coreQueryOpts = object : CoreQueryOptions {
            override fun adhoc(): Boolean = adhoc
            override fun clientContextId(): String? = clientContextId
            override fun consistentWith(): CoreMutationState? = (consistency as? QueryScanConsistency.ConsistentWith)?.toCore()
            override fun maxParallelism(): Int? = maxParallelism
            override fun metrics(): Boolean = metrics

            override fun namedParameters(): ObjectNode? = (parameters as? QueryParameters.Named)?.let { params ->
                // Let the user's serializer serialize arguments
                val map = mutableMapOf<String, Any?>()
                params.inject(map)
                val queryBytes = actualSerializer.serialize(map, typeRef())
                Mapper.decodeIntoTree(queryBytes) as ObjectNode
            }

            override fun pipelineBatch(): Int? = pipelineBatch
            override fun pipelineCap(): Int? = pipelineCap

            override fun positionalParameters(): ArrayNode? = (parameters as? QueryParameters.Positional)?.let { params ->
                // Let the user's serializer serialize arguments
                val map = mutableMapOf<String, Any?>()
                params.inject(map)
                val queryBytes = actualSerializer.serialize(map, typeRef())
                Mapper.decodeIntoTree(queryBytes).get("args") as? ArrayNode
            }

            override fun profile(): CoreQueryProfile = profile.core

            override fun raw(): JsonNode? {
                if (raw.isEmpty()) return null
                val rawBytes = actualSerializer.serialize(raw, typeRef())
                return Mapper.decodeIntoTree(rawBytes)
            }

            override fun readonly(): Boolean = readonly
            override fun scanWait(): java.time.Duration? = consistency.scanWait?.toJavaDuration()
            override fun scanCap(): Int? = scanCap

            override fun scanConsistency(): CoreQueryScanConsistency? = when (consistency) {
                is QueryScanConsistency.NotBounded -> CoreQueryScanConsistency.NOT_BOUNDED
                is QueryScanConsistency.RequestPlus -> CoreQueryScanConsistency.REQUEST_PLUS
                else -> null // ConsistentWith is handled by separate consistentWith() accessor
            }

            override fun flexIndex(): Boolean = flexIndex
            override fun preserveExpiry(): Boolean? = if (preserveExpiry) true else null
            override fun asTransactionOptions(): CoreSingleQueryTransactionOptions? = null
            override fun commonOptions(): CoreCommonOptions = common.toCore()
            override fun useReplica(): Boolean? = useReplica
        }

        return flow {
            val response = queryOps.queryReactive(
                statement,
                coreQueryOpts,
                queryContext,
                null,
                null,
            ).awaitSingle()

            emitAll(
                response.rows().asFlow().map {
                    QueryRow(it.data(), actualSerializer)
                }
            )

            emit(QueryMetadata(response.metaData().awaitSingle()))
        }
    }
}
