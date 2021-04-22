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

package com.couchbase.client.kotlin.analytics

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference
import com.couchbase.client.core.error.ErrorCodeAndMessage
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.internal.toStringUtf8
import java.util.*

public sealed class AnalyticsFlowItem

/**
 * One row of a query result.
 */
public class AnalyticsRow(
    public val content: ByteArray,
    @property:PublishedApi internal val defaultSerializer: JsonSerializer,
) : AnalyticsFlowItem() {

    public inline fun <reified T> contentAs(serializer: JsonSerializer? = null): T {
        return (serializer ?: defaultSerializer).deserialize(content, typeRef())
    }

    override fun toString(): String {
        return "AnalyticsRow(content=${content.toStringUtf8()})"
    }
}

/**
 * Metadata about analytics execution. Always the last item in the flow.
 */
public class AnalyticsMetadata(
    private val header: AnalyticsChunkHeader,
    private val trailer: AnalyticsChunkTrailer,
) : AnalyticsFlowItem() {

    public val requestId: String
        get() = header.requestId()

    public val clientContextId: String
        get() = header.clientContextId().orElse("")

    public val status: AnalyticsStatus
        get() = AnalyticsStatus.from(trailer.status())

    public val signature: Map<String, Any?>?
        get() = header.signature().parseAsMap().orElse(null)

    public val metrics: AnalyticsMetrics
        get() = AnalyticsMetrics(Mapper.decodeInto(trailer.metrics(), MAP_TYPE_REF))

    public val warnings: List<AnalyticsWarning>
        get() = trailer.warnings().map { warnings ->
            ErrorCodeAndMessage.fromJsonArray(warnings)
                .map { AnalyticsWarning(it.code(), it.message()) }
        }.orElse(emptyList())

    override fun toString(): String {
        return "AnalyticsMetadata(requestId='$requestId', clientContextId='$clientContextId', status=$status, signature=$signature, metrics=$metrics, warnings=$warnings)"
    }
}


private val MAP_TYPE_REF = object : TypeReference<Map<String, Any?>>() {}

private fun Optional<ByteArray>.parseAsMap(): Optional<Map<String, Any?>> {
    return map { Mapper.decodeInto(it, MAP_TYPE_REF) }
}
