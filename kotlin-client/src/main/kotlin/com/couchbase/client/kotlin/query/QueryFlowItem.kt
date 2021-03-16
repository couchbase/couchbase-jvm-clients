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

package com.couchbase.client.kotlin.query

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference
import com.couchbase.client.core.error.ErrorCodeAndMessage
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.query.QueryChunkHeader
import com.couchbase.client.core.msg.query.QueryChunkTrailer
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.internal.toStringUtf8
import java.util.*

public sealed class QueryFlowItem

public class QueryRow(
    public val content: ByteArray,
    @property:PublishedApi internal val defaultSerializer: JsonSerializer,
) : QueryFlowItem() {

    public inline fun <reified T> contentAs(serializer: JsonSerializer? = null): T {
        return (serializer ?: defaultSerializer).deserialize(content, typeRef())
    }

    override fun toString(): String {
        return "QueryRow(content=${content.toStringUtf8()})"
    }
}

public class QueryMetadata(
    private val header: QueryChunkHeader,
    private val trailer: QueryChunkTrailer,
) : QueryFlowItem() {

    public val requestId: String
        get() = header.requestId()

    public val clientContextId: String
        get() = header.clientContextId().orElse("")

    public val status: QueryStatus
        get() = QueryStatus.from(trailer.status())

    public val signature: Map<String, Any?>?
        get() = header.signature().parseAsMap().orElse(null)

    public val profile: Map<String, Any?>?
        get() = trailer.profile().parseAsMap().orElse(null)

    public val metrics: QueryMetrics?
        get() = trailer.metrics().parseAsMap().map { QueryMetrics(it) }.orElse(null)

    public val warnings: List<QueryWarning>
        get() = trailer.warnings().map { warnings ->
            ErrorCodeAndMessage.fromJsonArray(warnings)
                .map { QueryWarning(it.code(), it.message()) }
        }.orElse(emptyList())

    override fun toString(): String {
        return "QueryMetadata(requestId='$requestId', clientContextId='$clientContextId', status=$status, signature=$signature, profile=$profile, metrics=$metrics, warnings=$warnings)"
    }
}

private val MAP_TYPE_REF = object : TypeReference<Map<String, Any?>>() {}

private fun Optional<ByteArray>.parseAsMap(): Optional<Map<String, Any?>> {
    return map { Mapper.decodeInto(it, MAP_TYPE_REF) }
}
