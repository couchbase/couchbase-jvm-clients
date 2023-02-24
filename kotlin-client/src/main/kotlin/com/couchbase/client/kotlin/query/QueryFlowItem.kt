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

import com.couchbase.client.core.api.query.CoreQueryMetaData
import com.couchbase.client.core.classic.query.ClassicCoreQueryMetaData
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.query.QueryChunkHeader
import com.couchbase.client.core.msg.query.QueryChunkTrailer
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.internal.toStringUtf8
import java.util.Optional

public sealed class QueryFlowItem

/**
 * One row of a query result.
 */
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

/**
 * Metadata about query execution. Always the last item in the flow.
 */
public class QueryMetadata internal constructor(
    private val core: CoreQueryMetaData,
) : QueryFlowItem() {

    // Oops, this constructor is part of the public API :-(
    @Deprecated(message = "QueryMetadata constructor will be private in a future version.")
    public constructor(
        header: QueryChunkHeader,
        trailer: QueryChunkTrailer,
    ) : this(ClassicCoreQueryMetaData(header, trailer))

    public val requestId: String
        get() = core.requestId()

    public val clientContextId: String
        get() = core.clientContextId()

    public val status: QueryStatus
        get() = QueryStatus.from(core.status().name)

    public val signature: Map<String, Any?>?
        get() = core.signature().parseAsMap().orElse(null)

    public val profile: Map<String, Any?>?
        get() = core.profile().parseAsMap().orElse(null)

    public val metrics: QueryMetrics?
        get() = core.metrics().map { QueryMetrics(it) }.orElse(null)

    public val warnings: List<QueryWarning>
        get() = core.warnings().map { QueryWarning(it) }

    override fun toString(): String {
        return "QueryMetadata(requestId='$requestId', clientContextId='$clientContextId', status=$status, signature=$signature, profile=$profile, metrics=$metrics, warnings=$warnings)"
    }
}

private val MAP_TYPE_REF = object : TypeReference<Map<String, Any?>>() {}

private fun Optional<ByteArray>.parseAsMap(): Optional<Map<String, Any?>> {
    return map { Mapper.decodeInto(it, MAP_TYPE_REF) }
}
