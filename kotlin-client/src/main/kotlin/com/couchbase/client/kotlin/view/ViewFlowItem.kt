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

package com.couchbase.client.kotlin.view

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.view.ViewChunkHeader
import com.couchbase.client.core.msg.view.ViewChunkTrailer
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import kotlin.LazyThreadSafetyMode.PUBLICATION

public sealed class ViewFlowItem

/**
 * One row of a View result.
 */
public class ViewRow internal constructor(
    content: ByteArray,
    @property:PublishedApi internal val defaultSerializer: JsonSerializer,
) : ViewFlowItem() {

    private val rootNode = Mapper.decodeIntoTree(content) as ObjectNode

    /**
     * Raw key bytes.
     */
    public val key: ByteArray = Mapper.encodeAsBytes(rootNode.get("key"))

    /**
     * Raw value bytes.
     */
    public val value: ByteArray by lazy(PUBLICATION) { Mapper.encodeAsBytes(rootNode.get("value")) }

    /**
     * Document ID associated with this row, or null if reduction was used.
     */
    public val id: String? by lazy(PUBLICATION) { rootNode.path("id").textValue() }

    public inline fun <reified T> keyAs(serializer: JsonSerializer? = null): T =
        (serializer ?: defaultSerializer).deserialize(key, typeRef())

    public inline fun <reified T> valueAs(serializer: JsonSerializer? = null): T =
        (serializer ?: defaultSerializer).deserialize(value, typeRef())

    override fun toString(): String {
        return "ViewRow(content=$rootNode)"
    }
}

/**
 * Metadata about View execution. Always the last item in the flow.
 */
public class ViewMetadata internal constructor(
    internal val header: ViewChunkHeader,
    internal val trailer: ViewChunkTrailer,
) : ViewFlowItem() {

    public val totalRows: Long = header.totalRows()

    public val debug: Map<String, Any?>? by lazy {
        header.debug().map { Mapper.decodeInto(it, MAP_TYPE_REF) }.orElse(null)
    }

    override fun toString(): String {
        return "ViewMetadata(totalRows=$totalRows, error=${trailer.error().orElse(null)} debug=${debug})"
    }
}

private val MAP_TYPE_REF = object : TypeReference<Map<String, Any?>>() {}
