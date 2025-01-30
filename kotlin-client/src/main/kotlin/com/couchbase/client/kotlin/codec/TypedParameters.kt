/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.codec

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.json.Mapper

@PublishedApi
internal data class ValueAndType<T>(val value: T, val type: TypeRef<T>) {
    companion object {
        private val any = typeRef<Any?>()
        internal fun untyped(value: Any?) = ValueAndType(value, any)

        internal fun <T> JsonSerializer.serializeAsJsonNode(valueAndType: ValueAndType<T>) =
            Mapper.decodeIntoTree(serialize(valueAndType.value, valueAndType.type))

        internal fun JsonSerializer.serializeAsArrayNode(values: List<ValueAndType<*>>): ArrayNode {
            val node = Mapper.createArrayNode()
            values.forEach { node.add(serializeAsJsonNode(it)) }
            return node
        }

        internal fun JsonSerializer.serializeAsObjectNode(nameToValue: Map<String, ValueAndType<*>>): ObjectNode {
            val node = Mapper.createObjectNode()
            nameToValue.forEach { node.replace(it.key, serializeAsJsonNode(it.value)) }
            return node
        }
    }
}

public class PositionalParametersBuilder internal constructor() {
    private val list: MutableList<ValueAndType<*>> = mutableListOf()

    /**
     * @param T Any type your [JsonSerializer] can serialize.
     */
    public inline fun <reified T> param(value: T): Unit = typedParam(ValueAndType(value, typeRef<T>()))

    @PublishedApi
    internal fun <T> typedParam(value: ValueAndType<T>) {
        list.add(value)
    }

    internal fun build(): List<ValueAndType<*>> = list.toList()
}

public class NamedParametersBuilder internal constructor() {
    private val map: MutableMap<String, ValueAndType<*>> = mutableMapOf()

    /**
     * @param T Any type your [JsonSerializer] can serialize.
     */
    public inline fun <reified T> param(name: String, value: T): Unit = typedParam(name, ValueAndType(value, typeRef<T>()))

    @PublishedApi
    internal fun <T> typedParam(name: String, value: ValueAndType<T>) {
        map[name] = value
    }

    internal fun build(): Map<String, ValueAndType<*>> = map.toMap()
}
