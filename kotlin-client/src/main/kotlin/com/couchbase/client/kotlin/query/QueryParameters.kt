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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.NamedParametersBuilder
import com.couchbase.client.kotlin.codec.PositionalParametersBuilder
import com.couchbase.client.kotlin.codec.ValueAndType
import com.couchbase.client.kotlin.codec.ValueAndType.Companion.serializeAsArrayNode
import com.couchbase.client.kotlin.codec.ValueAndType.Companion.serializeAsJsonNode
import com.couchbase.client.kotlin.codec.ValueAndType.Companion.serializeAsObjectNode
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.query.QueryParameters.Companion.named
import com.couchbase.client.kotlin.query.QueryParameters.Companion.positional

/**
 * Create instances using the [positional] or [named] factory methods.
 */
public sealed class QueryParameters {
    internal open fun serializeIfNamed(serializer: JsonSerializer): ObjectNode? = null
    internal open fun serializeIfPositional(serializer: JsonSerializer): ArrayNode? = null

    public object None : QueryParameters()

    private class Positional(
        private val values: List<ValueAndType<*>>,
    ) : QueryParameters() {
        override fun serializeIfPositional(serializer: JsonSerializer): ArrayNode = serializer.serializeAsArrayNode(values)
    }

    private class Named(
        private val nameToValue: Map<String, ValueAndType<*>>,
    ) : QueryParameters() {
        override fun serializeIfNamed(serializer: JsonSerializer): ObjectNode = serializer.serializeAsObjectNode(nameToValue)
    }

    private class NamedFromParameterBlock(
        private val value: ValueAndType<*>,
    ) : QueryParameters() {
        override fun serializeIfNamed(serializer: JsonSerializer): ObjectNode = serializer.serializeAsJsonNode(value) as ObjectNode
    }

    public companion object {
        /**
         * Values to plug into positional placeholders in the query statement.
         * ```
         * parameters = QueryParameters.positional {
         *     param("airline") // replacement for first ?
         *     param(3)         // replacement for second ?
         * }
         * ```
         * @sample com.couchbase.client.kotlin.samples.queryWithPositionalParameters
         */
        public fun positional(paramSetterBlock: PositionalParametersBuilder.() -> Unit): QueryParameters {
            val builder = PositionalParametersBuilder()
            builder.apply(paramSetterBlock)
            return Positional(builder.build())
        }

        /**
         * Values to plug into named placeholders in the query statement.
         * ```
         * parameters = QueryParameters.named {
         *     param("type", "airline")
         *     param("limit", 3)
         * }
         * ```
         * @sample com.couchbase.client.kotlin.samples.queryWithNamedParameters
         */
        public fun named(paramSetterBlock: NamedParametersBuilder.() -> Unit): QueryParameters {
            val builder = NamedParametersBuilder()
            builder.apply(paramSetterBlock)
            return Named(builder.build())
        }

        /**
         * Sets query parameters by using the query's JSON serializer to serialize the
         * given object. The resulting JSON Object is used as the named parameter map.
         *
         * For example, if you have a data class like this:
         * ```
         *     // Annotate as @Serializable if using kotlinx.serialization
         *     data class MyParams(val name: String, val number: Int)
         * ```
         * then
         * ```
         *     parameters = QueryParameters.namedFrom(MyParams("Fido", 3))
         * ```
         * is equivalent to
         * ```
         *     parameters = QueryParameters.named {
         *         param("name", "Fido")
         *         param("number", 3)
         *     }
         * ```
         * @param parameterBlock The object to serialize to get named parameters.
         *
         * @sample com.couchbase.client.kotlin.samples.queryWithNamedParameterBlock
         */
        public inline fun <reified T> namedFrom(parameterBlock: T): QueryParameters {
            return typedNamedFrom(ValueAndType(parameterBlock, typeRef<T>()))
        }

        @PublishedApi
        internal fun <T> typedNamedFrom(typedValue: ValueAndType<T>): QueryParameters {
            return NamedFromParameterBlock(typedValue)
        }

        @Deprecated(
            level = DeprecationLevel.WARNING,
            message = "Not compatible with JsonSerializer implementations that require type information, like kotlinx.serialization." +
                    " Please use the overload that takes a parameter builder lambda."
        )
        public fun named(values: Map<String, Any?>): QueryParameters = Named(values.mapValues { entry -> ValueAndType.untyped(entry.value) })

        @Deprecated(
            level = DeprecationLevel.WARNING,
            message = "Not compatible with JsonSerializer implementations that require type information, like kotlinx.serialization." +
                    " Please use the overload that takes a parameter builder lambda."
        )
        @Suppress("DeprecatedCallableAddReplaceWith")
        public fun named(vararg values: Pair<String, Any?>): QueryParameters = named(values.toMap())

        @Deprecated(
            level = DeprecationLevel.WARNING,
            message = "Not compatible with JsonSerializer implementations that require type information, like kotlinx.serialization." +
                    " Please use the overload that takes a parameter builder lambda."
        )
        @Suppress("DeprecatedCallableAddReplaceWith")
        public fun positional(values: List<Any?>): QueryParameters = Positional(values.map { ValueAndType.untyped(it) })
    }
}
