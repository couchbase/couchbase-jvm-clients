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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.kotlin.analytics.AnalyticsParameters.Companion.named
import com.couchbase.client.kotlin.analytics.AnalyticsParameters.Companion.positional
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.NamedParametersBuilder
import com.couchbase.client.kotlin.codec.PositionalParametersBuilder
import com.couchbase.client.kotlin.codec.ValueAndType
import com.couchbase.client.kotlin.codec.ValueAndType.Companion.serializeAsArrayNode
import com.couchbase.client.kotlin.codec.ValueAndType.Companion.serializeAsObjectNode

/**
 * Create instances using the [positional] or [named] factory methods.
 */
public sealed class AnalyticsParameters {

    internal open fun serialize(serializer: JsonSerializer): JsonNode? = null

    public object None : AnalyticsParameters()

    private class Positional(
        private val values: List<ValueAndType<*>>,
    ) : AnalyticsParameters() {
        override fun serialize(serializer: JsonSerializer) = serializer.serializeAsArrayNode(values)
    }

    private class Named(
        private val nameToValue: Map<String, ValueAndType<*>>,
    ) : AnalyticsParameters() {
        override fun serialize(serializer: JsonSerializer) = serializer.serializeAsObjectNode(nameToValue)
    }

    public companion object {
        /**
         * Values to plug into positional placeholders in the statement.
         * ```
         * parameters = AnalyticsParameters.positional {
         *     param("airline") // replacement for first ?
         *     param(3)         // replacement for second ?
         * }
         * ```
         */
        public fun positional(paramSetterBlock: PositionalParametersBuilder.() -> Unit): AnalyticsParameters {
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
        public fun named(paramSetterBlock: NamedParametersBuilder.() -> Unit): AnalyticsParameters {
            val builder = NamedParametersBuilder()
            builder.apply(paramSetterBlock)
            return Named(builder.build())
        }

        @Deprecated(
            level = DeprecationLevel.WARNING,
            message = "Not compatible with JsonSerializer implementations that require type information, like kotlinx.serialization." +
                    " Please use the overload that takes a parameter builder lambda."
        )
        public fun named(values: Map<String, Any?>): AnalyticsParameters = Named(values.mapValues { entry -> ValueAndType.untyped(entry.value) })

        @Deprecated(
            level = DeprecationLevel.WARNING,
            message = "Not compatible with JsonSerializer implementations that require type information, like kotlinx.serialization." +
                    " Please use the overload that takes a parameter builder lambda."
        )
        @Suppress("DeprecatedCallableAddReplaceWith")
        public fun named(vararg values: Pair<String, Any?>): AnalyticsParameters = named(values.toMap())

        @Deprecated(
            level = DeprecationLevel.WARNING,
            message = "Not compatible with JsonSerializer implementations that require type information, like kotlinx.serialization." +
                    " Please use the overload that takes a parameter builder lambda."
        )
        @Suppress("DeprecatedCallableAddReplaceWith")
        public fun positional(values: List<Any?>): AnalyticsParameters = Positional(values.map { ValueAndType.untyped(it) })

    }
}
