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

/**
 * Create instances using the [positional] or [named] factory methods.
 */
public sealed class QueryParameters {

    internal abstract fun inject(queryJson: MutableMap<String, Any?>)

    public object None : QueryParameters() {
        override fun inject(queryJson: MutableMap<String, Any?>) {
        }
    }

    internal class Named internal constructor(
        private val values: Map<String, Any?>,
    ) : QueryParameters() {
        override fun inject(queryJson: MutableMap<String, Any?>): Unit =
            values.forEach { (key, value) ->
                queryJson[key.addPrefixIfAbsent("$")] = value
            }

        private fun String.addPrefixIfAbsent(prefix: String) =
            if (startsWith(prefix)) this else prefix + this
    }

    internal class Positional internal constructor(
        private val values: List<Any?>,
    ) : QueryParameters() {
        override fun inject(queryJson: MutableMap<String, Any?>): Unit {
            if (values.isNotEmpty()) {
                queryJson["args"] = values
            }
        }
    }

    public companion object {
        // positional has no varargs overload because there would be ambiguity in the case of values that are themselves lists.
        public fun positional(values: List<Any?>): QueryParameters = Positional(values)
        public fun named(values: Map<String, Any?>): QueryParameters = Named(values)
        public fun named(vararg values: Pair<String, Any?>): QueryParameters = Named(values.toMap())
    }
}
