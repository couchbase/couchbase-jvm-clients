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

public sealed class QueryParameters {

    internal abstract fun inject(queryJson: MutableMap<String, Any?>)

    public object None : QueryParameters() {
        override fun inject(queryJson: MutableMap<String, Any?>) {
        }
    }

    public class Named(private val values: Map<String, Any?>) : QueryParameters() {
        public constructor(vararg pairs: Pair<String, Any?>) : this(pairs.toMap())

        override fun inject(queryJson: MutableMap<String, Any?>): Unit =
            values.forEach { (key, value) ->
                queryJson[key.addPrefixIfAbsent("$")] = value
            }

        private fun String.addPrefixIfAbsent(prefix: String) =
            if (startsWith(prefix)) this else prefix + this
    }

    public class Positional(private val values: List<Any?>) : QueryParameters() {
        public constructor(vararg values: Any?) : this(values.toList())

        override fun inject(queryJson: MutableMap<String, Any?>): Unit {
            if (values.isNotEmpty()) {
                queryJson["args"] = values
            }
        }
    }

    public companion object {
        public fun positional(values: List<Any?>): QueryParameters = Positional(values)
        public fun named(values: Map<String, Any?>): QueryParameters = Named(values)
    }
}
