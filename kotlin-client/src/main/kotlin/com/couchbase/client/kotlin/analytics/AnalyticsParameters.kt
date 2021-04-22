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

/**
 * Create instances using the [positional] or [named] factory methods.
 */
public sealed class AnalyticsParameters {

    internal abstract fun inject(queryJson: MutableMap<String, Any?>)

    public object None : AnalyticsParameters() {
        override fun inject(queryJson: MutableMap<String, Any?>) {
        }
    }

    private class Named internal constructor(
        private val values: Map<String, Any?>,
    ) : AnalyticsParameters() {
        override fun inject(queryJson: MutableMap<String, Any?>): Unit =
            values.forEach { (key, value) ->
                queryJson[key.addPrefixIfAbsent("$")] = value
            }

        private fun String.addPrefixIfAbsent(prefix: String) =
            if (startsWith(prefix)) this else prefix + this
    }

    private class Positional internal constructor(
        private val values: List<Any?>,
    ) : AnalyticsParameters() {
        override fun inject(queryJson: MutableMap<String, Any?>): Unit {
            if (values.isNotEmpty()) {
                queryJson["args"] = values
            }
        }
    }

    public companion object {
        public fun positional(values: List<Any?>): AnalyticsParameters = Positional(values)
        public fun named(values: Map<String, Any?>): AnalyticsParameters = Named(values)
        public fun named(vararg values: Pair<String, Any?>): AnalyticsParameters = Named(values.toMap())
    }
}
