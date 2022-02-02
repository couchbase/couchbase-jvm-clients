/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.http

import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.kotlin.http.NameValuePairs.Companion.ofPreEncoded

/**
 * Represents a query string or form data.
 *
 * Automatically URL-encodes the values (unless you use the [ofPreEncoded]
 * which takes a preformatted string).
 */
public class NameValuePairs internal constructor(internal val urlEncoded: String) {
    override fun toString(): String = urlEncoded

    public fun isEmpty(): Boolean = urlEncoded.isEmpty()

    public companion object {
        public fun of(params: Map<String, Any>): NameValuePairs {
            val builder = UrlQueryStringBuilder.createForUrlSafeNames()
            params.forEach { (k, v) -> builder.add(k, v.toString()) }
            return NameValuePairs(builder.build())
        }

        public fun of(vararg values: Pair<String, Any>): NameValuePairs {
            val builder = UrlQueryStringBuilder.createForUrlSafeNames()
            values.forEach { builder.add(it.first, it.second.toString()) }
            return NameValuePairs(builder.build())
        }

        /**
         * Here's the "escape hatch." If you already have a pre-encoded string,
         * use this factory method to prevent the values from being redundantly URL-encoded.
         *
         * @param preEncoded a query string of the form `name1=value1&name2=value2...`
         * with any unsafe characters already URL-encoded.
         */
        public fun ofPreEncoded(preEncoded: String): NameValuePairs = NameValuePairs(preEncoded)
    }
}
