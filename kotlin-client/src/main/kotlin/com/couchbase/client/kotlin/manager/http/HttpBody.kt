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

package com.couchbase.client.kotlin.manager.http

public class HttpBody internal constructor(
    public val contentType: String,
    public val content: ByteArray,
) {
    public companion object {
        public fun json(json: ByteArray): HttpBody =
            HttpBody("application/json", json)

        public fun json(json: String): HttpBody =
            HttpBody("application/json", json.toByteArray())

        public fun form(values: NameValuePairs): HttpBody =
            HttpBody("application/x-www-form-urlencoded", values.urlEncoded.toByteArray())

        public fun form(values: Map<String, Any>): HttpBody = form(NameValuePairs.of(values))

        public fun form(vararg values: Pair<String, Any>): HttpBody = form(NameValuePairs.of(*values))
    }
}
