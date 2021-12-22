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

import com.couchbase.client.core.endpoint.http.CoreHttpResponse
import com.couchbase.client.core.error.HttpStatusCodeException
import com.couchbase.client.kotlin.internal.toStringUtf8

public class CouchbaseHttpResponse internal constructor(
    public val statusCode: Int,
    public val content: ByteArray,
) {
    public val success: Boolean get() = statusCode in 200..299
    public val contentAsString: String get() = content.toStringUtf8()

    internal constructor(coreResponse: CoreHttpResponse) : this(coreResponse.httpStatus(), coreResponse.content())

    internal constructor(e: HttpStatusCodeException) : this(e.httpStatusCode(), e.content().toByteArray())

    override fun toString(): String {
        return "CouchbaseHttpResponse(statusCode=$statusCode, contentAsString=$contentAsString)"
    }
}
