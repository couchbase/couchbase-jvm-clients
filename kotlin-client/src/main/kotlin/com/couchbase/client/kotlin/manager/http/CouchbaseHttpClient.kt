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

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.endpoint.http.CoreHttpClient
import com.couchbase.client.core.endpoint.http.CoreHttpPath
import com.couchbase.client.core.endpoint.http.CoreHttpRequest
import com.couchbase.client.core.error.HttpStatusCodeException
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.CommonOptions
import kotlinx.coroutines.future.await

public fun urlEncode(s: String): String = UrlQueryStringBuilder.urlEncode(s)

private val PATH_PLACEHOLDER = Regex("\\{}")

/**
 * Replaces each `{}` placeholder in the template string with the
 * URL-encoded form of the corresponding additional argument.
 *
 * For example:
 * ```
 *     formatPath("/foo/{}/bar/{}", "hello world", "a/b")
 * ```
 * returns the string `"/foo/hello%20world/bar/a%2Fb"`
 *
 * @throws IllegalArgumentException if the number of placeholders
 * does not match the number of additional arguments.
 */
public fun formatPath(template: String, vararg args: String): String {
    val i = args.iterator()
    val result = template.replace(PATH_PLACEHOLDER) {
        require(i.hasNext()) { "Too few arguments (${args.size}) for format string: $template" }
        urlEncode(i.next())
    }
    require(!i.hasNext()) { "Too many arguments (${args.size}) for format string: $template" }
    return result
}

/**
 * Specialized HTTP client for the Couchbase Server REST API.
 * Get an instance by calling [Cluster.httpClient].
 *
 * Instead of an explicit host and port, provide an [HttpTarget]
 * identifying the service to invoke.
 *
 * @sample com.couchbase.client.kotlin.samples.httpClientGetBucketStats
 */
@Stability.Volatile
public class CouchbaseHttpClient internal constructor(
    private val cluster: Cluster,
) {
    private val core = cluster.core

    /**
     * @sample com.couchbase.client.kotlin.samples.httpClientGetBucketStats
     * @sample com.couchbase.client.kotlin.samples.httpClientGetWithQueryParameters
     */
    public suspend fun get(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
        queryString: NameValuePairs? = null,
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .get(CoreHttpPath.path(path), common.toCore())

        queryString?.let { request.queryString(it.urlEncoded) }

        return exec(request)
    }

    /**
     * @sample com.couchbase.client.kotlin.samples.httpClientPostWithFormData
     * @sample com.couchbase.client.kotlin.samples.httpClientPostWithJsonBody
     */
    public suspend fun post(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
        body: HttpBody? = null,
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .post(CoreHttpPath.path(path), common.toCore())

        body?.let { request.content(it.content, it.contentType) }

        return exec(request)
    }

    public suspend fun put(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
        body: HttpBody? = null,
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .put(CoreHttpPath.path(path), common.toCore())

        body?.let { request.content(it.content, it.contentType) }

        return exec(request)
    }

    public suspend fun delete(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .delete(CoreHttpPath.path(path), common.toCore())

        return exec(request)
    }

    private suspend fun exec(request: CoreHttpRequest.Builder): CouchbaseHttpResponse {
        val req = request
            .bypassExceptionTranslation(true) // no domain-specific exceptions, please
            .build()

        // If the target has a bucket, make sure the SDK has loaded the bucket config so the
        // request doesn't mysteriously time out while the service locator twiddles its thumbs.
        req.bucket()?.let { cluster.bucket(it) }

        return try {
            CouchbaseHttpResponse(req.exec(core).await())
        } catch (e: HttpStatusCodeException) {
            CouchbaseHttpResponse(e);
        }
    }
}
