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
 * URL-encoded form of the corresponding list element.
 *
 * For example:
 * ```
 *     formatPath("/foo/{}/bar/{}", listOf("hello world", "a/b"))
 * ```
 * returns the string `"/foo/hello%20world/bar/a%2Fb"`
 *
 * @throws IllegalArgumentException if the number of placeholders
 * does not match the size of the list.
 */
public fun formatPath(template: String, args: List<String>): String {
    val i = args.iterator()
    val result = template.replace(PATH_PLACEHOLDER) {
        require(i.hasNext()) { "Too few arguments (${args.size}) for format string: $template" }
        urlEncode(i.next())
    }
    require(!i.hasNext()) { "Too many arguments (${args.size}) for format string: $template" }
    return result
}

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
public fun formatPath(template: String, vararg args: String): String =
    formatPath(template, listOf(*args))

/**
 * An HTTP header is represented as a pair of header name to value.
 */
public typealias Header = Pair<String, String>

/**
 * Specialized HTTP client for the Couchbase Server REST API.
 * An instance is available as [Cluster.httpClient].
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
     * Issues a `GET` request to the [target] Couchbase service.
     *
     * If the [path] has variable parts, consider using [formatPath]
     * to build the path string.
     *
     * Query parameters can be passed via [queryString], and/or
     * embedded in the [path] if you prefer.
     *
     * There's usually no reason to set extra [headers] unless you're
     * invoking an API that requires a special header.
     *
     * @sample com.couchbase.client.kotlin.samples.httpClientGetBucketStats
     * @sample com.couchbase.client.kotlin.samples.httpClientGetWithQueryParameters
     */
    public suspend fun get(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
        queryString: NameValuePairs? = null,
        headers: List<Header> = emptyList(),
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .get(CoreHttpPath.path(path), common.toCore())

        queryString?.let { request.queryString(it.urlEncoded) }

        return exec(request, headers)
    }

    /**
     * Issues a `POST` request to the [target] Couchbase service.
     *
     * If the [path] has variable parts, consider using [formatPath]
     * to build the path string.
     *
     * Request content and type is defined by [body]. Specifying a body
     * automatically sets the "Content-Type" header appropriately.
     *
     * There's usually no reason to set extra [headers] unless you're
     * invoking an API that requires a special header.
     *
     * @sample com.couchbase.client.kotlin.samples.httpClientPostWithFormData
     * @sample com.couchbase.client.kotlin.samples.httpClientPostWithJsonBody
     */
    public suspend fun post(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
        body: HttpBody? = null,
        headers: List<Header> = emptyList(),
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .post(CoreHttpPath.path(path), common.toCore())

        body?.let { request.content(it.content, it.contentType) }

        return exec(request, headers)
    }

    /**
     * Issues a `PUT` request to the [target] Couchbase service.
     *
     * If the [path] has variable parts, consider using [formatPath]
     * to build the path string.
     *
     * Request content and type is defined by [body]. Specifying a body
     * automatically sets the "Content-Type" header appropriately.
     *
     * There's usually no reason to set extra [headers] unless you're
     * invoking an API that requires a special header.
     */
    public suspend fun put(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
        body: HttpBody? = null,
        headers: List<Header> = emptyList(),
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .put(CoreHttpPath.path(path), common.toCore())

        body?.let { request.content(it.content, it.contentType) }

        return exec(request, headers)
    }

    /**
     * Issues a `DELETE` request to the [target] Couchbase service.
     *
     * If the [path] has variable parts, consider using [formatPath]
     * to build the path string.
     *
     * There's usually no reason to set extra [headers] unless you're
     * invoking an API that requires a special header.
     */
    public suspend fun delete(
        target: HttpTarget,
        path: String,
        common: CommonOptions = CommonOptions.Default,
        headers: List<Header> = emptyList(),
    ): CouchbaseHttpResponse {
        val request = CoreHttpClient(core, target.coreTarget)
            .delete(CoreHttpPath.path(path), common.toCore())

        return exec(request, headers)
    }

    private suspend fun exec(request: CoreHttpRequest.Builder, headers: List<Header>): CouchbaseHttpResponse {
        headers.forEach { request.header(it.first, it.second) }
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
