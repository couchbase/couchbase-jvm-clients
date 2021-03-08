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

package com.couchbase.client.kotlin

import com.couchbase.client.core.Core
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.DefaultErrorUtil
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.TimeoutException
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.kv.GetRequest
import com.couchbase.client.core.msg.kv.KeyValueRequest
import com.couchbase.client.core.msg.kv.SubdocGetResponse
import com.couchbase.client.kotlin.annotations.InternalApi
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.GetResult
import com.couchbase.client.kotlin.kv.MutationResult
import com.couchbase.client.kotlin.kv.internal.createSubdocGetRequest
import com.couchbase.client.kotlin.kv.internal.parseSubdocGet
import com.couchbase.client.kotlin.kv.internal.upsertWithReifiedType
import kotlinx.coroutines.future.await

/**
 * Operations that act on a Couchbase collection.
 */
public class Collection internal constructor(
    public val name: String,
    public val scopeName: String,
    public val bucket: Bucket,
) {
    internal val collectionId: CollectionIdentifier =
        CollectionIdentifier(bucket.name, scopeName.toOptional(), name.toOptional())

    internal val core: Core = bucket.core
    internal val env: ClusterEnvironment = bucket.env

    internal val defaultSerializer: JsonSerializer = bucket.env.jsonSerializer
    internal val defaultTranscoder: Transcoder = bucket.env.transcoder

    internal fun CommonOptions.actualKvTimeout() = timeout ?: env.timeoutConfig().kvTimeout()
    internal fun CommonOptions.actualRetryStrategy() = retryStrategy ?: env.retryStrategy()
    internal fun CommonOptions.actualSpan(name: String) = env.requestTracer().requestSpan(name, parentSpan)

    /**
     * Gets a document from this collection.
     *
     * @param id The ID of the document to get.
     * @param withExpiry As a performance optimization, the result does not include
     * information about document expiry unless this parameter is set to true.
     * @param project Prunes the returned JSON content, retaining only the elements
     * rooted at the specified sub-document paths, as well as any intermediate
     * parent elements required to preserve the document structure.
     * A maximum of 16 paths can be projected at a time (15 if `withExpiry` is true).
     * To work around this limitation, consider specifying shallower paths or
     * fetching the whole document.
     *
     * @throws DocumentNotFoundException if the document id is not found in the collection.
     * @throws TimeoutException if the operation times out before getting a result.
     * @throws CouchbaseException for all other error reasons.
     */
    public suspend fun get(
        id: String,
        options: CommonOptions = CommonOptions.Default,
        withExpiry: Boolean = false,
        project: List<String> = emptyList(),
    ): GetResult {
        if (!withExpiry && project.isEmpty()) {
            val request = GetRequest(
                id,
                options.actualKvTimeout(),
                core.context(),
                collectionId,
                options.actualRetryStrategy(),
                options.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET),
            )
            try {
                exec(request, options).apply {
                    return GetResult.withUnknownExpiry(id, cas(), Content(content(), flags()), defaultTranscoder)
                }
            } finally {
                request.endSpan()
            }
        }

        val request = createSubdocGetRequest(id, withExpiry, project, options)
        try {
            return parseSubdocGet(id, exec(request, options))
        } finally {
            request.endSpan()
        }
    }

    /**
     * Updates a document if it exists, otherwise inserts it.
     *
     * To update a document using a Compare And Swap (CAS) value,
     * see [replace].
     *
     * @param id The ID of the document to insert or update.
     * @param content document content
     * @param transcoder defaults to the transcoder configured on the cluster environment.
     * @throws TimeoutException if the operation times out before getting a result.
     * @throws CouchbaseException for all other error reasons.
     */
    public suspend inline fun <reified T> upsert(
        id: String,
        content: T,
        common: CommonOptions = CommonOptions.Default,
        transcoder: Transcoder? = null,
        durability: Durability = Durability.disabled(),
        expiry: Expiry = Expiry.None,
    ): MutationResult {
        @OptIn(InternalApi::class)
        return upsertWithReifiedType(this, id, content, typeRef(), common, transcoder, durability, expiry)
    }

    internal suspend fun <R : Response> exec(
        request: KeyValueRequest<R>,
        options: CommonOptions,
    ): R {
        val response = core.exec(request, options)
        if (response.status().success()) {
            return response
        }

        if (response is SubdocGetResponse) {
            response.error().ifPresent { throw it }
        }

        throw DefaultErrorUtil.keyValueStatusToException(request, response)
    }

    private suspend fun <R : Response> Core.exec(request: Request<R>, options: CommonOptions): R {
        request.context().clientContext(options.clientContext)
        send(request)
        return request.response().await()
    }

}

internal fun <R : Response> Request<R>.endSpan() = context().logicallyComplete()
