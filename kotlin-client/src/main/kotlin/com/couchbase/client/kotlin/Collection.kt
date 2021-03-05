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
import com.couchbase.client.core.error.DefaultErrorUtil
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.kv.KeyValueRequest
import com.couchbase.client.core.msg.kv.SubdocGetResponse
import com.couchbase.client.kotlin.annotations.InternalApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.MutationResult
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
     * Upserts a full document which might or might not exist yet.
     *
     * @param id The ID of the document to create or update.
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
