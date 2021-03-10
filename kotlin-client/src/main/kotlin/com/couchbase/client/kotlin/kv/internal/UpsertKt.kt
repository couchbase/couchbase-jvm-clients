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

package com.couchbase.client.kotlin.kv.internal

import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.msg.kv.UpsertRequest
import com.couchbase.client.core.util.Validators
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.InternalApi
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.TypeRef
import com.couchbase.client.kotlin.endSpan
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.MutationResult
import kotlin.system.measureNanoTime

@InternalApi
public object InternalUpsert {
    /**
     * Does the actual upsert after [Collection.upsert] captures the content's reified type.
     *
     * Public because it's called from a public inline method. Lives over here
     * (instead of on Collection) so it's less visible to users.
     */
    public suspend fun <T> upsertWithReifiedType(
        collection: Collection,
        id: String,
        content: T,
        contentType: TypeRef<T>,
        options: CommonOptions,
        transcoder: Transcoder?,
        durability: Durability,
        expiry: Expiry,
    ): MutationResult {
        val request = collection.upsertRequest(
            id, content, contentType, options,
            transcoder ?: collection.defaultTranscoder,
            durability, expiry,
        )
        try {
            val response = collection.exec(request, options)

            if (durability is Durability.ClientVerified) {
                observe(collection, request, id, durability, response.cas(), response.mutationToken())
            }

            return MutationResult(response.cas(), response.mutationToken().orElse(null))

        } finally {
            request.endSpan()
        }
    }
}

internal fun <T> Collection.upsertRequest(
    id: String,
    content: T,
    contentType: TypeRef<T>,
    options: CommonOptions,
    transcoder: Transcoder,
    durability: Durability,
    expiry: Expiry,
): UpsertRequest {
    Validators.notNullOrEmpty(id, "Id") { ReducedKeyValueErrorContext.create(id, collectionId) }

    val span = options.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_UPSERT)
    val encodeSpan = env.requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_ENCODING, span)

    var encodedContent: Content
    val encodingNanos = measureNanoTime {
        try {
            encodedContent = transcoder.encode(content, contentType)
        } finally {
            encodeSpan.end()
        }
    }

    val syncDurability = if (durability is Durability.Synchronous) durability.level else null

    val request = UpsertRequest(
        id,
        encodedContent.bytes,
        expiry.encode(),
        encodedContent.flags,
        options.actualKvTimeout(),
        core.context(),
        collectionId,
        options.actualRetryStrategy(),
        syncDurability.toOptional(),
        span,
    )
    request.context().encodeLatency(encodingNanos)
    return request
}
