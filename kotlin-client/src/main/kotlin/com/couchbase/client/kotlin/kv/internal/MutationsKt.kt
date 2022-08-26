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

import com.couchbase.client.core.cnc.CbTracing
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.service.kv.Observe
import com.couchbase.client.core.service.kv.ObserveContext
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.TypeRef
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import kotlinx.coroutines.future.await
import java.util.*
import kotlin.system.measureNanoTime

internal suspend fun Collection.observe(
    request: Request<*>,
    id: String,
    durability: Durability.ClientVerified,
    cas: Long,
    mutationToken: Optional<MutationToken>,
    remove: Boolean = false,
) {
    val ctx = ObserveContext(
        core.context(),
        durability.persistTo.coreHandle,
        durability.replicateTo.coreHandle,
        mutationToken,
        cas,
        collectionId,
        id,
        remove,
        request.timeout(),
        request.requestSpan()
    )

    Observe.poll(ctx).toFuture().await()
}

internal fun <T> Collection.encodeInSpan(
    transcoder: Transcoder?,
    input: T,
    type: TypeRef<T>,
    parentSpan: RequestSpan,
): Pair<Content, Long> {
    val encodedContent: Content
    val encodeSpan = CbTracing.newSpan(env.requestTracer(), TracingIdentifiers.SPAN_REQUEST_ENCODING, parentSpan)
    val encodingNanos = measureNanoTime {
        try {
            encodedContent = (transcoder ?: defaultTranscoder).encode(input, type)
        } finally {
            encodeSpan.end()
        }
    }
    return encodedContent to encodingNanos
}

internal fun Durability.levelIfSynchronous() =
    (this as? Durability.Synchronous)?.level.toOptional()
