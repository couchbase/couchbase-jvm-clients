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

import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.core.service.kv.Observe
import com.couchbase.client.core.service.kv.ObserveContext
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import kotlinx.coroutines.future.await
import java.util.Optional

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

internal fun Durability.levelIfSynchronous() =
    (this as? Durability.Synchronous)?.level.toOptional()
