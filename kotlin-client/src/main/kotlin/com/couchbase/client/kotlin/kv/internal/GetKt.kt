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

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.error.subdoc.PathMismatchException
import com.couchbase.client.core.error.subdoc.PathNotFoundException
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.core.projections.ProjectionsApplier
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.LookupInSpec
import java.nio.charset.StandardCharsets
import java.time.Instant

internal class SubdocGetResult(
    val cas: Long,
    val content: ByteArray,
    val flags: Int,
    val expiry: Expiry?,
)

@OptIn(VolatileCouchbaseApi::class)
internal suspend fun Collection.subdocGet(
    id: String,
    withExpiry: Boolean = false,
    project: List<String>,
    common: CommonOptions,
): SubdocGetResult {
    validateProjections(id, project, withExpiry)

    val spec = LookupInSpec()
    val wholeDoc = if (project.isEmpty()) spec.get("") else null
    val projections = project.map { spec.get(it) }
    val expiry = if (!withExpiry) null else spec.get(LookupInMacro.ExpiryTime)

    // If we have projections, we know the result is JSON. Otherwise fetch the flags.
    val flagsSpec = if (project.isNotEmpty()) null else spec.get(LookupInMacro.Flags)

    lookupIn(id, spec, common) {
        val content = wholeDoc?.content
            ?: ProjectionsApplier.reconstructDocument(
                // build map of path -> content for all existing paths
                projections.mapNotNull {
                    runCatching { it.path to it.content }
                        .recover { t -> if (meansPathIsAbsent(t)) null else throw t }
                        .getOrThrow()
                }.toMap()
            )

        return SubdocGetResult(
            cas,
            content,
            flagsSpec?.contentAs<Int>() ?: CodecFlags.JSON_COMPAT_FLAGS,
            parseExpiry(expiry?.content),
        )
    }
}

internal fun meansPathIsAbsent(t: Throwable): Boolean =
    t is PathNotFoundException || t is PathMismatchException

internal fun Collection.validateProjections(id: String, projections: List<String>, withExpiry: Boolean) {
    try {
        require(projections.none { it.isEmpty() }) { "Empty string is not a valid projection." }
        val limit = if (withExpiry) 15 else 16
        require(projections.size <= limit) { "Too many projections; limit when withExpiry=$withExpiry is $limit" }
    } catch (t: Throwable) {
        throw InvalidArgumentException(t.message, t, ReducedKeyValueErrorContext.create(id, collectionId))
    }
}

private fun parseExpiry(expiryBytes: ByteArray?): Expiry? {
    if (expiryBytes == null) return null
    val epochSecond = String(expiryBytes, StandardCharsets.UTF_8).toLong()
    return if (epochSecond == 0L) Expiry.None else Expiry.Absolute(Instant.ofEpochSecond(epochSecond))
}
