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
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.core.msg.kv.SubdocCommandType
import com.couchbase.client.core.msg.kv.SubdocGetRequest
import com.couchbase.client.core.msg.kv.SubdocGetResponse
import com.couchbase.client.core.projections.ProjectionsApplier
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.GetResult
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.*

internal fun Collection.createSubdocGetRequest(
    id: String,
    withExpiry: Boolean = false,
    project: List<String>,
    options: CommonOptions,
): SubdocGetRequest {
    validateProjections(id, project, withExpiry)
    val commands = ArrayList<SubdocGetRequest.Command>(16)

    if (project.isEmpty()) {
        // fetch whole document
        commands.add(SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false, commands.size))
    } else for (projection in project) {
        commands.add(SubdocGetRequest.Command(SubdocCommandType.GET, projection, false, commands.size))
    }

    if (withExpiry) {
        // xattrs must go first
        commands.add(
            0, SubdocGetRequest.Command(
                SubdocCommandType.GET,
                LookupInMacro.EXPIRY_TIME,
                true,
                commands.size
            )
        )

        // If we have projections, there is no need to fetch the flags
        // since only JSON is supported that implies the flags.
        // This will also "force" the transcoder on the read side to be
        // JSON aware since the flags are going to be hard-set to the
        // JSON compat flags.
        if (project.isEmpty()) {
            commands.add(
                1, SubdocGetRequest.Command(
                    SubdocCommandType.GET,
                    LookupInMacro.FLAGS,
                    true,
                    commands.size
                )
            )
        }
    }

    return SubdocGetRequest(
        options.actualKvTimeout(Durability.disabled()),
        core.context(),
        collectionId,
        options.actualRetryStrategy(),
        id,
        0x00,
        commands,
        options.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN)
    )
}

internal fun Collection.validateProjections(id: String, projections: List<String>, withExpiry: Boolean) {
    try {
        if (projections.any { it.isEmpty() }) {
            throw InvalidArgumentException.fromMessage("Empty string is not a valid projection.")
        }

        if (withExpiry) {
            if (projections.size > 15) {
                throw InvalidArgumentException.fromMessage(
                    "Only a maximum of 16 fields can be " +
                            "projected per request due to a server limitation (includes the expiration macro as one field)."
                )
            }
        } else {
            if (projections.size > 16) {
                throw InvalidArgumentException.fromMessage(
                    "Only a maximum of 16 fields can be " +
                            "projected per request due to a server limitation."
                )
            }
        }

    } catch (t: Throwable) {
        throw InvalidArgumentException(
            "Argument validation failed",
            t,
            ReducedKeyValueErrorContext.create(id, collectionId)
        )
    }
}

internal fun Collection.parseSubdocGet(id: String, response: SubdocGetResponse): GetResult {
    val cas = response.cas()
    var exptime: ByteArray? = null
    var content: ByteArray? = null
    var flags: ByteArray? = null
    for (value in response.values()) {
        if (value != null) {
            if (LookupInMacro.EXPIRY_TIME == value.path()) {
                exptime = value.value()
            } else if (LookupInMacro.FLAGS == value.path()) {
                flags = value.value()
            } else if (value.path().isEmpty()) {
                content = value.value()
            }
        }
    }
    val convertedFlags =
        if (flags == null) CodecFlags.JSON_COMPAT_FLAGS else String(flags, StandardCharsets.UTF_8).toInt()
    if (content == null) {
        content = try {
            ProjectionsApplier.reconstructDocument(response)
        } catch (e: Exception) {
            throw CouchbaseException("Unexpected Exception while decoding Sub-Document get", e)
        }
    }

    val expiration = parseExpiry(exptime)
    return GetResult.withKnownExpiry(id, cas, Content(content!!, convertedFlags), defaultTranscoder, expiration)
}

private fun parseExpiry(expiryBytes: ByteArray?): Expiry {
    if (expiryBytes == null) return Expiry.None
    val epochSecond = String(expiryBytes, StandardCharsets.UTF_8).toLong()
    return if (epochSecond == 0L) Expiry.None else Expiry.Absolute(Instant.ofEpochSecond(epochSecond))
}
