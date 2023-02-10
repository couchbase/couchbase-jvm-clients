/*
 * Copyright 2022 Couchbase, Inc.
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
// [skip:<1.1.1]
package com.couchbase.client.performer.kotlin

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.kotlin.codec.RawStringTranscoder
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.GetResult
import com.couchbase.client.kotlin.kv.KvScanConsistency
import com.couchbase.client.kotlin.kv.MutationState
import com.couchbase.client.kotlin.kv.ScanTerm
import com.couchbase.client.kotlin.kv.ScanType
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.protobuf.ByteString
import com.couchbase.client.protocol.run.Result as FitRunResult
import com.couchbase.client.protocol.sdk.kv.rangescan.Range as FitRange
import com.couchbase.client.protocol.sdk.kv.rangescan.Scan as FitScan
import com.couchbase.client.protocol.sdk.kv.rangescan.ScanResult as FitScanResult
import com.couchbase.client.protocol.sdk.kv.rangescan.ScanTermChoice as FitScanTermChoice
import com.couchbase.client.protocol.sdk.kv.rangescan.ScanType as FitScanType
import com.couchbase.client.protocol.shared.MutationState as FitMutationState
import com.couchbase.client.protocol.shared.MutationToken as FitMutationToken
import com.couchbase.client.protocol.streams.Error as FitStreamError
import com.couchbase.client.protocol.streams.Signal as FitStreamSignal

fun FitScanType.toKotlin(): ScanType = when {
    hasRange() -> with(range) {
        when {
            hasFromTo() -> fromTo.toKotlin()
            hasDocIdPrefix() -> ScanType.prefix(docIdPrefix)
            else -> throw UnsupportedOperationException("Unsupported scan range: $this")
        }
    }

    hasSampling() -> with(sampling) {
        if (hasSeed()) ScanType.sample(limit, seed)
        else ScanType.sample(limit)
    }

    else -> throw UnsupportedOperationException("Unsupported scan type: $this")
}

fun FitRange.toKotlin(): ScanType {
    val from = from.toKotlin()
    val to = to.toKotlin()
    return when {
        from != null && to != null -> ScanType.range(from = from, to = to)
        from != null -> ScanType.range(from = from)
        to != null -> ScanType.range(to = to)
        else -> ScanType.range()
    }
}

fun FitScanTermChoice.toKotlin(): ScanTerm? = when {
    hasDefault() -> null
    hasMaximum() -> ScanTerm.Maximum
    hasMinimum() -> ScanTerm.Minimum
    hasTerm() -> with(term) {
        val exclusive = hasExclusive() && exclusive
        when {
            hasAsString() -> ScanTerm(asString, exclusive)
            hasAsBytes() -> ScanTerm(asBytes.toByteArray(), exclusive)
            else -> throw UnsupportedOperationException("Unsupported ScanTermChoice $this")
        }
    }

    else -> throw UnsupportedOperationException("Unsupported ScanTermChoice $this")
}

fun FitMutationState.toKotlin(): KvScanConsistency {
    val result = MutationState()
    tokensList.forEach { mt: FitMutationToken ->
        result.add(
            MutationToken(
                mt.partitionId.toShort(),
                mt.partitionUuid,
                mt.sequenceNumber,
                mt.bucketName,
            )
        )
    }
    return KvScanConsistency.consistentWith(result)
}

fun processScanResult(request: FitScan, documentOrId: Any): FitRunResult = try {
    val builder = FitScanResult.newBuilder()
        .setStreamId(request.streamConfig.streamId)

    when (documentOrId) {
        is String -> builder.id = documentOrId

        is GetResult -> with(documentOrId) {
            builder.id = id
            builder.cas = cas

            if (expiry is Expiry.Absolute) {
                builder.expiryTime = (expiry as Expiry.Absolute).instant.epochSecond
            }

            if (request.hasContentAs()) {
                val bytes: ByteArray = when {
                    request.contentAs.hasAsString() -> contentAs<String>(RawStringTranscoder).toByteArray()
                    request.contentAs.hasAsByteArray() -> content.bytes
                    request.contentAs.hasAsJson() -> contentAs<ObjectNode>().toString().toByteArray()
                    else -> throw UnsupportedOperationException("Unknown contentAs: ${request.contentAs}")
                }

                builder.content = ByteString.copyFrom(bytes)
            }
        }

        else -> throw AssertionError("Expected String or GetResult, but got ${documentOrId::class.java}")
    }

    FitRunResult.newBuilder()
        .setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setRangeScanResult(builder.build())
        )
        .build()

} catch (err: RuntimeException) {
    FitRunResult.newBuilder()
        .setStream(
            FitStreamSignal.newBuilder()
                .setError(
                    FitStreamError.newBuilder()
                        .setException(convertExceptionKt(err))
                        .setStreamId(request.streamConfig.streamId)
                )
        )
        .build()
}
