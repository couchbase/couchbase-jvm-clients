package com.couchbase.client.performer.kotlin.util

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.kv.MutationState
import com.couchbase.client.protocol.run.Result
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds

internal fun kotlin.time.Duration.toProtobufDuration(): com.google.protobuf.Duration =
    toComponents { seconds, nanoseconds ->
        com.google.protobuf.Duration.newBuilder()
            .setSeconds(seconds)
            .setNanos(nanoseconds)
            .build()
    }

internal fun com.couchbase.client.protocol.shared.MutationState.toKotlin() =
    MutationState(
        tokensList.map {
            MutationToken(
                it.partitionId.toShort(),
                it.partitionUuid,
                it.sequenceNumber,
                it.bucketName
            )
        }
    )

internal fun Result.Builder.setSuccess() {
    setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setSuccess(true)
    )
}

class ConverterUtil {
    companion object {
        fun createCommon(hasTimeout: Boolean, timeout: Int): CommonOptions {
            return if (hasTimeout) CommonOptions(timeout = timeout.milliseconds)
            else CommonOptions.Default
        }

        fun setSuccess(result: Result.Builder) {
            return result.setSuccess()
        }
    }
}
