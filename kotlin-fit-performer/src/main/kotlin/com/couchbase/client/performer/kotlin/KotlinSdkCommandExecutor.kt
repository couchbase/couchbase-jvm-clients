/*
 * Copyright (c) 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.performer.kotlin

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.codec.JacksonJsonSerializer
import com.couchbase.client.kotlin.codec.JsonTranscoder
import com.couchbase.client.kotlin.codec.RawBinaryTranscoder
import com.couchbase.client.kotlin.codec.RawJsonTranscoder
import com.couchbase.client.kotlin.codec.RawStringTranscoder
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.GetResult
import com.couchbase.client.kotlin.kv.MutationResult
import com.couchbase.client.kotlin.kv.PersistTo
import com.couchbase.client.kotlin.kv.ReplicateTo
import com.couchbase.client.performer.core.commands.SdkCommandExecutor
import com.couchbase.client.performer.core.perf.Counters
import com.couchbase.client.performer.core.perf.PerRun
import com.couchbase.client.performer.core.util.ErrorUtil
import com.couchbase.client.performer.core.util.TimeUtil
import com.couchbase.client.performer.kotlin.util.ClusterConnection
import com.couchbase.client.protocol.shared.CouchbaseExceptionEx
import com.couchbase.client.protocol.shared.CouchbaseExceptionType
import com.couchbase.client.protocol.shared.Exception
import com.couchbase.client.protocol.shared.ExceptionOther
import com.couchbase.client.protocol.shared.MutationToken
import com.couchbase.client.protocol.shared.Transcoder
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import com.couchbase.client.protocol.shared.Content as ProtoContent

/**
 * SdkOperation performs each requested SDK operation
 */
class KotlinSdkCommandExecutor(private val connection: ClusterConnection,
                               private val counters: Counters) : SdkCommandExecutor(counters) {
    fun createCommon(hasTimeout: Boolean, timeout: Int): CommonOptions {
        if (hasTimeout) {
            return CommonOptions(timeout = timeout.milliseconds)
        }
        else {
            return CommonOptions.Default
        }
    }

    fun convertDurability(hasDurability: Boolean, durability: com.couchbase.client.protocol.shared.DurabilityType): Durability {
        if (hasDurability) {
            if (durability.hasDurabilityLevel()) {
                return when (durability.getDurabilityLevel()) {
                    com.couchbase.client.protocol.shared.Durability.NONE -> Durability.none()
                    com.couchbase.client.protocol.shared.Durability.MAJORITY -> Durability.majority()
                    com.couchbase.client.protocol.shared.Durability.MAJORITY_AND_PERSIST_TO_ACTIVE -> Durability.majorityAndPersistToActive()
                    com.couchbase.client.protocol.shared.Durability.PERSIST_TO_MAJORITY -> Durability.persistToMajority()
                    else -> throw UnsupportedOperationException("Unknown durability")
                }
            }
            else if (durability.hasObserve()) {
                return Durability.clientVerified(when (durability.getObserve().getPersistTo()) {
                    com.couchbase.client.protocol.shared.PersistTo.PERSIST_TO_NONE -> PersistTo.NONE
                    com.couchbase.client.protocol.shared.PersistTo.PERSIST_TO_ACTIVE -> PersistTo.ACTIVE
                    com.couchbase.client.protocol.shared.PersistTo.PERSIST_TO_ONE -> PersistTo.ONE
                    com.couchbase.client.protocol.shared.PersistTo.PERSIST_TO_TWO -> PersistTo.TWO
                    com.couchbase.client.protocol.shared.PersistTo.PERSIST_TO_THREE -> PersistTo.THREE
                    com.couchbase.client.protocol.shared.PersistTo.PERSIST_TO_FOUR -> PersistTo.FOUR
                    else -> throw UnsupportedOperationException("Unknown durability")
                }, when (durability.getObserve().getReplicateTo()) {
                    com.couchbase.client.protocol.shared.ReplicateTo.REPLICATE_TO_NONE -> ReplicateTo.NONE
                    com.couchbase.client.protocol.shared.ReplicateTo.REPLICATE_TO_ONE -> ReplicateTo.ONE
                    com.couchbase.client.protocol.shared.ReplicateTo.REPLICATE_TO_TWO -> ReplicateTo.TWO
                    com.couchbase.client.protocol.shared.ReplicateTo.REPLICATE_TO_THREE -> ReplicateTo.THREE
                    else -> throw UnsupportedOperationException("Unknown durability")
                })
            }
            else {
                throw UnsupportedOperationException("Unknown durability")
            }
        }
        else {
            return Durability.None
        }
    }

    fun convertExpiry(hasExpiry: Boolean, expiry: com.couchbase.client.protocol.shared.Expiry): Expiry {
        if (hasExpiry) {
            if (expiry.hasAbsoluteEpochSecs()) {
                return Expiry.of(Instant.ofEpochSecond(expiry.absoluteEpochSecs))
            }
            else if (expiry.hasRelativeSecs()) {
                return Expiry.of(expiry.relativeSecs.seconds)
            }
            else {
                throw UnsupportedOperationException("Unknown expiry")
            }
        }
        else {
            return Expiry.None
        }
    }

    override fun performOperation(op: com.couchbase.client.protocol.sdk.Command, perRun: PerRun): com.couchbase.client.protocol.run.Result {
        val result = com.couchbase.client.protocol.run.Result.newBuilder()

        runBlocking {
            if (op.hasInsert()) {
                val request = op.insert
                val collection = connection.collection(request.location)
                val content = content(request.content)
                val docId = getDocId(request.location)
                result.initiated = TimeUtil.getTimeNow()
                val start = System.nanoTime()
                val r = if (request.hasOptions()) {
                    val options = request.options
                    collection.insert(docId, content,
                        common = createCommon(options.hasTimeoutMsecs(), options.timeoutMsecs),
                        transcoder = convertTranscoder(options.hasTranscoder(), options.transcoder),
                        durability = convertDurability(options.hasDurability(), options.durability),
                        expiry = convertExpiry(options.hasExpiry(), options.expiry))
                }
                else collection.insert(docId, content)
                result.elapsedNanos = System.nanoTime() - start
                if (op.returnResult) populateResult(result, r)
                else setSuccess(result)
            } else if (op.hasGet()) {
                val request = op.get
                val collection = connection.collection(request.location)
                val docId = getDocId(request.location)
                result.initiated = TimeUtil.getTimeNow()
                val start = System.nanoTime()
                val r = if (request.hasOptions()) {
                    if (request.options.hasTranscoder()) {
                        // Kotlin does not have this
                        throw UnsupportedOperationException("Unknown transcoder")
                    }
                    val options = request.options
                    collection.get(docId,
                        common = createCommon(options.hasTimeoutMsecs(), options.timeoutMsecs),
                        withExpiry = if (options.hasWithExpiry()) options.hasWithExpiry() else false,
                        project = options.projectionList.toList())
                }
                else collection.get(docId)
                result.elapsedNanos = System.nanoTime() - start
                if (op.returnResult) populateResult(result, r)
                else setSuccess(result)
            } else if (op.hasRemove()) {
                val request = op.remove
                val collection = connection.collection(request.location)
                val docId = getDocId(request.location)
                result.initiated = TimeUtil.getTimeNow()
                val start = System.nanoTime()
                val r = if (request.hasOptions()) {
                    val options = request.options
                    collection.remove(docId,
                        common = createCommon(options.hasTimeoutMsecs(), options.timeoutMsecs),
                        durability = convertDurability(options.hasDurability(), options.durability),
                        cas = if (options.hasCas()) options.cas else 0)
                }
                else collection.remove(docId)
                result.elapsedNanos = System.nanoTime() - start
                if (op.returnResult) populateResult(result, r)
                else setSuccess(result)
            } else if (op.hasReplace()) {
                val request = op.replace
                val collection = connection.collection(request.location)
                val docId = getDocId(request.location)
                val content = content(request.content)
                result.initiated = TimeUtil.getTimeNow()
                val start = System.nanoTime()
                val r = if (request.hasOptions()) {
                    val options = request.options
                    collection.replace(docId, content,
                        common = createCommon(options.hasTimeoutMsecs(), options.timeoutMsecs),
                        transcoder = convertTranscoder(options.hasTranscoder(), options.transcoder),
                        durability = convertDurability(options.hasDurability(), options.durability),
                        expiry = convertExpiry(options.hasExpiry(), options.expiry),
                        preserveExpiry = if (options.hasPreserveExpiry()) options.preserveExpiry else false,
                        cas = if (options.hasCas()) options.cas else 0)
                }
                else collection.replace(docId, content)
                result.elapsedNanos = System.nanoTime() - start
                if (op.returnResult) populateResult(result, r)
                else setSuccess(result)
            } else if (op.hasUpsert()) {
                val request = op.upsert
                val collection = connection.collection(request.location)
                val docId = getDocId(request.location)
                val content = content(request.content)
                result.initiated = TimeUtil.getTimeNow()
                val start = System.nanoTime()
                val r = if (request.hasOptions()) {
                    val options = request.options
                    collection.upsert(docId, content,
                        common = createCommon(options.hasTimeoutMsecs(), options.timeoutMsecs),
                        transcoder = convertTranscoder(options.hasTranscoder(), options.transcoder),
                        durability = convertDurability(options.hasDurability(), options.durability),
                        expiry = convertExpiry(options.hasExpiry(), options.expiry),
                        preserveExpiry = if (options.hasPreserveExpiry()) options.preserveExpiry else false)
                }
                else collection.upsert(docId, content)
                result.elapsedNanos = System.nanoTime() - start
                if (op.returnResult) populateResult(result, r)
                else setSuccess(result)
            } else {
                throw UnsupportedOperationException(IllegalArgumentException("Unknown operation"))
            }
        }

        return result.build()
    }

    override fun convertException(raw: Throwable?): Exception {
        val ret = Exception.newBuilder()

        if (raw is CouchbaseException || raw is java.lang.UnsupportedOperationException) {
            val type = if (raw is java.lang.UnsupportedOperationException) {
                CouchbaseExceptionType.SDK_UNSUPPORTED_OPERATION_EXCEPTION
            } else {
                ErrorUtil.convertException(raw as CouchbaseException)
            }
            val out = CouchbaseExceptionEx.newBuilder()
                    .setName(raw.javaClass.simpleName)
                    .setType(type)
                    .setSerialized(raw.toString())
            if (raw.cause != null) {
                out.cause = convertException(raw.cause)
            }
            ret.setCouchbase(out)
        } else {
            ret.setOther(ExceptionOther.newBuilder()
                    .setName(raw!!.javaClass.simpleName)
                    .setSerialized(raw.toString()))
        }

        return ret.build()
    }

    fun content(content: ProtoContent): Any? {
        return when {
            content.hasPassthroughString() -> content.passthroughString

            content.hasConvertToJson() -> jsonMapper.readValue(
                content.convertToJson.toByteArray(),
                jacksonTypeRef<Map<String, Any?>>(),
            )

            else -> throw UnsupportedOperationException("Unknown content: $content")
        }
    }

    private fun convertTranscoder(hasTranscoder: Boolean, transcoderMaybe: Transcoder?): com.couchbase.client.kotlin.codec.Transcoder? {
        if (hasTranscoder) {
            val transcoder = transcoderMaybe!!
            return if (transcoder.hasRawJson()) RawJsonTranscoder
            else if (transcoder.hasJson()) jsonTranscoder
            else if (transcoder.hasRawString()) RawStringTranscoder
            else if (transcoder.hasRawBinary()) RawBinaryTranscoder
            // Kotlin does not have LegacyTranscoder
            else throw UnsupportedOperationException("Unknown transcoder")
        }
        return null
    }

    private fun setSuccess(result: com.couchbase.client.protocol.run.Result.Builder) {
        result.setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setSuccess(true)
        )
    }

    private fun populateResult(
        result: com.couchbase.client.protocol.run.Result.Builder,
        value: MutationResult
    ) {
        val builder = com.couchbase.client.protocol.sdk.kv.MutationResult.newBuilder()
            .setCas(value.cas)
        if (value.mutationToken != null) {
            val mt = value.mutationToken!!
            builder.setMutationToken(
                MutationToken.newBuilder()
                    .setPartitionId(mt.partitionID().toInt())
                    .setPartitionUuid(mt.partitionUUID())
                    .setSequenceNumber(mt.sequenceNumber())
                    .setBucketName(mt.bucketName())
            )
        }
        result.setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setMutationResult(builder)
        )
    }

    private fun populateResult(result: com.couchbase.client.protocol.run.Result.Builder, value: GetResult) {
        val builder = com.couchbase.client.protocol.sdk.kv.GetResult.newBuilder()
            .setCas(value.cas)
            .setContent(ByteString.copyFrom(value.content.bytes))
        when (val expiry = value.expiry) {
            is Expiry.Absolute -> builder.expiryTime = expiry.instant.epochSecond
            else -> {}
        }
        result.setSdk(
            com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setGetResult(builder)
        )
    }

    companion object {
        val jsonMapper = jsonMapper {
            addModule(Jdk8Module())
            addModule(KotlinModule.Builder().build())
        }

        val jsonTranscoder = JsonTranscoder(JacksonJsonSerializer(jsonMapper))
    }
}
