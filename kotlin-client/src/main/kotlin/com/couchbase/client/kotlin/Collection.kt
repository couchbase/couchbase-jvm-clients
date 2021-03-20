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
import com.couchbase.client.core.cnc.TracingIdentifiers.SPAN_GET_ALL_REPLICAS
import com.couchbase.client.core.env.TimeoutConfig
import com.couchbase.client.core.error.DefaultErrorUtil
import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.DocumentUnretrievableException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.context.KeyValueErrorContext
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.kv.GetAndLockRequest
import com.couchbase.client.core.msg.kv.GetAndTouchRequest
import com.couchbase.client.core.msg.kv.GetMetaRequest
import com.couchbase.client.core.msg.kv.GetRequest
import com.couchbase.client.core.msg.kv.InsertRequest
import com.couchbase.client.core.msg.kv.InsertResponse
import com.couchbase.client.core.msg.kv.KeyValueRequest
import com.couchbase.client.core.msg.kv.RemoveRequest
import com.couchbase.client.core.msg.kv.ReplaceRequest
import com.couchbase.client.core.msg.kv.SubdocGetResponse
import com.couchbase.client.core.msg.kv.SubdocMutateResponse
import com.couchbase.client.core.msg.kv.TouchRequest
import com.couchbase.client.core.msg.kv.UnlockRequest
import com.couchbase.client.core.msg.kv.UpsertRequest
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.TypeRef
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.ExistsResult
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.GetReplicaResult
import com.couchbase.client.kotlin.kv.GetResult
import com.couchbase.client.kotlin.kv.MutationResult
import com.couchbase.client.kotlin.kv.internal.createSubdocGetRequest
import com.couchbase.client.kotlin.kv.internal.encodeInSpan
import com.couchbase.client.kotlin.kv.internal.levelIfSynchronous
import com.couchbase.client.kotlin.kv.internal.observe
import com.couchbase.client.kotlin.kv.internal.parseSubdocGet
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.future.await
import java.time.Duration

/**
 * Operations that act on a Couchbase collection.
 */
public class Collection internal constructor(
    public val name: String,
    public val scope: Scope,
) {
    internal val collectionId: CollectionIdentifier =
        CollectionIdentifier(scope.bucket.name, scope.name.toOptional(), name.toOptional())

    internal val core: Core = scope.bucket.core
    internal val env: ClusterEnvironment = scope.bucket.env

    internal val defaultSerializer: JsonSerializer = env.jsonSerializer
    internal val defaultTranscoder: Transcoder = env.transcoder

    private fun TimeoutConfig.kvTimeout(durability: Durability): Duration =
        if (durability.isPersistent()) kvDurableTimeout() else kvTimeout()

    internal fun CommonOptions.actualKvTimeout(durability: Durability): Duration =
        timeout ?: env.timeoutConfig().kvTimeout(durability)

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
     */
    public suspend fun get(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        withExpiry: Boolean = false,
        project: List<String> = emptyList(),
    ): GetResult {
        if (!withExpiry && project.isEmpty()) {
            val request = GetRequest(
                validateDocumentId(id),
                common.actualKvTimeout(Durability.disabled()),
                core.context(),
                collectionId,
                common.actualRetryStrategy(),
                common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET),
            )

            return exec(request, common) {
                GetResult.withUnknownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder)
            }
        }

        val request = createSubdocGetRequest(validateDocumentId(id), withExpiry, project, common)
        return exec(request, common) { response -> parseSubdocGet(id, response) }
    }
    /**
     * Like [get], but returns null instead of throwing
     * [DocumentNotFoundException] if the document is not found.
     *
     * @see get
     */
    @VolatileCouchbaseApi
    public suspend inline fun getOrNull(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        withExpiry: Boolean = false,
        project: List<String> = emptyList(),
    ): GetResult? = try {
        get(id, common, withExpiry, project)
    } catch (t: DocumentNotFoundException) {
        null
    }

    /**
     * @throws DocumentNotFoundException if the document id is not found in the collection.
     */
    public suspend fun getAndLock(
        id: String,
        lockTime: Duration,
        common: CommonOptions = CommonOptions.Default,
    ): GetResult {
        val request = GetAndLockRequest(
            validateDocumentId(id),
            common.actualKvTimeout(Durability.disabled()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            lockTime,
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_GAL),
        )

        return exec(request, common) {
            GetResult.withUnknownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder)
        }
    }

    public suspend fun getAndTouch(
        id: String,
        expiry: Expiry,
        common: CommonOptions = CommonOptions.Default,
    ): GetResult {
        val request = GetAndTouchRequest(
            validateDocumentId(id),
            common.actualKvTimeout(Durability.disabled()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            expiry.encode(),
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_GAT),
        )

        return exec(request, common) {
            GetResult.withKnownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder, expiry)
        }
    }

    public suspend fun getAllReplicas(
        id: String,
        common: CommonOptions,
    ): Flow<GetReplicaResult> = internalGetAllReplicas(id, common, SPAN_GET_ALL_REPLICAS)

    /**
     * @throws DocumentUnretrievableException if the document could not be
     * retrieved from at least one replica.
     */
    public suspend fun getAnyReplica(
        id: String,
        common: CommonOptions,
    ): GetReplicaResult = internalGetAllReplicas(id, common, SPAN_GET_ALL_REPLICAS).first()

    private suspend fun internalGetAllReplicas(
        id: String,
        common: CommonOptions,
        tracingIdentifier: String,
    ): Flow<GetReplicaResult> {
        validateDocumentId(id)
        TODO()
    }

    public suspend fun exists(
        id: String,
        common: CommonOptions = CommonOptions.Default,
    ): ExistsResult {
        val request = GetMetaRequest(
            validateDocumentId(id),
            common.actualKvTimeout(Durability.disabled()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_EXISTS),
        )

        try {
            val response = core.exec(request, common)
            val success = response.status().success()

            return when {
                success && !response.deleted() -> ExistsResult(true, response.cas())
                response.status() == ResponseStatus.NOT_FOUND || success -> ExistsResult.NotFound
                else -> throw DefaultErrorUtil.keyValueStatusToException(request, response)
            }
        } finally {
            request.logicallyComplete()
        }
    }

    public suspend fun remove(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.disabled(),
        cas: Long = 0,
    ): MutationResult {
        val request = RemoveRequest(
            validateDocumentId(id),
            cas,
            common.actualKvTimeout(durability),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            durability.levelIfSynchronous(),
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_REMOVE),
        )

        return exec(request, common) {
            if (durability is Durability.ClientVerified) {
                observe(request, id, durability, it.cas(), it.mutationToken())
            }
            MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    public suspend inline fun <reified T> insert(
        id: String,
        content: T,
        common: CommonOptions = CommonOptions.Default,
        transcoder: Transcoder? = null,
        durability: Durability = Durability.disabled(),
        expiry: Expiry = Expiry.None,
    ): MutationResult = internalInsert(id, content, typeRef(), common, transcoder, durability, expiry)

    @PublishedApi
    internal suspend fun <T> internalInsert(
        id: String,
        content: T,
        contentType: TypeRef<T>,
        common: CommonOptions,
        transcoder: Transcoder?,
        durability: Durability,
        expiry: Expiry,
    ): MutationResult {
        val span = common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_INSERT)
        val (encodedContent, encodingNanos) = encodeInSpan(transcoder, content, contentType, span)

        val request = InsertRequest(
            validateDocumentId(id),
            encodedContent.bytes,
            expiry.encode(),
            encodedContent.flags,
            common.actualKvTimeout(durability),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            durability.levelIfSynchronous(),
            span,
        )
        request.context().encodeLatency(encodingNanos)

        return exec(request, common) {
            if (durability is Durability.ClientVerified) {
                observe(request, id, durability, it.cas(), it.mutationToken())
            }
            MutationResult(it.cas(), it.mutationToken().orElse(null))
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
     */
    public suspend inline fun <reified T> upsert(
        id: String,
        content: T,
        common: CommonOptions = CommonOptions.Default,
        transcoder: Transcoder? = null,
        durability: Durability = Durability.disabled(),
        expiry: Expiry = Expiry.None,
    ): MutationResult = internalUpsert(id, content, typeRef(), common, transcoder, durability, expiry)

    @PublishedApi
    internal suspend fun <T> internalUpsert(
        id: String,
        content: T,
        contentType: TypeRef<T>,
        common: CommonOptions,
        transcoder: Transcoder?,
        durability: Durability,
        expiry: Expiry,
    ): MutationResult {
        val span = common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_UPSERT)
        val (encodedContent, encodingNanos) = encodeInSpan(transcoder, content, contentType, span)

        val request = UpsertRequest(
            validateDocumentId(id),
            encodedContent.bytes,
            expiry.encode(),
            encodedContent.flags,
            common.actualKvTimeout(durability),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            durability.levelIfSynchronous(),
            span,
        )
        request.context().encodeLatency(encodingNanos)

        return exec(request, common) {
            if (durability is Durability.ClientVerified) {
                observe(request, id, durability, it.cas(), it.mutationToken())
            }
            MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    public suspend inline fun <reified T> replace(
        id: String,
        content: T,
        common: CommonOptions = CommonOptions.Default,
        transcoder: Transcoder? = null,
        durability: Durability = Durability.disabled(),
        expiry: Expiry = Expiry.None,
        cas: Long = 0,
    ): MutationResult = internalReplace(id, content, typeRef(), common, transcoder, durability, expiry, cas)

    @PublishedApi
    internal suspend fun <T> internalReplace(
        id: String,
        content: T,
        contentType: TypeRef<T>,
        common: CommonOptions,
        transcoder: Transcoder?,
        durability: Durability,
        expiry: Expiry,
        cas: Long,
    ): MutationResult {
        val span = common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_REPLACE)
        val (encodedContent, encodingNanos) = encodeInSpan(transcoder, content, contentType, span)

        val request = ReplaceRequest(
            validateDocumentId(id),
            encodedContent.bytes,
            expiry.encode(),
            encodedContent.flags,
            common.actualKvTimeout(durability),
            cas,
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            durability.levelIfSynchronous(),
            span,
        )
        request.context().encodeLatency(encodingNanos)

        return exec(request, common) {
            if (durability is Durability.ClientVerified) {
                observe(request, id, durability, it.cas(), it.mutationToken())
            }
            MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    public suspend fun touch(
        id: String,
        expiry: Expiry,
        common: CommonOptions = CommonOptions.Default,
    ): MutationResult {
        val request = TouchRequest(
            common.actualKvTimeout(Durability.disabled()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            validateDocumentId(id),
            expiry.encode(),
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_TOUCH),
        )

        return exec(request, common) {
            MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    public suspend fun unlock(
        id: String,
        cas: Long,
        common: CommonOptions = CommonOptions.Default,
    ): Unit {
        if (cas == 0L) throw InvalidArgumentException("Unlock CAS must be non-zero.", null, ReducedKeyValueErrorContext.create(id, collectionId))

        val request = UnlockRequest(
            common.actualKvTimeout(Durability.disabled()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            validateDocumentId(id),
            cas,
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_UNLOCK)
        )
        exec(request, common) {}
    }

    internal suspend inline fun <RESPONSE : Response, RESULT> exec(
        request: KeyValueRequest<RESPONSE>,
        common: CommonOptions,
        resultExtractor: (RESPONSE) -> RESULT,
    ): RESULT {
        try {
            val response = core.exec(request, common)
            if (response.status().success()) return resultExtractor(response)

            if (response is SubdocGetResponse) response.error().ifPresent { throw it }
            if (response is SubdocMutateResponse) response.error().ifPresent { throw it }

            if (response is InsertResponse && response.status() == ResponseStatus.EXISTS) {
                throw DocumentExistsException(KeyValueErrorContext.completedRequest(request, response.status()))
            }

            throw DefaultErrorUtil.keyValueStatusToException(request, response)
        } finally {
            request.logicallyComplete()
        }
    }

    internal suspend fun <R : Response> Core.exec(request: Request<R>, common: CommonOptions): R {
        request.context().clientContext(common.clientContext)
        send(request)
        return request.response().await()
    }
}

internal fun <R : Response> Request<R>.logicallyComplete() = context().logicallyComplete()

private fun validateDocumentId(id: String): String {
    require(id.isNotEmpty()) { "Document ID must not be empty." }
    return id;
}
