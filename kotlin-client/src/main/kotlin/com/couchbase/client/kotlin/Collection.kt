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
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.env.TimeoutConfig
import com.couchbase.client.core.error.CasMismatchException
import com.couchbase.client.core.error.DefaultErrorUtil
import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.DocumentUnretrievableException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.context.KeyValueErrorContext.completedRequest
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.msg.Response
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.ResponseStatus.SUBDOC_FAILURE
import com.couchbase.client.core.msg.kv.GetAndLockRequest
import com.couchbase.client.core.msg.kv.GetAndTouchRequest
import com.couchbase.client.core.msg.kv.GetMetaRequest
import com.couchbase.client.core.msg.kv.GetRequest
import com.couchbase.client.core.msg.kv.InsertRequest
import com.couchbase.client.core.msg.kv.KeyValueRequest
import com.couchbase.client.core.msg.kv.RemoveRequest
import com.couchbase.client.core.msg.kv.ReplaceRequest
import com.couchbase.client.core.msg.kv.SubdocGetRequest
import com.couchbase.client.core.msg.kv.SubdocMutateRequest
import com.couchbase.client.core.msg.kv.TouchRequest
import com.couchbase.client.core.msg.kv.UnlockRequest
import com.couchbase.client.core.msg.kv.UpsertRequest
import com.couchbase.client.core.service.kv.ReplicaHelper
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.TypeRef
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Counter
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.ExistsResult
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.GetReplicaResult
import com.couchbase.client.kotlin.kv.GetResult
import com.couchbase.client.kotlin.kv.LookupInResult
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.MutateInResult
import com.couchbase.client.kotlin.kv.MutateInSpec
import com.couchbase.client.kotlin.kv.MutationResult
import com.couchbase.client.kotlin.kv.StoreSemantics
import com.couchbase.client.kotlin.kv.internal.encodeInSpan
import com.couchbase.client.kotlin.kv.internal.levelIfSynchronous
import com.couchbase.client.kotlin.kv.internal.observe
import com.couchbase.client.kotlin.kv.internal.subdocGet
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

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

    internal val defaultJsonSerializer: JsonSerializer = env.jsonSerializer
    internal val defaultTranscoder: Transcoder = env.transcoder

    /**
     * Provides access to operations that apply only to binary documents.
     *
     * This is also where the counter increment and decrement operations live.
     */
    public val binary: BinaryCollection = BinaryCollection(this)

    private fun TimeoutConfig.kvTimeout(durability: Durability): Duration =
        (if (durability.isPersistent()) kvDurableTimeout() else kvTimeout()).toKotlinDuration()

    internal fun CommonOptions.actualKvTimeout(durability: Durability): java.time.Duration =
        (timeout ?: env.timeoutConfig().kvTimeout(durability)).toJavaDuration()

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
     * @throws DocumentNotFoundException if a document with ID [id] is not found in the collection.
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
                common.actualKvTimeout(Durability.none()),
                core.context(),
                collectionId,
                common.actualRetryStrategy(),
                common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET),
            )

            return exec(request, common) {
                GetResult.withUnknownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder)
            }
        }

        subdocGet(validateDocumentId(id), withExpiry, project, common).let {
            return GetResult(id, it.cas, Content(it.content, it.flags), it.expiry, defaultTranscoder)
        }
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
     * @throws DocumentNotFoundException if a document with ID [id] is not found in the collection.
     */
    public suspend fun getAndLock(
        id: String,
        lockTime: Duration,
        common: CommonOptions = CommonOptions.Default,
    ): GetResult {
        val request = GetAndLockRequest(
            validateDocumentId(id),
            common.actualKvTimeout(Durability.none()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            lockTime.toJavaDuration(),
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_LOCK),
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
            common.actualKvTimeout(Durability.none()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            expiry.encode(),
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET_AND_TOUCH),
        )

        return exec(request, common) {
            GetResult.withKnownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder, expiry)
        }
    }

    public fun getAllReplicas(
        id: String,
        common: CommonOptions = CommonOptions.Default,
    ): Flow<GetReplicaResult> = ReplicaHelper.getAllReplicasReactive(
        core,
        collectionId,
        id,
        common.actualKvTimeout(Durability.none()),
        common.actualRetryStrategy(),
        common.clientContext,
        common.parentSpan,
    ).asFlow().map { GetReplicaResult(id, it, defaultTranscoder) }

    /**
     * @throws DocumentUnretrievableException if the document could not be
     * retrieved from at least one replica.
     */
    public suspend fun getAnyReplica(
        id: String,
        common: CommonOptions = CommonOptions.Default,
    ): GetReplicaResult {
        return getAnyReplicaOrNull(id, common)
            ?: throw DocumentUnretrievableException(ReducedKeyValueErrorContext.create(id, collectionId))
    }

    /**
     * Like [getAnyReplica], but returns null instead of throwing
     * [DocumentUnretrievableException] if the document was not found.
     */
    @VolatileCouchbaseApi
    public suspend fun getAnyReplicaOrNull(
        id: String,
        common: CommonOptions = CommonOptions.Default,
    ): GetReplicaResult? {
        val response = ReplicaHelper.getAnyReplicaReactive(
            core,
            collectionId,
            id,
            common.actualKvTimeout(Durability.none()),
            common.actualRetryStrategy(),
            common.clientContext,
            common.parentSpan,
        ).awaitFirstOrNull() ?: return null

        return GetReplicaResult(id, response, defaultTranscoder)
    }

    public suspend fun exists(
        id: String,
        common: CommonOptions = CommonOptions.Default,
    ): ExistsResult {
        val request = GetMetaRequest(
            validateDocumentId(id),
            common.actualKvTimeout(Durability.none()),
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
                success || response.status() == ResponseStatus.NOT_FOUND -> ExistsResult.NotFound
                else -> throw DefaultErrorUtil.keyValueStatusToException(request, response)
            }
        } finally {
            request.logicallyComplete()
        }
    }

    /**
     * @throws DocumentNotFoundException if a document with ID [id] is not found in the collection.
     * @throws CasMismatchException if [cas] != 0 and does not match the existing document's CAS value.
     */
    public suspend fun remove(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.none(),
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

    /**
     * Like [remove], but returns null instead of throwing
     * [DocumentNotFoundException] if the document was not found.
     */
    @VolatileCouchbaseApi
    public suspend fun removeOrNull(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.none(),
        cas: Long = 0,
    ): MutationResult? {
        return try {
            remove(id, common, durability, cas)
        } catch (_: DocumentNotFoundException) {
            null;
        }
    }

    /**
     * @throws DocumentExistsException if a document with the same ID already exists.
     */
    public suspend inline fun <reified T> insert(
        id: String,
        content: T,
        common: CommonOptions = CommonOptions.Default,
        transcoder: Transcoder? = null,
        durability: Durability = Durability.none(),
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

        try {
            return exec(request, common) {
                if (durability is Durability.ClientVerified) {
                    observe(request, id, durability, it.cas(), it.mutationToken())
                }
                MutationResult(it.cas(), it.mutationToken().orElse(null))
            }
        } catch (t: CasMismatchException) {
            throw DocumentExistsException(t.context())
        } catch (t: DocumentNotFoundException) { // Map NOT_STORED
            throw DocumentExistsException(t.context())
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
        durability: Durability = Durability.none(),
        expiry: Expiry = Expiry.None,
        @SinceCouchbase("7.0") preserveExpiry: Boolean = false,
    ): MutationResult = internalUpsert(id, content, typeRef(), common, transcoder, durability, expiry, preserveExpiry)

    @PublishedApi
    internal suspend fun <T> internalUpsert(
        id: String,
        content: T,
        contentType: TypeRef<T>,
        common: CommonOptions,
        transcoder: Transcoder?,
        durability: Durability,
        expiry: Expiry,
        preserveExpiry: Boolean,
    ): MutationResult {
        val span = common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_UPSERT)
        val (encodedContent, encodingNanos) = encodeInSpan(transcoder, content, contentType, span)

        val request = UpsertRequest(
            validateDocumentId(id),
            encodedContent.bytes,
            expiry.encode(),
            preserveExpiry,
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
     * @throws DocumentNotFoundException if a document with ID [id] is not found in the collection.
     * @throws CasMismatchException if [cas] != 0 and does not match the existing document's CAS value.
     */
    public suspend inline fun <reified T> replace(
        id: String,
        content: T,
        common: CommonOptions = CommonOptions.Default,
        transcoder: Transcoder? = null,
        durability: Durability = Durability.none(),
        expiry: Expiry = Expiry.None,
        @SinceCouchbase("7.0") preserveExpiry: Boolean = false,
        cas: Long = 0,
    ): MutationResult = internalReplace(id, content, typeRef(), common, transcoder, durability, expiry, preserveExpiry, cas)

    @PublishedApi
    internal suspend fun <T> internalReplace(
        id: String,
        content: T,
        contentType: TypeRef<T>,
        common: CommonOptions,
        transcoder: Transcoder?,
        durability: Durability,
        expiry: Expiry,
        preserveExpiry: Boolean,
        cas: Long,
    ): MutationResult {
        val span = common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_REPLACE)
        val (encodedContent, encodingNanos) = encodeInSpan(transcoder, content, contentType, span)

        val request = ReplaceRequest(
            validateDocumentId(id),
            encodedContent.bytes,
            expiry.encode(),
            preserveExpiry,
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
            common.actualKvTimeout(Durability.none()),
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
            common.actualKvTimeout(Durability.none()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            validateDocumentId(id),
            cas,
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_UNLOCK)
        )
        exec(request, common) {}
    }

    /**
     * Retrieves specific fields of a document.
     *
     * @param block callback for processing the results, with [LookupInResult] as the receiver.
     *
     * @sample com.couchbase.client.kotlin.samples.subdocLookup
     * @sample com.couchbase.client.kotlin.samples.subdocLookupWithoutLambda
     */
    public suspend inline fun <T, L : LookupInSpec> lookupIn(
        id: String,
        spec: L,
        common: CommonOptions = CommonOptions.Default,
        accessDeleted: Boolean = false,
        block: LookupInResult.() -> T
    ): T {
        val result = lookupIn(id, spec, common, accessDeleted)
        return block(result)
    }

    /**
     * Retrieves specific fields of a document.
     *
     * @sample com.couchbase.client.kotlin.samples.subdocLookup
     * @sample com.couchbase.client.kotlin.samples.subdocLookupWithoutLambda
     */
    public suspend fun lookupIn(
        id: String,
        spec: LookupInSpec,
        common: CommonOptions = CommonOptions.Default,
        accessDeleted: Boolean = false,
    ): LookupInResult {
        require(spec.commands.isNotEmpty()) { "Must specify at least one lookup" }
        require(spec.commands.size <= 16) { "Must specify no more than 16 lookups" }

        val flags: Byte = if (accessDeleted) SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED else 0
        val request = SubdocGetRequest(
            common.actualKvTimeout(Durability.none()),
            core.context(),
            collectionId,
            common.actualRetryStrategy(),
            id,
            flags,
            spec.commands.sortedBy { !it.xattr() }, // xattr commands must come first
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN)
        )

        try {
            core.exec(request, common).let { response ->
                if (response.status().success() || response.status() == SUBDOC_FAILURE) {
                    return LookupInResult(
                        id,
                        spec.commands.size,
                        response.cas(),
                        response.isDeleted,
                        response.values().toList(),
                        defaultJsonSerializer,
                        spec,
                    )
                }
                throw DefaultErrorUtil.keyValueStatusToException(request, response)
            }
        } finally {
            request.logicallyComplete()
        }
    }

    public suspend fun mutateIn(
        id: String,
        common: CommonOptions = CommonOptions.Default,
        expiry: Expiry = Expiry.none(),
        preserveExpiry: Boolean = false,
        durability: Durability = Durability.none(),
        storeSemantics: StoreSemantics = StoreSemantics.replace(),
        serializer: JsonSerializer? = null,
        accessDeleted: Boolean = false,
        createAsDeleted: Boolean = false,
        block: MutateInSpec.() -> Unit,
    ): MutateInResult {
        // contract { callsInPlace(block, EXACTLY_ONCE) }
        //
        // When contracts become stable, we can do cool things like:
        //    val counter : SubdocLong
        //    val result = collection.mutateIn("foo") { counter = incrementAndGet("bar") }
        //    println(counter.get(result))

        val spec = MutateInSpec()
        spec.block()
        return mutateIn(
            id,
            spec,
            common,
            expiry,
            preserveExpiry,
            durability,
            storeSemantics,
            serializer,
            accessDeleted,
            createAsDeleted
        )
    }

    public suspend fun mutateIn(
        id: String,
        spec: MutateInSpec,
        common: CommonOptions = CommonOptions.Default,
        expiry: Expiry = Expiry.none(),
        preserveExpiry: Boolean = false,
        durability: Durability = Durability.none(),
        storeSemantics: StoreSemantics = StoreSemantics.replace(),
        serializer: JsonSerializer? = null,
        accessDeleted: Boolean = false,
        createAsDeleted: Boolean = false,
    ): MutateInResult {
        require(spec.commands.isNotEmpty()) { "Must specify at least one mutation" }
        require(spec.commands.size <= 16) { "Must specify no more than 16 mutations" }
        spec.checkNotExecuted()
        spec.executed = true

        val defaultCreateParent = (storeSemantics as? StoreSemantics.Replace)?.createParent ?: true
        val encodedCommands = spec.commands.map { it.encode(defaultCreateParent, serializer ?: defaultJsonSerializer) }
        val timeout = common.actualKvTimeout(durability)

        val request = SubdocMutateRequest(
            timeout,
            core.context(),
            collectionId,
            if (createAsDeleted) scope.bucket.config(timeout.toKotlinDuration()) else null,
            common.actualRetryStrategy(),
            id,
            storeSemantics == StoreSemantics.Insert,
            storeSemantics == StoreSemantics.Upsert,
            false,
            accessDeleted,
            createAsDeleted,
            encodedCommands.sortedBy { !it.xattr() }, // xattr commands must come first
            expiry.encode(),
            preserveExpiry,
            (storeSemantics as? StoreSemantics.Replace)?.cas ?: 0L,
            durability.levelIfSynchronous(),
            common.actualSpan(TracingIdentifiers.SPAN_REQUEST_KV_MUTATE_IN),
        )

        try {
            core.exec(request, common).let { response ->
                if (response.status().success()) {
                    if (durability is Durability.ClientVerified) {
                        observe(request, id, durability, response.cas(), response.mutationToken())
                    }
                    return MutateInResult(
                        spec.commands.size,
                        response.values().toList(),
                        response.cas(),
                        response.mutationToken().orElse(null),
                        spec,
                    )
                }

                if (storeSemantics == StoreSemantics.Insert
                        && (response.status() == ResponseStatus.EXISTS || response.status() == ResponseStatus.NOT_STORED)) {
                    throw DocumentExistsException(completedRequest(request, response.status()))
                }
                if (response.status() == SUBDOC_FAILURE) response.error().map { throw it }
                throw DefaultErrorUtil.keyValueStatusToException(request, response)
            }
        } finally {
            request.logicallyComplete()
        }
    }

    /**
     * Returns a counter backed by a document on the server.
     *
     * @param documentId the ID of the document to hold the counter value
     * @param expiry how long the counter document should exist before the counter is reset.
     * The expiry param is ignored if the counter document already exists.
     * @param durability durability requirements for counter operations
     *
     * @sample com.couchbase.client.kotlin.samples.counterRateLimiting
     * @sample com.couchbase.client.kotlin.samples.counterGenerateDocumentIds
     */
    @VolatileCouchbaseApi
    public fun counter(
        documentId: String,
        common: CommonOptions = CommonOptions.Default,
        durability: Durability = Durability.none(),
        expiry: Expiry = Expiry.none()
    ): Counter = Counter(this, documentId, common, durability, expiry)

    internal suspend inline fun <RESPONSE : Response, RESULT> exec(
        request: KeyValueRequest<RESPONSE>,
        common: CommonOptions,
        resultExtractor: (RESPONSE) -> RESULT,
    ): RESULT {
        try {
            val response = core.exec(request, common)
            if (response.status().success()) return resultExtractor(response)
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

internal fun validateDocumentId(id: String): String {
    require(id.isNotEmpty()) { "Document ID must not be empty." }
    return id
}
