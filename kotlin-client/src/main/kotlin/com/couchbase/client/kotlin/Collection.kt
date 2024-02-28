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

import com.couchbase.client.core.CoreKeyspace
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.api.kv.CoreAsyncResponse
import com.couchbase.client.core.api.shared.CoreMutationState
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.error.CasMismatchException
import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.DocumentUnretrievableException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.kv.*
import com.couchbase.client.core.manager.CoreCollectionQueryIndexManager
import com.couchbase.client.core.msg.Request
import com.couchbase.client.core.msg.Response
import com.couchbase.client.kotlin.annotations.UncommittedCouchbaseApi
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.TypeRef
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.internal.toSaturatedInt
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.couchbase.client.kotlin.kv.Counter
import com.couchbase.client.kotlin.kv.DEFAULT_SCAN_BATCH_ITEM_LIMIT
import com.couchbase.client.kotlin.kv.DEFAULT_SCAN_BATCH_SIZE_LIMIT
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.ExistsResult
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.kv.GetReplicaResult
import com.couchbase.client.kotlin.kv.GetResult
import com.couchbase.client.kotlin.kv.KvScanConsistency
import com.couchbase.client.kotlin.kv.LookupInReplicaResult
import com.couchbase.client.kotlin.kv.LookupInResult
import com.couchbase.client.kotlin.kv.LookupInSpec
import com.couchbase.client.kotlin.kv.MutateInResult
import com.couchbase.client.kotlin.kv.MutateInSpec
import com.couchbase.client.kotlin.kv.MutationResult
import com.couchbase.client.kotlin.kv.ScanType
import com.couchbase.client.kotlin.kv.StoreSemantics
import com.couchbase.client.kotlin.manager.query.CollectionQueryIndexManager
import com.couchbase.client.kotlin.util.StorageSize
import com.couchbase.client.kotlin.util.StorageSizeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import reactor.core.publisher.Flux
import java.util.*
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Operations that act on a Couchbase collection.
 */
public class Collection internal constructor(
    public val name: String,
    public val scope: Scope,
) {
    @VolatileCouchbaseApi
    public val keyspace: Keyspace = Keyspace(scope.bucket.name, scope.name, name)

    internal val collectionId: CollectionIdentifier =
        CollectionIdentifier(scope.bucket.name, scope.name.toOptional(), name.toOptional())

    internal val couchbaseOps: CoreCouchbaseOps = scope.couchbaseOps
    internal val env: ClusterEnvironment = scope.bucket.env
    internal val rangeScanOrchestrator
        get() = RangeScanOrchestrator(couchbaseOps.asCore(), collectionId)

    internal val defaultJsonSerializer: JsonSerializer = env.jsonSerializer
    internal val defaultTranscoder: Transcoder = env.transcoder

    /**
     * Provides access to operations that apply only to binary documents.
     *
     * This is also where the counter increment and decrement operations live.
     */
    public val binary: BinaryCollection = BinaryCollection(this)

    /**
     * A manager for administering this collection's SQL++ indexes.
     *
     * Requires Couchbase Server 7.0 or later. For earlier versions,
     * please use [Cluster.queryIndexes].
     */
    @SinceCouchbase("7.0")
    @VolatileCouchbaseApi
    public val queryIndexes: CollectionQueryIndexManager = CollectionQueryIndexManager(
        CoreCollectionQueryIndexManager(
            couchbaseOps.queryOps(),
            env.requestTracer(),
            CoreKeyspace.from(collectionId),
        )
    )

    private val kvOps = couchbaseOps.kvOps(CoreKeyspace.from(collectionId))

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
        return kvOps.getAsync(
            common.toCore(),
            validateDocumentId(id),
            project,
            withExpiry
        ).await().let {
            if (withExpiry) {
                val expiry = it.expiry()?.let { instant -> Expiry.of(instant) } ?: Expiry.none()
                GetResult.withKnownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder, expiry)
            } else {
                GetResult.withUnknownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder)
            }
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
     * Depending on the scan [type], returns from this collection:
     *
     * - Every document whose ID starts with a [ScanType.prefix].
     * - Every document whose ID is within a lexicographic [ScanType.range].
     * - A random [ScanType.sample] of documents.
     *
     * **CAVEAT:** This method is suitable for use cases that require relatively
     * low concurrency and tolerate relatively high latency.
     * If your application does many scans at once, or requires low latency results,
     * we recommend using SQL++ (with a primary index on the collection) instead.
     *
     * @param type Specifies which documents to include in the scan results.
     * Defaults to every document in the collection.
     *
     * @param consistency By default, the scan runs immediately, without waiting for
     * previous KV mutations to be indexed. If the scan results must include
     * certain mutations, pass [KvScanConsistency.consistentWith].
     *
     * @param batchItemLimit Tunes how many documents the server may send in a single round trip.
     * The value is per partition (vbucket), so multiply by 1024 when calculating memory requirements.
     * Might affect performance, but does not change the results of this method.
     *
     * @param batchSizeLimit Tunes how many bytes the server may send in a single round trip.
     * The value is per partition (vbucket), so multiply by 1024 when calculating memory requirements.
     * Might affect performance, but does not change the results of this method.
     */
    @SinceCouchbase("7.6")
    public fun scanDocuments(
        type: ScanType = ScanType.range(),
        common: CommonOptions = CommonOptions.Default,
        consistency: KvScanConsistency = KvScanConsistency.notBounded(),
        batchItemLimit: Int = DEFAULT_SCAN_BATCH_ITEM_LIMIT,
        batchSizeLimit: StorageSize = DEFAULT_SCAN_BATCH_SIZE_LIMIT,
    ): Flow<GetResult> {
        return scan(type, common, idsOnly = false, consistency, batchItemLimit, batchSizeLimit)
            .map {
                GetResult(
                    id = it.keyBytes().toStringUtf8(),
                    cas = it.cas(),
                    content = Content(it.value(), it.flags()),
                    expiry = it.expiry()?.let { expiryInstant -> Expiry.of(expiryInstant) } ?: Expiry.none(),
                    defaultTranscoder = defaultTranscoder,
                )
            }.asFlow()
    }

    /**
     * Like [scanDocuments], but returns only document IDs instead of full documents.
     *
     * **CAVEAT:** This method is suitable for use cases that require relatively
     * low concurrency and tolerate relatively high latency.
     * If your application does many scans at once, or requires low latency results,
     * we recommend using SQL++ (with a primary index on the collection) instead.
     */
    @SinceCouchbase("7.6")
    public fun scanIds(
        type: ScanType = ScanType.range(),
        common: CommonOptions = CommonOptions.Default,
        consistency: KvScanConsistency = KvScanConsistency.notBounded(),
        batchItemLimit: Int = DEFAULT_SCAN_BATCH_ITEM_LIMIT,
        batchSizeLimit: StorageSize = DEFAULT_SCAN_BATCH_SIZE_LIMIT,
    ): Flow<String> {
        return scan(type, common, idsOnly = true, consistency, batchItemLimit, batchSizeLimit)
            .map { it.keyBytes().toStringUtf8() }
            .asFlow()
    }

    /**
     * If in the future kv range scan must return metadata or something other than
     * a document ID or [GetResult], the evolution path is to mirror [Cluster.query].
     * That is, make `scan` public and return `Flow<ScanFlowItem>`, where `ScanFlowItem`
     * is a sealed class with subclasses `ScanRow` and `ScanMetadata`. Add `ScanResult`
     * to mirror `QueryResult`, along with `Flow<ScanFlowItem>.execute` extensions.
     *
     * Not doing all that stuff now because YAGNI.
     */
    internal fun scan(
        type: ScanType = ScanType.range(),
        common: CommonOptions = CommonOptions.Default,
        idsOnly: Boolean = false,
        consistency: KvScanConsistency = KvScanConsistency.notBounded(),
        batchItemLimit: Int = RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_ITEM_LIMIT,
        batchSizeLimit: StorageSize = StorageSize(
            RangeScanOrchestrator.RANGE_SCAN_DEFAULT_BATCH_BYTE_LIMIT.toLong(),
            StorageSizeUnit.BYTES
        )
    ): Flux<CoreRangeScanItem> {

        val options = object : CoreScanOptions {
            override fun commonOptions(): CoreCommonOptions = common.toCore()
            override fun idsOnly(): Boolean = idsOnly
            override fun consistentWith(): CoreMutationState? = consistency.mutationState?.let { CoreMutationState(consistency.mutationState) }
            override fun batchItemLimit(): Int = batchItemLimit
            override fun batchByteLimit(): Int = batchSizeLimit.inBytes.toSaturatedInt()
        }

        return when (type) {
            is ScanType.Sample -> {

                val samplingScan = object : CoreSamplingScan {
                    override fun limit(): Long = type.limit
                    override fun seed(): Optional<Long> = type.seed.toOptional();
                }

                return rangeScanOrchestrator.samplingScan(samplingScan, options)
            }

            is ScanType.Range -> {
                val rangeScan = object : CoreRangeScan {
                    override fun from(): CoreScanTerm = if (type.from == null) CoreScanTerm.MIN else CoreScanTerm(type.from.term, type.from.exclusive)
                    override fun to(): CoreScanTerm = if (type.to == null) CoreScanTerm.MAX else CoreScanTerm(type.to.term, type.to.exclusive)
                }
                rangeScanOrchestrator.rangeScan(rangeScan, options)
            }

            is ScanType.Prefix -> {
                rangeScanOrchestrator.rangeScan(CoreRangeScan.forPrefix(type.prefix), options)
            }
        }
    }

    /**
     * @throws DocumentNotFoundException if a document with ID [id] is not found in the collection.
     */
    public suspend fun getAndLock(
        id: String,
        lockTime: Duration,
        common: CommonOptions = CommonOptions.Default,
    ): GetResult {
        return kvOps.getAndLockAsync(
            common.toCore(),
            validateDocumentId(id),
            lockTime.toJavaDuration(),
        ).await().let {
            GetResult.withUnknownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder)
        }
    }

    public suspend fun getAndTouch(
        id: String,
        expiry: Expiry,
        common: CommonOptions = CommonOptions.Default,
    ): GetResult {
        return kvOps.getAndTouchAsync(
            common.toCore(),
            validateDocumentId(id),
            expiry.encode(),
        ).await().let {
            GetResult.withKnownExpiry(id, it.cas(), Content(it.content(), it.flags()), defaultTranscoder, expiry)
        }
    }

    /**
     * Like [get], but sends the request to all replicas in addition to the active.
     * Returns the results from all available sources.
     */
    public fun getAllReplicas(
        id: String,
        common: CommonOptions = CommonOptions.Default,
    ): Flow<GetReplicaResult> {
        return kvOps.getAllReplicasReactive(common.toCore(), id)
            .asFlow().map { GetReplicaResult(id, it, defaultTranscoder) }
    }

    /**
     * Like [get], but sends the request to all replicas in addition to the active.
     * Returns the result from whichever server node responded quickest.
     *
     * @throws DocumentUnretrievableException if the document could not be
     * retrieved from at least one location.
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
        val response = kvOps.getAnyReplicaReactive(common.toCore(), id)
            .awaitFirstOrNull() ?: return null

        return GetReplicaResult(id, response, defaultTranscoder)
    }

    public suspend fun exists(
        id: String,
        common: CommonOptions = CommonOptions.Default,
    ): ExistsResult {
        return kvOps.existsAsync(
            common.toCore(),
            id,
        ).await().let {
            if (it.exists()) ExistsResult(it.exists(), it.cas()) else ExistsResult.NotFound
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
        return kvOps.removeAsync(
            common.toCore(),
            id,
            cas,
            durability.toCore(),
        ).await().let {
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
            null
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
        return kvOps.insertAsync(
            common.toCore(),
            id,
            { (transcoder ?: defaultTranscoder).encode(content, contentType).toCore() },
            durability.toCore(),
            expiry.encode(),
        ).await().let {
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
        return kvOps.upsertAsync(
            common.toCore(),
            id,
            { (transcoder ?: defaultTranscoder).encode(content, contentType).toCore() },
            durability.toCore(),
            expiry.encode(),
            preserveExpiry,
        ).await().let {
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
        return kvOps.replaceAsync(
            common.toCore(),
            id,
            { (transcoder ?: defaultTranscoder).encode(content, contentType).toCore() },
            cas,
            durability.toCore(),
            expiry.encode(),
            preserveExpiry,
        ).await().let {
            MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    public suspend fun touch(
        id: String,
        expiry: Expiry,
        common: CommonOptions = CommonOptions.Default,
    ): MutationResult {
        return kvOps.touchAsync(
            common.toCore(),
            id,
            expiry.encode()
        ).await().let {
            MutationResult(it.cas(), it.mutationToken().orElse(null))
        }
    }

    public suspend fun unlock(
        id: String,
        cas: Long,
        common: CommonOptions = CommonOptions.Default,
    ) {
        kvOps.unlockAsync(
            common.toCore(),
            id,
            cas,
        ).await()
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
        val coreResult = kvOps.subdocGetAsync(
            common.toCore(),
            id,
            spec.commands,
            accessDeleted,
        ).await()

        return LookupInResult(coreResult, defaultJsonSerializer, spec)
    }

    /**
     * Like [lookupIn], but sends the request to all replicas in addition to the active.
     * Returns the result from whichever server node responded quickest.
     *
     * @param block callback for processing the results, with [LookupInReplicaResult] as the receiver.
     *
     * @throws DocumentUnretrievableException if the document could not be
     * retrieved from at least one location.
     *
     * @sample com.couchbase.client.kotlin.samples.subdocLookup
     * @sample com.couchbase.client.kotlin.samples.subdocLookupWithoutLambda
     */
    @SinceCouchbase("7.6")
    @UncommittedCouchbaseApi
    public suspend inline fun <T, L : LookupInSpec> lookupInAnyReplica(
        id: String,
        spec: L,
        common: CommonOptions = CommonOptions.Default,
        block: LookupInReplicaResult.() -> T
    ): T {
        val result = lookupInAnyReplica(id, spec, common)
        return block(result)
    }

    /**
     * Like [lookupIn], but sends the request to all replicas in addition to the active.
     * Returns the result from whichever server node responded quickest.
     *
     * @throws DocumentUnretrievableException if the document could not be
     * retrieved from at least one location.
     *
     * @sample com.couchbase.client.kotlin.samples.subdocLookupWithoutLambda
     */
    @SinceCouchbase("7.6")
    @UncommittedCouchbaseApi
    public suspend fun lookupInAnyReplica(
        id: String,
        spec: LookupInSpec,
        common: CommonOptions = CommonOptions.Default,
    ): LookupInReplicaResult {
        val coreResult = kvOps.subdocGetAnyReplicaReactive(
            common.toCore(),
            id,
            spec.commands
        ).awaitFirst()

        return LookupInReplicaResult(coreResult, defaultJsonSerializer, spec)
    }

    /**
     * Like [lookupIn], but sends the request to all replicas in addition to the active.
     * Returns the results from all available sources.
     */
    @SinceCouchbase("7.6")
    @UncommittedCouchbaseApi
    public fun lookupInAllReplicas(
        id: String,
        spec: LookupInSpec,
        common: CommonOptions = CommonOptions.Default,
    ): Flow<LookupInReplicaResult> {
        val flux = kvOps.subdocGetAllReplicasReactive(
            common.toCore(),
            id,
            spec.commands,
        )
        return flux
            .map { coreResult -> LookupInReplicaResult(coreResult, defaultJsonSerializer, spec) }
            .asFlow()
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
        spec.checkNotExecuted()
        spec.executed = true

        val defaultCreateParent = (storeSemantics as? StoreSemantics.Replace)?.createParent ?: true
        val cas: Long = (storeSemantics as? StoreSemantics.Replace)?.cas ?: 0L
        val actualSerializer = serializer ?: defaultJsonSerializer

        try {
            val coreResult = kvOps.subdocMutateAsync(
                common.toCore(),
                id,
                { spec.commands.map { it.encode(defaultCreateParent, actualSerializer) } },
                storeSemantics.toCore(),
                cas,
                durability.toCore(),
                expiry.encode(),
                preserveExpiry, accessDeleted, createAsDeleted
            ).await()

            return MutateInResult(coreResult, spec)
        } catch (e: InvalidArgumentException) {
            throw IllegalArgumentException(e.message, e)
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
}

internal fun <R : Response> Request<R>.logicallyComplete() = context().logicallyComplete()

internal fun validateDocumentId(id: String): String {
    require(id.isNotEmpty()) { "Document ID must not be empty." }
    return id
}

internal suspend fun <T> CoreAsyncResponse<T>.await() = toFuture().await()
