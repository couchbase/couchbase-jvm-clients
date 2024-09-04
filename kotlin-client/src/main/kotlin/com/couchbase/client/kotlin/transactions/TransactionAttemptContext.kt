/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.kotlin.transactions

import com.couchbase.client.core.api.query.CoreQueryResult
import com.couchbase.client.core.cnc.CbTracing
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.cnc.TracingIdentifiers
import com.couchbase.client.core.error.CasMismatchException
import com.couchbase.client.core.error.DocumentExistsException
import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext
import com.couchbase.client.core.transaction.support.SpanWrapper
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.Scope
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.TypeRef
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.internal.await
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryProfile
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.query.QueryScanConsistency
import com.couchbase.client.kotlin.query.internal.CoreQueryOptions
import kotlinx.coroutines.reactive.awaitSingle
import java.util.UUID
import java.util.stream.Collectors

public class TransactionAttemptContext internal constructor(
    private val internal: CoreTransactionAttemptContext,
    private val defaultJsonSerializer: JsonSerializer,
) {
    /**
     * Gets a document from the specified Couchbase [collection] matching the specified [id].
     *
     * @param collection the Couchbase collection containing the document
     * @param id     the ID of the document to get
     * @return a [TransactionGetResult] containing the document
     * @throws DocumentNotFoundException if the document does not exist
     */
    public suspend fun get(collection: Collection, id: String): TransactionGetResult {
        val core = internal.get(collection.collectionId, id).awaitSingle()
        return TransactionGetResult(core, defaultJsonSerializer)
    }

    /**
     * Mutates the specified [doc] with new content.
     *
     * The mutation is staged until the transaction is committed.
     * That is, any read of the document by any Couchbase component
     * will see the document's current value, rather than this staged or 'dirty' data.
     * If the attempt is rolled back, the staged mutation will be removed.
     *
     * This staged data effectively locks the document from other transactional writes
     * until the attempt completes (commits or rolls back).
     *
     * If the mutation fails with a [CasMismatchException] or any other exception,
     * the transaction will automatically roll back this attempt, then retry.
     *
     * @param doc     identifies the document to update
     * @param content the new content for the document.
     * @return the document, updated with its new CAS value.
     */
    public suspend inline fun <reified T> replace(
        doc: TransactionGetResult,
        content: T,
        jsonSerializer: JsonSerializer? = null,
    ): TransactionGetResult {
        return replaceInternal(doc, content, typeRef(), jsonSerializer)
    }

    @PublishedApi
    internal suspend fun <T> replaceInternal(
        doc: TransactionGetResult,
        content: T,
        type: TypeRef<T>,
        jsonSerializer: JsonSerializer?,
    ): TransactionGetResult {
        val span: RequestSpan = CbTracing.newSpan(internal.core().context(), TracingIdentifiers.TRANSACTION_OP_REPLACE, internal.span())

        val encoded = serialize(content, type, jsonSerializer)
        val core = internal.replace(doc.internal, encoded.bytes, encoded.flags, SpanWrapper(span)).awaitSingle()
        return TransactionGetResult(core, defaultJsonSerializer)
    }

    /**
     * Inserts a new document into the specified Couchbase [collection].
     *
     * As with [replace], the insert is staged until the transaction is committed.
     * Due to technical limitations, it is not as possible to completely hide the staged data
     * from the rest of the Couchbase platform, as an empty document must be created.
     *
     * This staged data effectively locks the document from other transactional writes
     * until the attempt completes (commits or rolls back).
     *
     * @param collection the Couchbase collection in which to insert the doc
     * @param id         the document's unique ID
     * @param content    the content to insert.
     * @return the document, updated with its new CAS value and ID, and converted to a [TransactionGetResult]
     * @throws DocumentExistsException if the collection already contains a document with the given ID.
     */
    public suspend inline fun <reified T> insert(
        collection: Collection,
        id: String,
        content: T,
        jsonSerializer: JsonSerializer? = null,
    ): TransactionGetResult {
        return insertInternal(collection, id, content, typeRef(), jsonSerializer)
    }

    @PublishedApi
    internal suspend fun <T> insertInternal(
        collection: Collection,
        id: String,
        content: T,
        type: TypeRef<T>,
        jsonSerializer: JsonSerializer?,
    ): TransactionGetResult {
        val span: RequestSpan = CbTracing.newSpan(internal.core().context(), TracingIdentifiers.TRANSACTION_OP_INSERT, internal.span())

        val encoded = serialize(content, type, jsonSerializer)
        val core = internal.insert(collection.collectionId, id, encoded.bytes, encoded.flags, SpanWrapper(span)).awaitSingle()
        return TransactionGetResult(core, defaultJsonSerializer)
    }

    /**
     * Removes the specified [doc].
     *
     * As with [replace], the remove is staged until the transaction is committed.
     * That is, the document will continue to exist, and the rest of the Couchbase platform will continue to see it.
     *
     * This staged data effectively locks the document from other transactional writes
     * until the attempt completes (commits or rolls back).
     *
     * Note that an overload that takes the document ID as a string is not possible, as it's necessary to check a provided
     * [TransactionGetResult] to determine if the document is involved in another transaction.
     *
     * @param doc the document to remove
     */
    public suspend fun remove(doc: TransactionGetResult) {
        val span: RequestSpan = CbTracing.newSpan(internal.core().context(), TracingIdentifiers.TRANSACTION_OP_REMOVE, internal.span())

        internal.remove(doc.internal, SpanWrapper(span)).await()
    }

    public suspend fun query(
        statement: String,
        parameters: QueryParameters = QueryParameters.None,
        scope: Scope? = null,
        serializer: JsonSerializer? = null,

        consistency: QueryScanConsistency = QueryScanConsistency.notBounded(),
        readonly: Boolean = false,
        adhoc: Boolean = true,
        flexIndex: Boolean = false,

        profile: QueryProfile = QueryProfile.OFF,

        scanCap: Int? = null,
        pipelineBatch: Int? = null,
        pipelineCap: Int? = null,

        clientContextId: String? = UUID.randomUUID().toString(),
        raw: Map<String, Any?> = emptyMap(),
    ): QueryResult {
        require(consistency !is QueryScanConsistency.ConsistentWith) {
            "Query in transaction does not support `QueryScanConsistency.ConsistentWith`."
        }

        val actualSerializer = serializer ?: defaultJsonSerializer

        val common = CommonOptions.Default
        val maxParallelism: Int? = null
        val metrics = true
        val preserveExpiry = false
        val useReplica: Boolean? = null

        val coreQueryOpts = CoreQueryOptions(
            common = common,
            parameters = parameters,
            preserveExpiry = preserveExpiry,
            actualSerializer = actualSerializer,
            consistency = consistency,
            readonly = readonly,
            adhoc = adhoc,
            flexIndex = flexIndex,
            metrics = metrics,
            profile = profile,
            maxParallelism = maxParallelism,
            scanCap = scanCap,
            pipelineBatch = pipelineBatch,
            pipelineCap = pipelineCap,
            clientContextId = clientContextId,
            raw = raw,
            useReplica = useReplica,
        )

        val coreQueryResult: CoreQueryResult = internal.queryBlocking(
            statement,
            scope?.queryContext,
            coreQueryOpts,
            false,
        ).awaitSingle()

        val rows = coreQueryResult.rows()
            .map { QueryRow(it.data(), actualSerializer) }
            .collect(Collectors.toList())

        val metadata = QueryMetadata(coreQueryResult.metaData())

        return QueryResult(rows, metadata)
    }

    private fun <T> serialize(
        content: T,
        type: TypeRef<T>,
        jsonSerializer: JsonSerializer?,
    ): Content {
        if (content is Content) {
            require(content.flags == CodecFlags.JSON_COMPAT_FLAGS || content.flags == CodecFlags.BINARY_COMPAT_FLAGS) {
                "Content in transaction must be flagged as JSON or BINARY, but got ${content.flags}"
            }
            return content
        }
        val jsonBytes = (jsonSerializer ?: defaultJsonSerializer).serialize(content, type)
        return Content.json(jsonBytes)
    }

}
