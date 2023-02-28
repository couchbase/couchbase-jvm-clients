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

package com.couchbase.client.kotlin.manager.query

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.IndexExistsException
import com.couchbase.client.core.error.IndexFailureException
import com.couchbase.client.core.error.IndexNotFoundException
import com.couchbase.client.core.error.IndexesNotReadyException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.logging.RedactableArgument.redactMeta
import com.couchbase.client.core.manager.CoreQueryIndexManager
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.internal.RetryTimeoutException
import com.couchbase.client.kotlin.internal.findCause
import com.couchbase.client.kotlin.internal.hasCause
import com.couchbase.client.kotlin.internal.putIfNotNull
import com.couchbase.client.kotlin.internal.putIfTrue
import com.couchbase.client.kotlin.internal.retry
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.execute
import kotlin.time.Duration

/**
 * @sample com.couchbase.client.kotlin.samples.createPrimaryIndexOnDefaultCollectionPreCouchbase7
 * @sample com.couchbase.client.kotlin.samples.createPrimaryIndexPreCouchbase7
 * @sample com.couchbase.client.kotlin.samples.getAllIndexesInBucketPreCouchbase7
 * @sample com.couchbase.client.kotlin.samples.createDeferredIndexesPreCouchbase7
 */
public class QueryIndexManager internal constructor(
    private val cluster: Cluster,
) {

    /**
     * Creates a primary index (an index on all keys in the keyspace).
     */
    public suspend fun createPrimaryIndex(
        keyspace: Keyspace,
        common: CommonOptions = CommonOptions.Default,
        indexName: String? = null,
        ignoreIfExists: Boolean = false,
        deferred: Boolean = false,
        numReplicas: Int? = null,
        with: Map<String, Any?> = emptyMap(),
    ) {
        require((numReplicas ?: 0) >= 0) { "Number of replicas must be >= 0, but got $numReplicas" }
        val actualWith = with.toMutableMap()
        actualWith.putIfTrue("defer_build", deferred)
        actualWith.putIfNotNull("num_replicas", numReplicas)

        val statement = StringBuilder("CREATE PRIMARY INDEX ${indexName?.quote() ?: ""} ON ${keyspace.quote()}")
        doCreate(statement, actualWith, ignoreIfExists, common)
    }

    /**
     * Drops an anonymous primary index on [keyspace].
     *
     * To drop a named primary index, use [dropIndex] instead.
     *
     * @throws IndexNotFoundException if the index does not exist.
     * @throws IndexFailureException if dropping the index failed (see reason for details).
     * @throws CouchbaseException if any other generic unhandled/unexpected errors.
     */
    public suspend fun dropPrimaryIndex(
        keyspace: Keyspace,
        common: CommonOptions = CommonOptions.Default,
        ignoreIfNotExists: Boolean = false,
    ) {
        val statement = "DROP PRIMARY INDEX ON ${keyspace.quote()}"
        doDrop(statement, ignoreIfNotExists, common)
    }

    /**
     * Drops the primary or secondary index named [indexName] on [keyspace].
     *
     * @throws IndexNotFoundException if the index does not exist.
     * @throws IndexFailureException if dropping the index failed (see reason for details).
     * @throws CouchbaseException if any other generic unhandled/unexpected errors.
     */
    public suspend fun dropIndex(
        keyspace: Keyspace,
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
        ignoreIfNotExists: Boolean = false,
    ) {
        val statement = if (keyspace.isDefaultCollection()) "DROP INDEX " + quote(keyspace.bucket, indexName)
        else "DROP INDEX ${indexName.quote()} ON ${keyspace.quote()}"

        doDrop(statement, ignoreIfNotExists, common)
    }

    /**
     * Creates a secondary index.
     */
    public suspend fun createIndex(
        keyspace: Keyspace,
        indexName: String,
        fields: Collection<String>,
        common: CommonOptions = CommonOptions.Default,
        ignoreIfExists: Boolean = false,
        deferred: Boolean = false,
        numReplicas: Int? = null,
        with: Map<String, Any?> = emptyMap(),
    ) {
        require((numReplicas ?: 0) >= 0) { "Number of replicas must be >= 0, but got $numReplicas" }
        require(fields.isNotEmpty()) { "Must specify at least one field to index." }

        val actualWith = with.toMutableMap()
        actualWith.putIfTrue("defer_build", deferred)
        actualWith.putIfNotNull("num_replicas", numReplicas)

        val formattedFields = "(" + fields.joinToString(",") + ")" // really don't quote
        val statement = StringBuilder("CREATE INDEX ${indexName.quote()} ON ${keyspace.quote()}$formattedFields")
        doCreate(statement, actualWith, ignoreIfExists, common)
    }

    private suspend fun doCreate(
        statement: StringBuilder,
        with: Map<String, Any?>,
        ignoreIfExists: Boolean,
        common: CommonOptions,
    ) {
        if (with.isNotEmpty()) statement.append(" WITH ").append(Mapper.encodeAsString(with))

        try {
            cluster.query(
                statement = statement.toString(),
                common = common,
            ).execute()
        } catch (t: Throwable) {
            if (!ignoreIfExists || !t.hasCause<IndexExistsException>()) throw t
        }
    }

    private suspend fun doDrop(
        statement: String,
        ignoreIfNotExists: Boolean = false,
        common: CommonOptions = CommonOptions.Default,
    ) {
        try {
            cluster.query(
                statement = statement,
                common = common,
            ).execute()
        } catch (t: Throwable) {
            if (!ignoreIfNotExists || !t.hasCause<IndexNotFoundException>()) throw t
        }
    }

    /**
     * Returns all indexes from the collection identified by [keyspace].
     *
     * NOTE: To get all indexes in a scope or bucket, use the overload
     * with `bucket`, `scope`, and `collection` parameters.
     */
    public suspend fun getAllIndexes(
        keyspace: Keyspace,
        common: CommonOptions = CommonOptions.Default,
    ): List<QueryIndex> = getAllIndexes(
        bucket = keyspace.bucket,
        scope = keyspace.scope,
        collection = keyspace.collection,
        common = common,
    )

    /**
     * Returns all indexes from a bucket, scope, or collection.
     *
     * If [scope] and [collection] are null, returns all indexes in the bucket.
     * If [scope] is non-null and [collection] is null, returns all indexes in the scope.
     * If [scope] and [collection] are both non-null, returns all indexes in the collection.
     *
     * It is an error to specify a non-null [collection] and a null [scope].
     */
    public suspend fun getAllIndexes(
        bucket: String,
        @SinceCouchbase("7.0") scope: String? = null,
        @SinceCouchbase("7.0") collection: String? = null,
        common: CommonOptions = CommonOptions.Default,
    ): List<QueryIndex> {
        return cluster.query(
            statement = CoreQueryIndexManager.getStatementForGetAllIndexes(bucket, scope, collection),
            parameters = QueryParameters.named(CoreQueryIndexManager.getNamedParamsForGetAllIndexes(bucket, scope, collection)),
            readonly = true,
            common = common,
        ).execute().rows.map { QueryIndex.parse(it.content) }
    }

    /**
     * Builds any deferred indexes in [keyspace].
     */
    public suspend fun buildDeferredIndexes(
        keyspace: Keyspace,
        common: CommonOptions = CommonOptions.Default,
    ) {
        val indexNames = getAllIndexes(keyspace, common)
            .filter { it.state == "deferred" }
            .map { it.name }

        if (indexNames.isEmpty()) return

        val statement = "BUILD INDEX ON ${keyspace.quote()} (${indexNames.joinToString(",") { it.quote() }})"

        cluster.query(
            statement = statement,
            common = common,
        ).execute()
    }

    /**
     * Returns when the indexes named [indexNames] in [keyspace] are online.
     */
    public suspend fun watchIndexes(
        keyspace: Keyspace,
        indexNames: Collection<String>,
        includePrimary: Boolean = false,
        timeout: Duration,
        common: CommonOptions = CommonOptions.Default,
    ) {
        try {
            val indexNameSet = indexNames.toSet()
            retry(timeout = timeout, onlyIf = { it is IndexesNotReadyException }) {
                failIfIndexesOffline(keyspace, indexNameSet, includePrimary, common)
            }
        } catch (t: RetryTimeoutException) {
            val msg = StringBuilder("A requested index is still not ready after $timeout.")
            t.lastError.findCause<IndexesNotReadyException>()?.let { notReady ->
                msg.append(" Unready index keyspace/name -> state: ")
                    .append(redactMeta(notReady.indexNameToState()))
            }

            throw UnambiguousTimeoutException(msg.toString(), null)
        }
    }

    /**
     * @throws IndexesNotReadyException if the lookup returned one or more indexes not in the "online" state.
     * @throws IndexNotFoundException if [includePrimary] is true and the lookup did not return at least one primary index
     */
    private suspend fun failIfIndexesOffline(
        keyspace: Keyspace,
        indexNames: Set<String>,
        includePrimary: Boolean = false,
        common: CommonOptions = CommonOptions.Default,
    ) {
        val indexes = getAllIndexes(keyspace, common)
            .filter { it.name in indexNames || (includePrimary && it.primary) }

        if (includePrimary && !indexes.any { it.primary }) throw IndexNotFoundException("#primary")

        val missingIndexNames = indexNames - indexes.map { it.name }.toSet()
        if (missingIndexNames.isNotEmpty()) throw IndexNotFoundException(missingIndexNames.toString())

        val offlineIndexNameToState: Map<String, String> = indexes
            .filter { it.state != "online" }
            .associateBy({ it.keyspace.format() + "/" + it.name }, QueryIndex::state)

        if (offlineIndexNameToState.isNotEmpty()) {
            throw IndexesNotReadyException(offlineIndexNameToState)
        }
    }
}

private fun quote(vararg components: String) =
    components.joinToString(".") { it.quote() }

private fun String.quote() =
    if (contains("`")) throw InvalidArgumentException.fromMessage("Value [" + redactMeta(this) + "] may not contain backticks.")
    else "`$this`"

private fun Keyspace.quote() =
    // omit default scope & collection in case we're talking to a pre-7.0 server
    if (scope == DEFAULT_SCOPE && collection == DEFAULT_COLLECTION) quote(bucket)
    else quote(bucket, scope, collection)


private fun Keyspace.isDefaultCollection() = scope == DEFAULT_SCOPE && collection == DEFAULT_COLLECTION
