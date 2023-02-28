/*
 * Copyright 2023 Couchbase, Inc.
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

import com.couchbase.client.core.api.manager.CoreBuildQueryIndexOptions
import com.couchbase.client.core.api.manager.CoreCreatePrimaryQueryIndexOptions
import com.couchbase.client.core.api.manager.CoreCreateQueryIndexOptions
import com.couchbase.client.core.api.manager.CoreDropPrimaryQueryIndexOptions
import com.couchbase.client.core.api.manager.CoreDropQueryIndexOptions
import com.couchbase.client.core.api.manager.CoreGetAllQueryIndexesOptions
import com.couchbase.client.core.api.manager.CoreWatchQueryIndexesOptions
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.IndexFailureException
import com.couchbase.client.core.error.IndexNotFoundException
import com.couchbase.client.core.manager.CoreCollectionQueryIndexManager
import com.couchbase.client.kotlin.CommonOptions
import kotlinx.coroutines.future.await
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * @sample com.couchbase.client.kotlin.samples.createPrimaryIndex
 * @sample com.couchbase.client.kotlin.samples.getAllIndexesInBucket
 * @sample com.couchbase.client.kotlin.samples.createDeferredIndexes
 */
public class CollectionQueryIndexManager internal constructor(
    private val core: CoreCollectionQueryIndexManager,
) {

    /**
     * Creates a primary index (an index on all keys in the collection).
     */
    public suspend fun createPrimaryIndex(
        common: CommonOptions = CommonOptions.Default,
        indexName: String? = null,
        ignoreIfExists: Boolean = false,
        deferred: Boolean = false,
        numReplicas: Int? = null,
        with: Map<String, Any?> = emptyMap(),
    ) {
        core.createPrimaryIndex(object : CoreCreatePrimaryQueryIndexOptions {
            override fun ignoreIfExists() = ignoreIfExists
            override fun numReplicas() = numReplicas
            override fun deferred() = deferred
            override fun with() = with
            override fun scopeAndCollection() = null
            override fun commonOptions() = common.toCore()
            override fun indexName() = indexName
        }).await()
    }

    /**
     * Drops an anonymous primary index on this collection.
     *
     * To drop a named primary index, use [dropIndex] instead.
     *
     * @throws IndexNotFoundException if the index does not exist.
     * @throws IndexFailureException if dropping the index failed (see reason for details).
     * @throws CouchbaseException if any other generic unhandled/unexpected errors.
     */
    public suspend fun dropPrimaryIndex(
        common: CommonOptions = CommonOptions.Default,
        ignoreIfNotExists: Boolean = false,
    ) {
        core.dropPrimaryIndex(object : CoreDropPrimaryQueryIndexOptions {
            override fun ignoreIfNotExists() = ignoreIfNotExists
            override fun scopeAndCollection() = null
            override fun commonOptions() = common.toCore()
        }).await()
    }

    /**
     * Drops the primary or secondary index named [indexName] on this collection.
     *
     * @throws IndexNotFoundException if the index does not exist.
     * @throws IndexFailureException if dropping the index failed (see reason for details).
     * @throws CouchbaseException if any other generic unhandled/unexpected errors.
     */
    public suspend fun dropIndex(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
        ignoreIfNotExists: Boolean = false,
    ) {
        core.dropIndex(indexName, object : CoreDropQueryIndexOptions {
            override fun ignoreIfNotExists() = ignoreIfNotExists
            override fun scopeAndCollection() = null
            override fun commonOptions() = common.toCore()
        }).await()
    }

    /**
     * Creates a secondary index.
     */
    public suspend fun createIndex(
        indexName: String,
        fields: Collection<String>,
        common: CommonOptions = CommonOptions.Default,
        ignoreIfExists: Boolean = false,
        deferred: Boolean = false,
        numReplicas: Int? = null,
        with: Map<String, Any?> = emptyMap(),
    ) {
        core.createIndex(indexName, fields, object : CoreCreateQueryIndexOptions {
            override fun ignoreIfExists() = ignoreIfExists
            override fun numReplicas() = numReplicas
            override fun deferred() = deferred
            override fun with() = with
            override fun scopeAndCollection() = null
            override fun commonOptions() = common.toCore()
        }).await()
    }

    public suspend fun getAllIndexes(
        common: CommonOptions = CommonOptions.Default,
    ): List<QueryIndex> {
        return core.getAllIndexes(object : CoreGetAllQueryIndexesOptions {
            override fun scopeName() = null
            override fun collectionName() = null
            override fun commonOptions() = common.toCore()
        }).await().map { QueryIndex(it) }
    }

    /**
     * Builds any deferred indexes in this collection.
     */
    public suspend fun buildDeferredIndexes(
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.buildDeferredIndexes(object : CoreBuildQueryIndexOptions {
            override fun scopeAndCollection() = null
            override fun commonOptions() = common.toCore()
        }).await()
    }

    /**
     * Returns when the indexes named [indexNames] in this collection are online.
     */
    public suspend fun watchIndexes(
        indexNames: Collection<String>,
        includePrimary: Boolean = false,
        timeout: Duration,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.watchIndexes(indexNames, timeout.toJavaDuration(), object : CoreWatchQueryIndexesOptions {
            override fun watchPrimary() = includePrimary
            override fun scopeAndCollection() = null
            override fun commonOptions() = common.toCore()
        }).await()
    }
}
