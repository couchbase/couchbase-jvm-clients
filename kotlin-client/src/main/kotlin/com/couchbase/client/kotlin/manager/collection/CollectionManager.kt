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

package com.couchbase.client.kotlin.manager.collection

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.error.CollectionExistsException
import com.couchbase.client.core.error.CollectionNotFoundException
import com.couchbase.client.core.error.ScopeExistsException
import com.couchbase.client.core.error.ScopeNotFoundException
import com.couchbase.client.core.manager.CoreCollectionManager
import com.couchbase.client.core.manager.collection.CoreCreateOrUpdateCollectionSettings
import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.UncommittedCouchbaseApi
import kotlinx.coroutines.future.await
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Manager for a bucket's scopes and collections.
 *
 * Access via [Bucket.collections].
 *
 * @sample com.couchbase.client.kotlin.samples.createScopeAndCollections
 * @sample com.couchbase.client.kotlin.samples.copyScope
 */
public class CollectionManager internal constructor(bucket: Bucket) {
    private val core: CoreCollectionManager = bucket.couchbaseOps.collectionManager(bucket.name);

    /**
     * Creates a collection in an existing scope. Equivalent to:
     * ```
     * createCollection(CollectionSpec(scopeName, collectionName, maxExpiry))
     * ```
     *
     * @param scopeName Name of the parent scope. This scope must already exist.
     * @param collectionName Name of the collection to create.
     * @param maxExpiry Maximum expiry for documents in the collection.
     * [Duration.ZERO] (or null) means the collection's max expiry is always the same as the bucket's max expiry.
     * If using Couchbase Server 7.6 or later, [CollectionSpec.NEVER_EXPIRE] (or -1 seconds)
     * means documents in the collection never expire, regardless of the bucket's max expiry.
     *
     * @throws ScopeNotFoundException if the parent scope does not exist.
     * @throws CollectionExistsException if there is already a collection with the same name in the parent scope.
     *
     * @sample com.couchbase.client.kotlin.samples.createScopeAndCollections
     */
    public suspend fun createCollection(
        scopeName: String,
        collectionName: String,
        common: CommonOptions = CommonOptions.Default,
        maxExpiry: Duration? = null,
        @SinceCouchbase("7.2") history: Boolean? = null,
    ) {
        core.createCollection(scopeName, collectionName, object : CoreCreateOrUpdateCollectionSettings {
            override fun maxExpiry(): java.time.Duration? = maxExpiry?.toJavaDuration()
            override fun history(): Boolean? = history
        }, common.toCore()).await()
    }

    /**
     * Creates a collection in an existing scope.
     *
     * @param collection Specifies the collection to create. The parent scope must already exist.
     *
     * @throws ScopeNotFoundException if the parent scope does not exist.
     * @throws CollectionExistsException if there is already a collection with the same name in the parent scope.
     */
    public suspend fun createCollection(
        collection: CollectionSpec,
        common: CommonOptions = CommonOptions.Default,
    ) {
        with(collection) {
            createCollection(
                scopeName = scopeName,
                collectionName = name,
                common = common,
                maxExpiry = maxExpiry,
                history = history,
            )
        }
    }

    /**
     * Modifies an existing collection.
     *
     * @param scopeName Name of the parent scope. This scope must already exist.
     * @param collectionName Name of the collection to update. This collection must already exist.
     * @param maxExpiry New value for the maximum expiry for documents in the collection.
     * Pass null to leave unmodified.
     * Pass [Duration.ZERO] to have the collection's max expiry always be the same as the bucket's max expiry.
     * If using Couchbase Server 7.6 or later, pass [CollectionSpec.NEVER_EXPIRE] (or -1 seconds)
     * to have documents in the collection never expire, regardless of the bucket's max expiry.
     * @param history New value for history preservation for this collection, or null to leave unmodified.
     *
     * @throws ScopeNotFoundException if the parent scope does not exist.
     * @throws CollectionNotFoundException if the collection does not exist.
     */
    @SinceCouchbase("7.2")
    public suspend fun updateCollection(
        scopeName: String,
        collectionName: String,
        common: CommonOptions = CommonOptions.Default,
        @SinceCouchbase("7.6") maxExpiry: Duration? = null,
        history: Boolean? = null,
    ) {
        core.updateCollection(scopeName, collectionName, object : CoreCreateOrUpdateCollectionSettings {
            override fun maxExpiry(): java.time.Duration? = maxExpiry?.toJavaDuration()
            override fun history(): Boolean? = history
        }, common.toCore()).await()
    }

    /**
     * Deletes a collection from a scope.
     *
     * **WARNING:** All documents and indexes in the collection will be lost.
     *
     * @param scopeName Name of the collection's parent scope.
     * @param collectionName Name of the collection to drop.
     *
     * @throws ScopeNotFoundException if the parent scope does not exist.
     * @throws CollectionNotFoundException if there is no collection with this name in the parent scope.
     */
    public suspend fun dropCollection(
        scopeName: String,
        collectionName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.dropCollection(scopeName, collectionName, common.toCore()).await()
    }

    /**
     * Deletes a collection from a scope. Equivalent to:
     * ```
     * dropCollection(collection.scopeName, collection.name)
     * ```
     *
     * **WARNING:** All documents and indexes in the collection will be lost.
     *
     * @param collection Specifies the collection to drop.
     *
     * @throws ScopeNotFoundException if the parent scope does not exist.
     * @throws CollectionNotFoundException if there is no collection with this name in the parent scope.
     */
    public suspend fun dropCollection(
        collection: CollectionSpec,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.dropCollection(collection.scopeName, collection.name, common.toCore()).await()
    }

    /**
     * Creates a scope in the bucket.
     *
     * @param scopeName Name of the scope to create.
     *
     * @throws ScopeExistsException if the bucket already has a scope with this name.
     *
     * @sample com.couchbase.client.kotlin.samples.createScopeAndCollections
     */
    public suspend fun createScope(
        scopeName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.createScope(scopeName, common.toCore()).await()
    }

    /**
     * Deletes a scope from the bucket.
     *
     * **WARNING:** All documents (and collections & indexes) in the scope will be lost.
     *
     * @param scopeName Name of the scope to drop.
     *
     * @throws ScopeNotFoundException if the bucket does not have a scope with this name.
     */
    public suspend fun dropScope(
        scopeName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.dropScope(scopeName, common.toCore()).await()
    }

    /**
     * Returns information about a scope and its collections.
     *
     * @param scopeName Name of the scope to inspect.
     *
     * @throws ScopeNotFoundException if the bucket does not have a scope with this name.
     *
     * @sample com.couchbase.client.kotlin.samples.copyScope
     */
    @UncommittedCouchbaseApi
    public suspend fun getScope(
        scopeName: String,
        common: CommonOptions = CommonOptions.Default,
    ): ScopeSpec = getAllScopes(common).find { it.name == scopeName } ?: throw ScopeNotFoundException(scopeName, null)

    /**
     * Returns information about all scopes (and their collections) in the bucket.
     */
    public suspend fun getAllScopes(
        common: CommonOptions = CommonOptions.Default,
    ): List<ScopeSpec> {
        val manifest = core.getAllScopes(common.toCore()).await()
        val result = manifest.scopes().map { scope ->
            ScopeSpec(
                name = scope.name(),
                collections = scope.collections().map { collection ->
                    CollectionSpec(
                        scopeName = scope.name(),
                        name = collection.name(),
                        maxExpiry = if (collection.maxExpiry() == null || collection.maxExpiry() == 0) null else collection.maxExpiry().seconds,
                        history = collection.history()
                    )
                },
            )
        }
        return result
    }
}
