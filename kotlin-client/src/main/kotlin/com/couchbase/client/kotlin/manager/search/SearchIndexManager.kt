/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.kotlin.manager.search

import com.couchbase.client.core.api.manager.search.CoreSearchIndex
import com.couchbase.client.core.api.manager.search.CoreSearchIndexManager
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.error.IndexExistsException
import com.couchbase.client.core.error.IndexNotFoundException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.UncommittedCouchbaseApi
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import kotlinx.coroutines.future.await

@UncommittedCouchbaseApi
public class SearchIndexManager internal constructor(
    private val core: CoreSearchIndexManager
) {
    /**
     * @throws IndexNotFoundException
     */
    public suspend fun getIndex(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ): SearchIndex = SearchIndex(core.getIndex(indexName, common.toCore()).await())

    public suspend fun getAllIndexes(
        common: CommonOptions = CommonOptions.Default,
    ): List<SearchIndex> = core.getAllIndexes(common.toCore()).await().map { SearchIndex(it) }

    /**
     * @throws IndexExistsException if the given index's [SearchIndex.uuid] field is null
     * and there is an existing index with the same [SearchIndex.name].
     */
    public suspend fun upsertIndex(
        index: SearchIndex,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.upsertIndex(index.toCore(), common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun dropIndex(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.dropIndex(indexName, common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun getIndexedDocumentsCount(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ): Long = core.getIndexedDocumentsCount(indexName, common.toCore()).await()

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun allowQuerying(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.allowQuerying(indexName, common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun disallowQuerying(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.disallowQuerying(indexName, common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun freezePlan(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.freezePlan(indexName, common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun unfreezePlan(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.unfreezePlan(indexName, common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun pauseIngest(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.pauseIngest(indexName, common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     */
    public suspend fun resumeIngest(
        indexName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        core.resumeIngest(indexName, common.toCore()).await()
    }

    /**
     * @throws IndexNotFoundException
     * @return analysis results encoded as a JSON Array
     */
    @VolatileCouchbaseApi
    public suspend fun analyzeDocument(
        indexName: String,
        documentJsonBytes: ByteArray,
        common: CommonOptions = CommonOptions.Default,
    ): ByteArray {
        val coreResult: List<ObjectNode> = core.analyzeDocument(
            indexName,
            Mapper.decodeIntoTree(documentJsonBytes) as ObjectNode,
            common.toCore(),
        ).await()

        val arrayNode = Mapper.convertValue(coreResult, ArrayNode::class.java)
        return Mapper.encodeAsBytes(arrayNode)
    }
}

internal fun SearchIndex.toCore() = CoreSearchIndex(
    uuid,
    name,
    type,
    params,
    sourceUuid,
    sourceName,
    sourceParams,
    sourceType,
    planParams,
)
