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

package com.couchbase.client.performer.kotlin.manager

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Scope
import com.couchbase.client.kotlin.manager.search.ScopeSearchIndexManager
import com.couchbase.client.kotlin.manager.search.SearchIndex
import com.couchbase.client.kotlin.manager.search.SearchIndexManager
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.sdk.search.indexmanager.AllowQuerying
import com.couchbase.client.protocol.sdk.search.indexmanager.AnalyzeDocument
import com.couchbase.client.protocol.sdk.search.indexmanager.AnalyzeDocumentResult
import com.couchbase.client.protocol.sdk.search.indexmanager.DisallowQuerying
import com.couchbase.client.protocol.sdk.search.indexmanager.DropIndex
import com.couchbase.client.protocol.sdk.search.indexmanager.FreezePlan
import com.couchbase.client.protocol.sdk.search.indexmanager.GetAllIndexes
import com.couchbase.client.protocol.sdk.search.indexmanager.GetIndex
import com.couchbase.client.protocol.sdk.search.indexmanager.GetIndexedDocumentsCount
import com.couchbase.client.protocol.sdk.search.indexmanager.PauseIngest
import com.couchbase.client.protocol.sdk.search.indexmanager.ResumeIngest
import com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndexes
import com.couchbase.client.protocol.sdk.search.indexmanager.UnfreezePlan
import com.couchbase.client.protocol.sdk.search.indexmanager.UpsertIndex
import com.google.protobuf.ByteString
import kotlin.text.Charsets.UTF_8
import com.couchbase.client.protocol.run.Result as FitResult
import com.couchbase.client.protocol.sdk.Result as FitSdkResult
import com.couchbase.client.protocol.sdk.search.indexmanager.Result as FitIndexManagerResult
import com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndex as FitSearchIndex

suspend fun handleSearchIndexManager(target: ClusterOrScope, command: Command): FitResult.Builder {
    val sim =
        if (target.cluster != null) command.clusterCommand.searchIndexManager.shared
        else command.scopeCommand.searchIndexManager.shared

    val result = FitResult.newBuilder()

    when {
        sim.hasGetIndex() -> getIndex(target, sim.getIndex, result)
        sim.hasGetAllIndexes() -> getAllIndexes(target, sim.getAllIndexes, result)
        sim.hasUpsertIndex() -> upsertIndex(target, sim.upsertIndex, result)
        sim.hasDropIndex() -> dropIndex(target, sim.dropIndex, result)
        sim.hasGetIndexedDocumentsCount() -> getIndexedDocumentCount(target, sim.getIndexedDocumentsCount, result)
        sim.hasPauseIngest() -> pauseIngest(target, sim.pauseIngest, result)
        sim.hasResumeIngest() -> resumeIngest(target, sim.resumeIngest, result)
        sim.hasFreezePlan() -> freezePlan(target, sim.freezePlan, result)
        sim.hasUnfreezePlan() -> unfreezePlan(target, sim.unfreezePlan, result)
        sim.hasAllowQuerying() -> allowQuerying(target, sim.allowQuerying, result)
        sim.hasDisallowQuerying() -> disallowQuerying(target, sim.disallowQuerying, result)
        sim.hasAnalyzeDocument() -> analyzeDocument(target, sim.analyzeDocument, result)
    }

    if (!result.hasSdk()) result.success()

    return result
}


suspend fun allowQuerying(target: ClusterOrScope, req: AllowQuerying, result: FitResult.Builder) {
    target.exec(
        { allowQuerying(req.indexName) },
        { allowQuerying(req.indexName) },
    )
}

suspend fun disallowQuerying(target: ClusterOrScope, req: DisallowQuerying, result: FitResult.Builder) {
    target.exec(
        { disallowQuerying(req.indexName) },
        { disallowQuerying(req.indexName) },
    )
}

suspend fun freezePlan(target: ClusterOrScope, req: FreezePlan, result: FitResult.Builder) {
    target.exec(
        { freezePlan(req.indexName) },
        { freezePlan(req.indexName) },
    )
}

suspend fun unfreezePlan(target: ClusterOrScope, req: UnfreezePlan, result: FitResult.Builder) {
    target.exec(
        { unfreezePlan(req.indexName) },
        { unfreezePlan(req.indexName) },
    )
}

suspend fun pauseIngest(target: ClusterOrScope, req: PauseIngest, result: FitResult.Builder) {
    target.exec(
        { pauseIngest(req.indexName) },
        { pauseIngest(req.indexName) },
    )
}

suspend fun resumeIngest(target: ClusterOrScope, req: ResumeIngest, result: FitResult.Builder) {
    target.exec(
        { resumeIngest(req.indexName) },
        { resumeIngest(req.indexName) },
    )
}

suspend fun getIndexedDocumentCount(target: ClusterOrScope, req: GetIndexedDocumentsCount, result: FitResult.Builder) {
    val count = target.exec(
        { getIndexedDocumentsCount(req.indexName) },
        { getIndexedDocumentsCount(req.indexName) }
    )

    result.success {
        indexedDocumentCounts = count
            .toInt() // FIT bug, should be long.
    }
}


data class ClusterOrScope(
    val cluster: Cluster? = null,
    val scope: Scope? = null,
) {
    init {
        require(cluster != null || scope != null)
        require(cluster == null || scope == null)
    }

    suspend fun <R> exec(
        clusterBlock: suspend SearchIndexManager.() -> R,
        scopeBlock: suspend ScopeSearchIndexManager.() -> R,
    ) = if (cluster != null) cluster.searchIndexes.clusterBlock() else scope!!.searchIndexes.scopeBlock()
}

suspend fun upsertIndex(target: ClusterOrScope, req: UpsertIndex, result: FitResult.Builder) {
    val index = SearchIndex.fromJson(req.indexDefinition.toString(UTF_8))
    target.exec(
        { upsertIndex(index) },
        { upsertIndex(index) },
    )
}

suspend fun dropIndex(target: ClusterOrScope, req: DropIndex, result: FitResult.Builder) {
    target.exec(
        { dropIndex(req.indexName) },
        { dropIndex(req.indexName) },
    )
}

suspend fun getAllIndexes(target: ClusterOrScope, req: GetAllIndexes, result: FitResult.Builder) {
    val sdkResult = target.exec(
        { getAllIndexes() },
        { getAllIndexes() },
    )

    result.success {
        setIndexes(
            SearchIndexes.newBuilder()
                .addAllIndexes(sdkResult.map { it.toFit() })
        )
    }
}

suspend fun getIndex(target: ClusterOrScope, req: GetIndex, result: FitResult.Builder) {
    val sdkResult = target.exec(
        { getIndex(req.indexName) },
        { getIndex(req.indexName) },
    )

    result.success { index = sdkResult.toFit() }
}

suspend fun analyzeDocument(target: ClusterOrScope, req: AnalyzeDocument, result: FitResult.Builder) {
    val sdkResult = target.exec(
        { analyzeDocument(req.indexName, req.document.toByteArray()) },
        { analyzeDocument(req.indexName, req.document.toByteArray()) },
    )

    val jsonArray = Mapper.decodeIntoTree(sdkResult) as ArrayNode
    val listOfByteArrays = jsonArray.map { Mapper.encodeAsBytes(it).toByteString() }

    result.success {
        setAnalyzeDocument(AnalyzeDocumentResult.newBuilder().addAllResults(listOfByteArrays))
    }
}

private suspend fun FitResult.Builder.success(block: suspend FitIndexManagerResult.Builder.() -> Unit) {
    val r = FitIndexManagerResult.newBuilder()
    r.block()
    setSdk(FitSdkResult.newBuilder().setSearchIndexManagerResult(r))
}

private fun FitResult.Builder.success() {
    setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder().setSuccess(true))
}

internal fun ByteArray.toByteString(): ByteString = ByteString.copyFrom(this)
internal fun ByteString.decodeJsonToMap(): Map<String, Any?> = Mapper.decodeInto(this.toByteArray(), object : TypeReference<Map<String, Any?>>() {})

private fun SearchIndex.toFit(): FitSearchIndex {
    val sdk = this
    return FitSearchIndex.newBuilder().apply {
        name = sdk.name
//        sourceName = sdk.sourceName
        uuid = sdk.uuid
        type = sdk.type
        params = Mapper.encodeAsBytes(sdk.params).toByteString()
        sourceUuid = sdk.sourceUuid
        sourceParams = Mapper.encodeAsBytes(sdk.sourceParams).toByteString()
        sourceType = sdk.sourceType
        planParams = Mapper.encodeAsBytes(sdk.planParams).toByteString()

    }.build()
}

private fun FitSearchIndex.toSdk(): SearchIndex {
    return SearchIndex(
        name = this.name,
        sourceName = "",
        uuid = this.uuid,
        type = this.type,
        params = this.params.decodeJsonToMap(),
        sourceUuid = this.sourceUuid,
        sourceParams = this.sourceParams.decodeJsonToMap(),
        sourceType = this.sourceType,
        planParams = this.planParams.decodeJsonToMap()
    )
}
