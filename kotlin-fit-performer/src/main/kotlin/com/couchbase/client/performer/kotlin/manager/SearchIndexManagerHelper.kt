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
import com.couchbase.client.kotlin.manager.search.SearchIndex
import com.couchbase.client.protocol.run.Result
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
import com.couchbase.client.protocol.sdk.Result as FitSdkResult
import com.couchbase.client.protocol.sdk.search.indexmanager.Result as FitIndexManagerResult
import com.couchbase.client.protocol.sdk.search.indexmanager.SearchIndex as FitSearchIndex

suspend fun handleSearchIndexManager(cluster: Cluster, command: Command, result: Result.Builder) {
    if (!command.clusterCommand.searchIndexManager.hasShared()) TODO()
    val sim = command.clusterCommand.searchIndexManager.shared

    when {
        sim.hasGetIndex() -> getIndex(cluster, sim.getIndex, result)
        sim.hasGetAllIndexes() -> getAllIndexes(cluster, sim.getAllIndexes, result)
        sim.hasUpsertIndex() -> upsertIndex(cluster, sim.upsertIndex, result)
        sim.hasDropIndex() -> dropIndex(cluster, sim.dropIndex, result)
        sim.hasGetIndexedDocumentsCount() -> getIndexedDocumentCount(cluster, sim.getIndexedDocumentsCount, result)
        sim.hasPauseIngest() -> pauseIngest(cluster, sim.pauseIngest, result)
        sim.hasResumeIngest() -> resumeIngest(cluster, sim.resumeIngest, result)
        sim.hasFreezePlan() -> freezePlan(cluster, sim.freezePlan, result)
        sim.hasUnfreezePlan() -> unfreezePlan(cluster, sim.unfreezePlan, result)
        sim.hasAllowQuerying() -> allowQuerying(cluster, sim.allowQuerying, result)
        sim.hasDisallowQuerying() -> disallowQuerying(cluster, sim.disallowQuerying, result)
        sim.hasAnalyzeDocument() -> analyzeDocument(cluster, sim.analyzeDocument, result)
    }
}


suspend fun allowQuerying(cluster: Cluster, req: AllowQuerying, result: Result.Builder) {
    cluster.searchIndexes.allowQuerying(req.indexName)
    result.success()
}

suspend fun disallowQuerying(cluster: Cluster, req: DisallowQuerying, result: Result.Builder) {
    cluster.searchIndexes.disallowQuerying(req.indexName)
    result.success()
}

suspend fun freezePlan(cluster: Cluster, req: FreezePlan, result: Result.Builder) {
    cluster.searchIndexes.freezePlan(req.indexName)
    result.success()
}

suspend fun unfreezePlan(cluster: Cluster, req: UnfreezePlan, result: Result.Builder) {
    cluster.searchIndexes.unfreezePlan(req.indexName)
    result.success()
}

suspend fun pauseIngest(cluster: Cluster, req: PauseIngest, result: Result.Builder) {
    cluster.searchIndexes.pauseIngest(req.indexName)
    result.success()
}

suspend fun resumeIngest(cluster: Cluster, req: ResumeIngest, result: Result.Builder) {
    cluster.searchIndexes.resumeIngest(req.indexName)
    result.success()
}

suspend fun getIndexedDocumentCount(cluster: Cluster, req: GetIndexedDocumentsCount, result: Result.Builder) {
    result.success {
        indexedDocumentCounts = cluster.searchIndexes.getIndexedDocumentsCount(req.indexName)
            .toInt() // FIT bug, should be long.
    }
}

suspend fun upsertIndex(cluster: Cluster, req: UpsertIndex, result: Result.Builder) {
    cluster.searchIndexes.upsertIndex(SearchIndex.fromJson(req.indexDefinition.toString(UTF_8)))
    result.success()
}

suspend fun dropIndex(cluster: Cluster, req: DropIndex, result: Result.Builder) {
    cluster.searchIndexes.dropIndex(req.indexName)
    result.success()
}

suspend fun getAllIndexes(cluster: Cluster, req: GetAllIndexes, result: Result.Builder) {
    val sdkResult = cluster.searchIndexes.getAllIndexes()
    result.success {
        setIndexes(
            SearchIndexes.newBuilder()
                .addAllIndexes(sdkResult.map { it.toFit() })
        )
    }
}

suspend fun getIndex(cluster: Cluster, req: GetIndex, result: Result.Builder) {
    val sdkResult = cluster.searchIndexes.getIndex(req.indexName)
    result.success { index = sdkResult.toFit() }
}

suspend fun analyzeDocument(cluster: Cluster, req: AnalyzeDocument, result: Result.Builder) {
    val sdkResult = cluster.searchIndexes.analyzeDocument(req.indexName, req.document.toByteArray())
    val jsonArray = Mapper.decodeIntoTree(sdkResult) as ArrayNode
    val listOfByteArrays = jsonArray.map { Mapper.encodeAsBytes(it).toByteString() }

    result.success {
        setAnalyzeDocument(AnalyzeDocumentResult.newBuilder().addAllResults(listOfByteArrays))
    }
}

private suspend fun Result.Builder.success(block: suspend FitIndexManagerResult.Builder.() -> Unit) {
    val r = FitIndexManagerResult.newBuilder()
    r.block()
    setSdk(FitSdkResult.newBuilder().setSearchIndexManagerResult(r))
}

private fun Result.Builder.success() {
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
