/*
 * Copyright (c) 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.performer.kotlin.manager

import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.manager.collection.CollectionSpec
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CreateCollectionRequest
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CreateScopeRequest
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.DropCollectionRequest
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.DropScopeRequest
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.GetAllScopesRequest
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.GetAllScopesResult
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.ScopeSpec
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.UpdateCollectionRequest
import kotlin.time.Duration.Companion.seconds
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CollectionSpec as FitCollectionSpec

suspend fun createCollection(bucket: Bucket, request: CreateCollectionRequest, result: Result.Builder) {
    bucket.collections.createCollection(
        scopeName = request.scopeName,
        collectionName = request.name,
        maxExpiry = if (!request.settings.hasExpirySecs()) null else request.settings.expirySecs.seconds,
        // [start:1.2.0]
        history = if (!request.settings.hasHistory()) null else request.settings.history,
        // [end:1.2.0]
    )

    result.success()
}

// [start:1.2.0]
suspend fun updateCollection(bucket: Bucket, request: UpdateCollectionRequest, result: Result.Builder) {
    bucket.collections.updateCollection(
        scopeName = request.scopeName,
        collectionName = request.name,
        // [start:1.2.0]
        maxExpiry = if (!request.settings.hasExpirySecs()) null else request.settings.expirySecs.seconds,
        history = if (!request.settings.hasHistory()) null else request.settings.history,
        // [end:1.2.0]
    )

    result.success()
}
// [end:1.2.0]

suspend fun dropCollection(bucket: Bucket, request: DropCollectionRequest, result: Result.Builder) {
    bucket.collections.dropCollection(
        scopeName = request.scopeName,
        collectionName = request.name,
    )

    result.success()
}

suspend fun createScope(bucket: Bucket, request: CreateScopeRequest, result: Result.Builder) {
    bucket.collections.createScope(
        scopeName = request.name,
    )

    result.success()
}

suspend fun dropScope(bucket: Bucket, request: DropScopeRequest, result: Result.Builder) {
    bucket.collections.dropScope(
        scopeName = request.name,
    )

    result.success()
}

suspend fun getAllScopes(bucket: Bucket, request: GetAllScopesRequest, result: Result.Builder) {
    val allResult = bucket.collections.getAllScopes().map { sdk ->
        ScopeSpec.newBuilder().apply {
            name = sdk.name
            addAllCollections(sdk.collections.map { it.toFit().build() })
        }.build()
    }

    val builder = GetAllScopesResult.newBuilder().addAllResult(allResult)

    result.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setCollectionManagerResult(
                com.couchbase.client.protocol.sdk.bucket.collectionmanager.Result.newBuilder()
                    .setGetAllScopesResult(builder)
            )
    )
}

private fun CollectionSpec.toFit(): FitCollectionSpec.Builder {
    val sdk = this
    return FitCollectionSpec.newBuilder().apply {
        scopeName = sdk.scopeName
        name = sdk.name

        sdk.history?.let { history = it }
        sdk.maxExpiry?.let { expirySecs = it.inWholeSeconds.toInt() }
    }
}

suspend fun handleCollectionManager(
    bucket: Bucket,
    command: com.couchbase.client.protocol.sdk.bucket.collectionmanager.Command,
    result: Result.Builder,
) {
    when {
        command.hasCreateScope() -> createScope(bucket, command.createScope, result)
        command.hasDropScope() -> dropScope(bucket, command.dropScope, result)
        command.hasCreateCollection() -> createCollection(bucket, command.createCollection, result)
        // [start:1.2.0]
        command.hasUpdateCollection() -> updateCollection(bucket, command.updateCollection, result)
        // [end:1.2.0]
        command.hasDropCollection() -> dropCollection(bucket, command.dropCollection, result)
        command.hasGetAllScopes() -> getAllScopes(bucket, command.getAllScopes, result)
        else -> throw UnsupportedOperationException("unsupported")
    }
}

private fun Result.Builder.success() {
    check(!hasSdk()) { "Already set SDK result." }
    setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder().setSuccess(true))
}
