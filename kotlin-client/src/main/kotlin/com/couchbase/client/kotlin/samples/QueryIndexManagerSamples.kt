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

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.Collection
import kotlin.time.Duration.Companion.minutes

internal suspend fun createPrimaryIndex(collection: Collection) {
    // Create a new primary index on a collection

    collection.queryIndexes.createPrimaryIndex()
}

internal suspend fun getAllIndexesInBucket(bucket: Bucket) {
    // Get all indexes in bucket

    val allCollections = bucket.collections.getAllScopes()
        .flatMap { it.collections }
        .map { bucket.scope(it.scopeName).collection(it.name) }

    val allIndexesInBucket = allCollections
        .flatMap { it.queryIndexes.getAllIndexes() }

    allIndexesInBucket.forEach { println(it) }
}

internal suspend fun createDeferredIndexes(collection: Collection) {
    // Create deferred indexes, build them, and wait for them to come online

    with(collection.queryIndexes) {
        // create a deferred index
        createPrimaryIndex(deferred = true)

        // build all deferred indexes in a collection
        buildDeferredIndexes()

        // wait for build to complete
        watchIndexes(
            timeout = 1.minutes,
            indexNames = emptySet(),
            includePrimary = true,
        )
    }
}
