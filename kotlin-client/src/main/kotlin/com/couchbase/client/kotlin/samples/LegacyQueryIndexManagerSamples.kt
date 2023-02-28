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

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Keyspace
import kotlin.time.Duration.Companion.minutes

internal suspend fun createPrimaryIndexOnDefaultCollectionPreCouchbase7(cluster: Cluster) {
    // Create a new primary index on the default collection

    cluster.queryIndexes.createPrimaryIndex(
        keyspace = Keyspace("bucketName")
    )
}

internal suspend fun createPrimaryIndexPreCouchbase7(cluster: Cluster) {
    // Create a new primary index on a collection other than the default

    cluster.queryIndexes.createPrimaryIndex(
        keyspace = Keyspace("bucketName", "scopeName", "collectionName")
    )
}

internal suspend fun getAllIndexesInBucketPreCouchbase7(cluster: Cluster) {
    // Get all indexes in bucket

    val indexes = cluster.queryIndexes.getAllIndexes(
        keyspace = Keyspace("bucketName")
    )
    indexes.forEach { println(it) }
}

internal suspend fun createDeferredIndexesPreCouchbase7(cluster: Cluster) {
    // Create deferred indexes, build them, and wait for them to come online

    val keyspace = Keyspace("bucketName")
    with(cluster.queryIndexes) {
        // create a deferred index
        createPrimaryIndex(keyspace, deferred = true)

        // build all deferred indexes in a keyspace
        buildDeferredIndexes(keyspace)

        // wait for build to complete
        watchIndexes(
            keyspace = keyspace,
            timeout = 1.minutes,
            indexNames = emptySet(),
            includePrimary = true,
        )
    }
}
