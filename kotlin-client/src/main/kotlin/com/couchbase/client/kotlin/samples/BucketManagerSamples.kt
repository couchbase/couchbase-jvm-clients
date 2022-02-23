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

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.manager.bucket.BucketSettings
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes

@Suppress("UNUSED_VARIABLE")
internal suspend fun createBucket(cluster: Cluster) {
    // Create a new bucket
    cluster.buckets.createBucket(BucketSettings(
        name = "my-bucket",
        ramQuota = 256.mebibytes,
    ))
}

@Suppress("UNUSED_VARIABLE")
internal suspend fun updateBucket(cluster: Cluster) {
    // Modify the RAM quota of an existing bucket
    val newSettings = cluster.buckets.getBucket("my-bucket")
        .copy(ramQuota = 1024.mebibytes)
    cluster.buckets.updateBucket(newSettings)
}
