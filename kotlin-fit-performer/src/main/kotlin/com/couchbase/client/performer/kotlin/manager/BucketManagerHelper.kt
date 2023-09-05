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

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketSettings
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetBucketOptions
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.StorageBackend
import com.couchbase.client.protocol.shared.Durability
import com.google.protobuf.Duration
import java.util.concurrent.TimeUnit
import kotlin.time.DurationUnit
import kotlin.time.toDuration

const val secondsToNanos = 1000000000

suspend fun handleBucketManager(cluster: Cluster, command: Command, result: Result.Builder) {
    val bm = command.clusterCommand.bucketManager

    if (bm.hasGetBucket()) {
        val bucketName = bm.getBucket.bucketName
        val response: com.couchbase.client.kotlin.manager.bucket.BucketSettings
        if (bm.getBucket.hasOptions()) {
            response = cluster.buckets.getBucket(bucketName)
        } else {
            val options = createGetBucketOptions(bm.getBucket.options)
            response = cluster.buckets.getBucket(bucketName, options)
        }
        populateResult(result, response)
    }

}

fun createGetBucketOptions(getBucketOptions: GetBucketOptions): CommonOptions {
    val options = if (getBucketOptions.hasTimeoutMsecs()) {
        CommonOptions(getBucketOptions.timeoutMsecs.toDuration(DurationUnit.MILLISECONDS))
    } else {
        CommonOptions.Default
    }
    return options
}

fun populateResult(result: Result.Builder, response: com.couchbase.client.kotlin.manager.bucket.BucketSettings) {
    val builder = BucketSettings.newBuilder()
        .setCompressionMode(CompressionMode.valueOf(response.compressionMode.toString().uppercase()))
        .setFlushEnabled(response.flushEnabled)
        .setName(response.name)
        .setNumReplicas(response.replicas)
        .setStorageBackend(StorageBackend.valueOf(response.storageBackend.toString().uppercase()))
        .setRamQuotaMB(response.ramQuota.inBytes / 1000000)

    when (response.bucketType) {
        com.couchbase.client.kotlin.manager.bucket.BucketType.COUCHBASE -> builder.bucketType = BucketType.COUCHBASE
        com.couchbase.client.kotlin.manager.bucket.BucketType.EPHEMERAL -> builder.bucketType = BucketType.EPHEMERAL
        com.couchbase.client.kotlin.manager.bucket.BucketType.MEMCACHED -> builder.bucketType = BucketType.MEMCACHED
    }

    when (response.evictionPolicy) {
        com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.FULL -> builder.evictionPolicy = EvictionPolicyType.FULL
        com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.NO_EVICTION -> builder.evictionPolicy = EvictionPolicyType.NO_EVICTION
        com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.NOT_RECENTLY_USED -> builder.evictionPolicy = EvictionPolicyType.NOT_RECENTLY_USED
        com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.VALUE_ONLY -> builder.evictionPolicy = EvictionPolicyType.VALUE_ONLY
    }

    when (response.minimumDurability) {
        com.couchbase.client.kotlin.kv.Durability.majority() -> builder.minimumDurabilityLevel = Durability.MAJORITY
        com.couchbase.client.kotlin.kv.Durability.majorityAndPersistToActive() -> builder.minimumDurabilityLevel = Durability.MAJORITY_AND_PERSIST_TO_ACTIVE
        com.couchbase.client.kotlin.kv.Durability.persistToMajority() -> builder.minimumDurabilityLevel = Durability.PERSIST_TO_MAJORITY
        else -> builder.minimumDurabilityLevel = Durability.NONE
    }

    when (response.storageBackend) {
        com.couchbase.client.kotlin.manager.bucket.StorageBackend.COUCHSTORE -> builder.storageBackend = StorageBackend.COUCHSTORE
        com.couchbase.client.kotlin.manager.bucket.StorageBackend.MAGMA -> builder.storageBackend = StorageBackend.MAGMA
    }

    when (response.maximumExpiry) {
        Expiry.None -> builder.setMaxExpirySeconds(0)
        Expiry.Unknown -> builder.setMaxExpirySeconds(0)
        else -> builder.setMaxExpirySeconds((response.maximumExpiry as Expiry.Relative).duration.inWholeSeconds.toInt())
    }


    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
        .setBucketManagerResult(com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result.newBuilder()
            .setBucketSettings(builder)))
}
