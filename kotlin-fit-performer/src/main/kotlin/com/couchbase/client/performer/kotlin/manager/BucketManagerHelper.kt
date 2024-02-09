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
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.manager.bucket.BucketSettings
import com.couchbase.client.kotlin.manager.bucket.BucketType.Companion.COUCHBASE
import com.couchbase.client.kotlin.manager.bucket.BucketType.Companion.EPHEMERAL
import com.couchbase.client.kotlin.manager.bucket.BucketType.Companion.MEMCACHED
import com.couchbase.client.kotlin.manager.bucket.CompressionMode
import com.couchbase.client.kotlin.manager.bucket.ConflictResolutionType
import com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.Companion.FULL
import com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.Companion.NOT_RECENTLY_USED
import com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.Companion.NO_EVICTION
import com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType.Companion.VALUE_ONLY
import com.couchbase.client.kotlin.manager.bucket.StorageBackend
import com.couchbase.client.kotlin.manager.bucket.StorageBackend.Companion.COUCHSTORE
import com.couchbase.client.kotlin.manager.bucket.StorageBackend.Companion.MAGMA
import com.couchbase.client.kotlin.util.StorageSize.Companion.bytes
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.CreateBucketRequest
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.DropBucketRequest
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.FlushBucketRequest
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetAllBucketsRequest
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetAllBucketsResult
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetBucketOptions
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetBucketRequest
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.UpdateBucketRequest
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketSettings as FitBucketSettings
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType as FitBucketType
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode as FitCompressionMode
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.ConflictResolutionType as FitConflictResolutionType
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType as FitEvictionPolicyType
import com.couchbase.client.protocol.sdk.cluster.bucketmanager.StorageBackend as FitStorageBackend
import com.couchbase.client.protocol.shared.Durability as FitDurability

private fun unrecognized(type: String, value: Any?): Nothing {
    throw RuntimeException("Unrecognized $type: $value")
}

suspend fun dropBucket(cluster: Cluster, request: DropBucketRequest, result: Result.Builder) {
    cluster.buckets.dropBucket(request.bucketName)
    result.success()
}

suspend fun flushBucket(cluster: Cluster, request: FlushBucketRequest, result: Result.Builder) {
    cluster.buckets.flushBucket(request.bucketName)
    result.success()
}

suspend fun createBucket(cluster: Cluster, request: CreateBucketRequest, result: Result.Builder) {
    val settings = request.settings.settings

    cluster.buckets.createBucket(
        name = settings.name,
        common = CommonOptions.Default, // todo
        ramQuota = settings.ramQuotaMB.mebibytes,
        bucketType = if (!settings.hasBucketType()) null else when (settings.bucketType) {
            FitBucketType.COUCHBASE -> COUCHBASE
            FitBucketType.EPHEMERAL -> EPHEMERAL
            FitBucketType.MEMCACHED -> MEMCACHED
            else -> unrecognized("bucket type", settings.bucketType)
        },
        storageBackend = if (!settings.hasStorageBackend()) null else when (settings.storageBackend) {
            FitStorageBackend.COUCHSTORE -> COUCHSTORE
            FitStorageBackend.MAGMA -> MAGMA
            else -> unrecognized("storage backend", settings.storageBackend)
        },
        evictionPolicy = if (!settings.hasEvictionPolicy()) null else when (settings.evictionPolicy) {
            FitEvictionPolicyType.FULL -> FULL
            FitEvictionPolicyType.NOT_RECENTLY_USED -> NOT_RECENTLY_USED
            FitEvictionPolicyType.NO_EVICTION -> NO_EVICTION
            FitEvictionPolicyType.VALUE_ONLY -> VALUE_ONLY
            else -> unrecognized("eviction policy", settings.evictionPolicy)
        },
        flushEnabled = if (!settings.hasFlushEnabled()) null else settings.flushEnabled,
        replicas = if (!settings.hasNumReplicas()) null else settings.numReplicas,
        maximumExpiry = if (!settings.hasMaxExpirySeconds()) null else Expiry.of(settings.maxExpirySeconds.seconds),
        compressionMode = if (!settings.hasCompressionMode()) null else when (settings.compressionMode) {
            FitCompressionMode.ACTIVE -> CompressionMode.ACTIVE
            FitCompressionMode.OFF -> CompressionMode.Companion.OFF
            FitCompressionMode.PASSIVE -> CompressionMode.PASSIVE
            else -> unrecognized("compression mode", settings.compressionMode)
        },
        minimumDurability = if (!settings.hasMinimumDurabilityLevel()) null else when (settings.minimumDurabilityLevel) {
            FitDurability.NONE -> Durability.none()
            FitDurability.MAJORITY -> Durability.majority()
            FitDurability.MAJORITY_AND_PERSIST_TO_ACTIVE -> Durability.majorityAndPersistToActive()
            FitDurability.PERSIST_TO_MAJORITY -> Durability.persistToMajority()
            else -> unrecognized("durability", settings.minimumDurabilityLevel)
        },
        conflictResolutionType = if (!request.settings.hasConflictResolutionType()) null else when (request.settings.conflictResolutionType) {
            FitConflictResolutionType.TIMESTAMP -> ConflictResolutionType.TIMESTAMP
            FitConflictResolutionType.SEQUENCE_NUMBER -> ConflictResolutionType.SEQUENCE_NUMBER
            FitConflictResolutionType.CUSTOM -> ConflictResolutionType.CUSTOM
            else -> unrecognized("conflict resolution type", request.settings.conflictResolutionType)
        },
        replicateViewIndexes = if (!settings.hasReplicaIndexes()) null else settings.replicaIndexes,
        // [start:1.2.0]
        historyRetentionCollectionDefault = if (!settings.hasHistoryRetentionCollectionDefault()) null else settings.historyRetentionCollectionDefault,
        historyRetentionSize = if (!settings.hasHistoryRetentionBytes()) null else settings.historyRetentionBytes.bytes,
        historyRetentionDuration = if (!settings.hasHistoryRetentionSeconds()) null else settings.historyRetentionSeconds.seconds,
        // [end:1.2.0]
    )

    result.success()
}

suspend fun updateBucket(cluster: Cluster, request: UpdateBucketRequest, result: Result.Builder) {
    val settings = request.settings

    cluster.buckets.updateBucket(
        name = settings.name,
        common = CommonOptions.Default, // todo
        ramQuota = settings.ramQuotaMB.let { if (it == 0L) null else it.mebibytes },
        evictionPolicy = if (!settings.hasEvictionPolicy()) null else when (settings.evictionPolicy) {
            FitEvictionPolicyType.FULL -> FULL
            FitEvictionPolicyType.NOT_RECENTLY_USED -> NOT_RECENTLY_USED
            FitEvictionPolicyType.NO_EVICTION -> NO_EVICTION
            FitEvictionPolicyType.VALUE_ONLY -> VALUE_ONLY
            else -> unrecognized("eviction policy", settings.evictionPolicy)
        },
        flushEnabled = if (!settings.hasFlushEnabled()) null else settings.flushEnabled,
        replicas = if (!settings.hasNumReplicas()) null else settings.numReplicas,
        maximumExpiry = if (!settings.hasMaxExpirySeconds()) null else {
            if (settings.maxExpirySeconds == 0) Expiry.none() else Expiry.of(settings.maxExpirySeconds.seconds)
        },
        compressionMode = if (!settings.hasCompressionMode()) null else when (settings.compressionMode) {
            FitCompressionMode.ACTIVE -> CompressionMode.ACTIVE
            FitCompressionMode.OFF -> CompressionMode.Companion.OFF
            FitCompressionMode.PASSIVE -> CompressionMode.PASSIVE
            else -> unrecognized("compression mode", settings.compressionMode)
        },
        minimumDurability = if (!settings.hasMinimumDurabilityLevel()) null else when (settings.minimumDurabilityLevel) {
            FitDurability.NONE -> Durability.none()
            FitDurability.MAJORITY -> Durability.majority()
            FitDurability.MAJORITY_AND_PERSIST_TO_ACTIVE -> Durability.majorityAndPersistToActive()
            FitDurability.PERSIST_TO_MAJORITY -> Durability.persistToMajority()
            else -> unrecognized("durability", settings.minimumDurabilityLevel)
        },
        // [start:1.2.0]
        historyRetentionCollectionDefault = if (!settings.hasHistoryRetentionCollectionDefault()) null else settings.historyRetentionCollectionDefault,
        historyRetentionSize = if (!settings.hasHistoryRetentionBytes()) null else settings.historyRetentionBytes.bytes,
        historyRetentionDuration = if (!settings.hasHistoryRetentionSeconds()) null else settings.historyRetentionSeconds.seconds,
        // [end:1.2.0]
    )

    result.success()
}

suspend fun handleBucketManager(cluster: Cluster, command: Command, result: Result.Builder) {
    val bm = command.clusterCommand.bucketManager
    when {
        bm.hasGetBucket() -> getBucket(cluster, bm.getBucket, result)
        bm.hasGetAllBuckets() -> getAllBucket(cluster, bm.getAllBuckets, result)
        bm.hasCreateBucket() -> createBucket(cluster, bm.createBucket, result)
        bm.hasUpdateBucket() -> updateBucket(cluster, bm.updateBucket, result)
        bm.hasDropBucket() -> dropBucket(cluster, bm.dropBucket, result)
        bm.hasFlushBucket() -> flushBucket(cluster, bm.flushBucket, result)
    }
}

suspend fun getBucket(cluster: Cluster, request: GetBucketRequest, result: Result.Builder) {
    val bucketName = request.bucketName
    val response: BucketSettings
    if (request.hasOptions()) {
        response = cluster.buckets.getBucket(bucketName)
    } else {
        val options = createGetBucketOptions(request.options)
        response = cluster.buckets.getBucket(bucketName, options)
    }
    populateResult(result, response)
}

suspend fun getAllBucket(cluster: Cluster, request: GetAllBucketsRequest, result: Result.Builder) {
    val s: Map<String, FitBucketSettings> = cluster.buckets.getAllBuckets()
        .groupBy(BucketSettings::name) { it.toFit() }
        .mapValues { it.value.single().build() }

    result.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setBucketManagerResult(
                com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result.newBuilder()
                    .setGetAllBucketsResult(GetAllBucketsResult.newBuilder().putAllResult(s))
            )
    )
}

fun createGetBucketOptions(getBucketOptions: GetBucketOptions): CommonOptions {
    val options = if (getBucketOptions.hasTimeoutMsecs()) {
        CommonOptions(getBucketOptions.timeoutMsecs.toDuration(DurationUnit.MILLISECONDS))
    } else {
        CommonOptions.Default
    }
    return options
}

private fun BucketSettings.toFit(): FitBucketSettings.Builder {
    val sdk = this
    return FitBucketSettings.newBuilder().apply {
        compressionMode = FitCompressionMode.valueOf(sdk.compressionMode.toString().uppercase())
        flushEnabled = sdk.flushEnabled
        name = sdk.name
        numReplicas = sdk.replicas
        ramQuotaMB = sdk.ramQuota.inBytes / 1.mebibytes.inBytes

        bucketType = when (sdk.bucketType) {
            COUCHBASE -> FitBucketType.COUCHBASE
            EPHEMERAL -> FitBucketType.EPHEMERAL
            MEMCACHED -> FitBucketType.MEMCACHED
            else -> unrecognized("bucket type", sdk.bucketType)
        }

        evictionPolicy = when (sdk.evictionPolicy) {
            FULL -> FitEvictionPolicyType.FULL
            NO_EVICTION -> FitEvictionPolicyType.NO_EVICTION
            NOT_RECENTLY_USED -> FitEvictionPolicyType.NOT_RECENTLY_USED
            VALUE_ONLY -> FitEvictionPolicyType.VALUE_ONLY
            else -> unrecognized("eviction policy", sdk.evictionPolicy)
        }

        minimumDurabilityLevel = when (sdk.minimumDurability) {
            Durability.majority() -> FitDurability.MAJORITY
            Durability.majorityAndPersistToActive() -> FitDurability.MAJORITY_AND_PERSIST_TO_ACTIVE
            Durability.persistToMajority() -> FitDurability.PERSIST_TO_MAJORITY
            else -> FitDurability.NONE
        }

        when (sdk.storageBackend) {
            COUCHSTORE -> storageBackend = FitStorageBackend.COUCHSTORE
            MAGMA -> storageBackend = FitStorageBackend.MAGMA
            StorageBackend.of("undefined") -> {} // don't set; ephemeral / memcached
            StorageBackend.of("") -> {} // don't set; old server doesn't know about storage backends
            else -> unrecognized("storage backend", sdk.storageBackend)
        }

        maxExpirySeconds = when (sdk.maximumExpiry) {
            Expiry.None -> 0
            Expiry.Unknown -> 0
            else -> (sdk.maximumExpiry as Expiry.Relative).duration.inWholeSeconds.toInt()
        }

        sdk.historyRetentionCollectionDefault?.let { historyRetentionCollectionDefault = it }
        sdk.historyRetentionDuration?.let { historyRetentionSeconds = it.inWholeSeconds }
        sdk.historyRetentionSize?.let { historyRetentionBytes = it.inBytes }
    }
}

private fun populateResult(result: Result.Builder, response: BucketSettings) {
    val builder = response.toFit()

    result.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setBucketManagerResult(
                com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result.newBuilder()
                    .setBucketSettings(builder)
            )
    )
}

private fun Result.Builder.success() {
    setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder().setSuccess(true))
}
