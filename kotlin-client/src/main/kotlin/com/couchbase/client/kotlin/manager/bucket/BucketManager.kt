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

package com.couchbase.client.kotlin.manager.bucket

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.error.BucketExistsException
import com.couchbase.client.core.error.BucketNotFlushableException
import com.couchbase.client.core.error.BucketNotFoundException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.manager.CoreBucketManager
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.util.StorageSize
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes
import kotlinx.coroutines.future.await
import kotlin.time.Duration

public class BucketManager(core: Core) {
    private val coreManager = CoreBucketManager(core)

    /**
     * @sample com.couchbase.client.kotlin.samples.createBucket
     *
     * @throws BucketExistsException if bucket already exists
     */
    public suspend fun createBucket(
        name: String,
        common: CommonOptions = CommonOptions.Default,
        ramQuota: StorageSize = 100.mebibytes,
        bucketType: BucketType? = null,
        storageBackend: StorageBackend? = null,
        evictionPolicy: EvictionPolicyType? = null,
        flushEnabled: Boolean? = null,
        replicas: Int? = null,
        maximumExpiry: Expiry? = null,
        compressionMode: CompressionMode? = null,
        minimumDurability: Durability? = null,
        conflictResolutionType: ConflictResolutionType? = null,
        replicateViewIndexes: Boolean? = null,
        @SinceCouchbase("7.2") historyRetentionCollectionDefault: Boolean? = null,
        @SinceCouchbase("7.2") historyRetentionSize: StorageSize? = null,
        @SinceCouchbase("7.2") historyRetentionDuration: Duration? = null,
    ) {
        val params = toMap(
            name = name,
            ramQuota = ramQuota,
            bucketType = bucketType,
            storageBackend = storageBackend,
            evictionPolicy = evictionPolicy,
            flushEnabled = flushEnabled,
            conflictResolutionType = conflictResolutionType,
            replicas = replicas,
            replicateViewIndexes = replicateViewIndexes,
            historyRetentionCollectionDefault = historyRetentionCollectionDefault,
            historyRetentionDuration = historyRetentionDuration,
            historyRetentionSize = historyRetentionSize,
            maximumExpiry = maximumExpiry,
            compressionMode = compressionMode,
            minimumDurability = minimumDurability,
        )

        coreManager.createBucket(params.toMap(), common.toCore()).await()
    }

    @Deprecated("Retained for binary compatibility", level = DeprecationLevel.HIDDEN)
    public suspend fun createBucket(
        name: String,
        common: CommonOptions = CommonOptions.Default,
        ramQuota: StorageSize = 100.mebibytes,
        bucketType: BucketType = BucketType.COUCHBASE,
        storageBackend: StorageBackend? = null, // null means default for bucket type
        evictionPolicy: EvictionPolicyType? = null, // null means default for bucket type
        flushEnabled: Boolean = false,
        replicas: Int = 1,
        maximumExpiry: Expiry = Expiry.none(),
        compressionMode: CompressionMode = CompressionMode.PASSIVE,
        minimumDurability: Durability = Durability.none(),
        conflictResolutionType: ConflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER,
    ) {
        @Suppress("DEPRECATION")
        createBucket(
            name = name,
            common = common,
            ramQuota = ramQuota,
            bucketType = bucketType,
            storageBackend = storageBackend,
            evictionPolicy = evictionPolicy,
            flushEnabled = flushEnabled,
            replicas = if (bucketType == BucketType.MEMCACHED) null else replicas,
            conflictResolutionType = conflictResolutionType,
            // Workarounds for Community Edition, which rejects requests that have these parameters.
            maximumExpiry = if (maximumExpiry == Expiry.none()) null else maximumExpiry,
            compressionMode = if (compressionMode == CompressionMode.PASSIVE) null else compressionMode,
            minimumDurability = if (minimumDurability !is Durability.Synchronous) null else minimumDurability,
        )
    }

    /**
     * Modifies an existing bucket.
     *
     * @throws BucketNotFoundException if the bucket does not exist
     * @sample com.couchbase.client.kotlin.samples.updateBucket
     */
    public suspend fun updateBucket(
        name: String,
        common: CommonOptions = CommonOptions.Default,
        ramQuota: StorageSize? = null,
        flushEnabled: Boolean? = null,
        replicas: Int? = null,
        maximumExpiry: Expiry? = null,
        compressionMode: CompressionMode? = null,
        minimumDurability: Durability? = null,
        evictionPolicy: EvictionPolicyType? = null,
        @SinceCouchbase("7.2") historyRetentionCollectionDefault: Boolean? = null,
        @SinceCouchbase("7.2") historyRetentionSize: StorageSize? = null,
        @SinceCouchbase("7.2") historyRetentionDuration: Duration? = null,
    ) {
        val params = toMap(
            name = name,
            ramQuota = ramQuota,
            flushEnabled = flushEnabled,
            replicas = replicas,
            maximumExpiry = maximumExpiry,
            compressionMode = compressionMode,
            minimumDurability = minimumDurability,
            evictionPolicy = evictionPolicy,
            historyRetentionCollectionDefault = historyRetentionCollectionDefault,
            historyRetentionDuration = historyRetentionDuration,
            historyRetentionSize = historyRetentionSize,

            // these cannot be updated
            bucketType = null,
            conflictResolutionType = null,
            storageBackend = null,
            replicateViewIndexes = null,
        )
        coreManager.updateBucket(params, common.toCore()).await()
    }

    @Deprecated("Retained for binary compatibility", level = DeprecationLevel.HIDDEN)
    public suspend fun updateBucket(
        name: String,
        common: CommonOptions = CommonOptions.Default,
        ramQuota: StorageSize? = null,
        flushEnabled: Boolean? = null,
        replicas: Int? = null,
        maximumExpiry: Expiry? = null,
        compressionMode: CompressionMode? = null,
        minimumDurability: Durability? = null,
        evictionPolicy: EvictionPolicyType? = null,
    ) {
        updateBucket(
            name = name,
            common = common,
            ramQuota = ramQuota,
            flushEnabled = flushEnabled,
            replicas = replicas,
            maximumExpiry = maximumExpiry,
            compressionMode = compressionMode,
            minimumDurability = minimumDurability,
            evictionPolicy = evictionPolicy,
        )
    }

    /**
     * Deletes a bucket.
     *
     * @throws BucketNotFoundException if the specified bucket does not exist.
     */
    public suspend fun dropBucket(
        name: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        coreManager.dropBucket(name, common.toCore()).await()
    }

    /**
     * @throws BucketNotFoundException if the specified bucket does not exist.
     */
    public suspend fun getBucket(
        name: String,
        common: CommonOptions = CommonOptions.Default,
    ): BucketSettings = BucketSettings.fromJson(
        coreManager.getBucket(name, common.toCore()).await()
    )

    public suspend fun getAllBuckets(
        common: CommonOptions = CommonOptions.Default,
    ): List<BucketSettings> = coreManager.getAllBuckets(common.toCore()).await()
        .values.toList().map { BucketSettings.fromJson(it) }

    /**
     * Removes all documents from a bucket.
     *
     * Flush must be enabled on the bucket in order to perform this operation.
     * Enabling flush is not recommended in a production cluster, as it increases
     * the chance of accidental data loss.
     *
     * Keep in mind that flush is not an atomic operation. The server will need some time
     * to clean the partitions out completely. If an integration test scenario requires isolation,
     * creating individual buckets might provide better results.
     *
     * @throws BucketNotFoundException if the specified bucket does not exist.
     * @throws BucketNotFlushableException if flush is not enabled on the bucket.
     */
    public suspend fun flushBucket(
        bucketName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        coreManager.flushBucket(bucketName, common.toCore()).await()
    }

    /**
     * Returns true if all nodes report a bucket status of "healthy".
     *
     * @throws BucketNotFoundException if the specified bucket does not exist.
     */
    @VolatileCouchbaseApi
    public suspend fun isHealthy(
        bucketName: String,
        common: CommonOptions = CommonOptions.Default,
    ): Boolean {
        val tree = Mapper.decodeIntoTree(coreManager.getBucket(bucketName, common.toCore()).await())
        with(tree.path("nodes")) {
            return isArray && !isEmpty && all { it["status"].asText() == "healthy" }
        }
    }

    private fun toMap(
        name: String,
        ramQuota: StorageSize?,
        bucketType: BucketType?,
        storageBackend: StorageBackend?,
        flushEnabled: Boolean?,
        replicas: Int?,
        maximumExpiry: Expiry?,
        compressionMode: CompressionMode?,
        minimumDurability: Durability?,
        evictionPolicy: EvictionPolicyType?,
        conflictResolutionType: ConflictResolutionType?,
        replicateViewIndexes: Boolean?,
        historyRetentionCollectionDefault: Boolean?,
        historyRetentionSize: StorageSize?,
        historyRetentionDuration: Duration?,
    ): Map<String, String> {
        val params = mutableMapOf<String, Any>("name" to name)
        ramQuota?.let { params["ramQuotaMB"] = it.inWholeMebibytes }
        flushEnabled?.let { params["flushEnabled"] = if (it) 1 else 0 }
        evictionPolicy?.let { params["evictionPolicy"] = it.name }
        compressionMode?.let { params["compressionMode"] = it.name }
        storageBackend?.let { params["storageBackend"] = it.name }
        bucketType?.let { params["bucketType"] = it.name }
        conflictResolutionType?.let { params["conflictResolutionType"] = it.name }
        replicas?.let { params["replicaNumber"] = it }
        replicateViewIndexes?.let { params["replicaIndex"] = if (it) 1 else 0 }
        historyRetentionCollectionDefault?.let { params["historyRetentionCollectionDefault"] = it }
        historyRetentionSize?.let { params["historyRetentionBytes"] = it.inBytes }
        historyRetentionDuration?.let { params["historyRetentionSeconds"] = it.inWholeSeconds }

        maximumExpiry?.let {
            require(it is Expiry.None || it is Expiry.Relative) {
                "Maximum expiry must be Expiry.none() or Expiry.of(Duration)."
            }
            params["maxTTL"] = if (it is Expiry.Relative) it.duration.inWholeSeconds else 0
        }

        minimumDurability?.let {
            require(it !is Durability.ClientVerified) {
                "Minimum durability must not be client verified."
            }
            params["durabilityMinLevel"] = it.levelIfSynchronous().orElse(DurabilityLevel.NONE)
                .encodeForManagementApi()
        }

        return params.mapValues { (_, v) -> v.toString() }
    }
}

internal fun Durability.levelIfSynchronous() =
    (this as? Durability.Synchronous)?.level.toOptional()
