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
import com.couchbase.client.core.error.BucketExistsException
import com.couchbase.client.core.error.BucketNotFlushableException
import com.couchbase.client.core.error.BucketNotFoundException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.manager.CoreBucketManager
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import kotlinx.coroutines.future.await

public class BucketManager(core: Core) {
    private val coreManager = CoreBucketManager(core)

    /**
     * @sample com.couchbase.client.kotlin.samples.createBucket
     *
     * @throws BucketExistsException if bucket already exists
     */
    public suspend fun createBucket(
        settings: BucketSettings,
        common: CommonOptions = CommonOptions.Default,
    ) {
        coreManager.createBucket(settings.toMap(), common.toCore()).await()
    }

    /**
     * Modifies an existing bucket.
     *
     * First call [getBucket] to get the current settings.
     * Then call [BucketSettings.copy] and specify the properties you'd like to change.
     * Finally, pass the copy to updateBucket.
     *
     * @throws BucketNotFoundException if the bucket does not exist
     * @sample com.couchbase.client.kotlin.samples.updateBucket
     */
    public suspend fun updateBucket(
        settings: BucketSettings,
        common: CommonOptions = CommonOptions.Default,
    ) {
        coreManager.updateBucket(settings.toMap(), common.toCore()).await()
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

}
