/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.manager.bucket

import com.couchbase.client.core.error.BucketExistsException
import com.couchbase.client.core.error.BucketNotFlushableException
import com.couchbase.client.core.error.BucketNotFoundException
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.manager.bucket.BucketManager
import com.couchbase.client.kotlin.manager.bucket.BucketSettings
import com.couchbase.client.kotlin.manager.bucket.BucketType
import com.couchbase.client.kotlin.manager.bucket.EvictionPolicyType
import com.couchbase.client.kotlin.manager.bucket.StorageBackend
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import com.couchbase.client.test.Util
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.*

/**
 * Verifies the functionality of the bucket manager.
 */
@IgnoreWhen(clusterTypes = [ClusterType.MOCKED, ClusterType.CAVES])
internal class BucketManagerIntegrationTest : KotlinIntegrationTest() {
    private val buckets: BucketManager by lazy { cluster.buckets }

    private val bucketsToDrop: MutableSet<String> = HashSet()

    @AfterEach
    fun dropBuckets(): Unit = runBlocking {
        bucketsToDrop.forEach { runCatching { buckets.dropBucket(it) } }
        bucketsToDrop.clear()
    }

    private fun waitUntilHealthy(bucket: String) {
        Util.waitUntilCondition {
            try {
                return@waitUntilCondition runBlocking { buckets.isHealthy(bucket) }
            } catch (err: BucketNotFoundException) {
                return@waitUntilCondition false
            }
        }
    }

    private fun waitUntilDropped(bucket: String) {
        Util.waitUntilCondition {
            return@waitUntilCondition runBlocking {
                buckets.getAllBuckets().none { it.name == bucket }
            }
        }
    }

    private fun randomName() = UUID.randomUUID().toString()

    private suspend fun createAndCheck(settings: BucketSettings, block: suspend BucketSettings.() -> Unit) {
        createBucket(settings)
        buckets.getBucket(settings.name).block()
    }

    /**
     * This sanity test is kept intentionally vague on its assertions since it depends how the test-util decide
     * to setup the default bucket when the test is created.
     */
    @Test
    fun getBucket(): Unit = runBlocking {
        buckets.getBucket(bucket.name)
    }

    /**
     * Since we don't know how many buckets are in the cluster when the test runs make sure it is at least one and
     * perform some basic assertions on them.
     */
    @Test
    fun getAllBuckets() = runBlocking {
        val allBucketSettings: List<BucketSettings> = buckets.getAllBuckets()
        assertTrue(allBucketSettings.isNotEmpty())
        allBucketSettings.forEach { assertTrue(it.ramQuota >= 100.mebibytes) }
    }

    @Test
    fun createEphemeralBucketWithDefaultEvictionPolicy(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.EPHEMERAL,
        )) {
            assertEquals(EvictionPolicyType.NO_EVICTION, evictionPolicy)
        }
    }

    @Test
    fun createEphemeralBucketWithNruEvictionPolicy(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.EPHEMERAL,
            evictionPolicy = EvictionPolicyType.NOT_RECENTLY_USED,
        )) {
            assertEquals(EvictionPolicyType.NOT_RECENTLY_USED, evictionPolicy)
        }
    }

    @Test
    fun createCouchbaseBucketWithDefaultEvictionPolicy(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.COUCHBASE,
        )) {
            assertEquals(EvictionPolicyType.VALUE_ONLY, evictionPolicy)
        }
    }

    @Test
    fun createCouchbaseBucketWithFullEvictionPolicy(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.COUCHBASE,
            evictionPolicy = EvictionPolicyType.FULL,
        )) {
            assertEquals(EvictionPolicyType.FULL, evictionPolicy)
        }
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.BUCKET_MINIMUM_DURABILITY])
    fun createCouchbaseBucketWithMinimumDurability(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.COUCHBASE,
            replicas = 0,
            minimumDurability = Durability.majority(),
        )) {
            assertEquals(Durability.majority(), minimumDurability)
        }
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.STORAGE_BACKEND])
    fun createCouchbaseBucketWithStorageBackendCouchstore(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.COUCHBASE,
            storageBackend = StorageBackend.COUCHSTORE,
        )) {
            assertEquals(StorageBackend.COUCHSTORE, storageBackend)
        }
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.STORAGE_BACKEND])
    fun createCouchbaseBucketWithStorageBackendDefault(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.COUCHBASE,
        )) {
            assertEquals(StorageBackend.COUCHSTORE, storageBackend)
        }
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.STORAGE_BACKEND])
    fun createCouchbaseBucketWithStorageBackendMagma(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            ramQuota = 256.mebibytes,
            bucketType = BucketType.COUCHBASE,
            storageBackend = StorageBackend.MAGMA,
        )) {
            assertEquals(StorageBackend.MAGMA, storageBackend)
            assertEquals(256.mebibytes, ramQuota)
        }
    }

    @Test
    fun shouldPickNoDurabilityLevelIfNotSpecified(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.COUCHBASE,
        )) {
            assertEquals(Durability.none(), minimumDurability)
        }
    }

    @Test
    @Suppress("DEPRECATION")
    fun createMemcachedBucket(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            bucketType = BucketType.MEMCACHED,
            minimumDurability = Durability.majority(),
        )) {
            assertEquals(BucketType.MEMCACHED, bucketType)
        }
    }

    @Test
    fun createAndDropBucket(): Unit = runBlocking {
        val name = randomName()
        createBucket(BucketSettings(name))
        waitUntilHealthy(name)
        assertNotNull(buckets.getAllBuckets().find { it.name == name })
        buckets.dropBucket(name)
        waitUntilDropped(name)
        assertNull(buckets.getAllBuckets().find { it.name == name })
    }

    @Test
    fun flushBucket(): Unit = runBlocking {
        val collection = bucket.defaultCollection()
        val id = UUID.randomUUID().toString()
        collection.upsert(id, "value")
        assertTrue(collection.exists(id).exists)
        buckets.flushBucket(bucket.name)
        Util.waitUntilCondition {
            runBlocking { !collection.exists(id).exists }
        }
    }

    @Test
    fun failIfBucketFlushDisabled(): Unit = runBlocking {
        val bucketName = UUID.randomUUID().toString()
        createBucket(BucketSettings(bucketName, flushEnabled = false))
        assertThrows<BucketNotFlushableException> {
            buckets.flushBucket(bucketName)
        }
    }

    @Test
    fun createShouldFailWhenPresent(): Unit = runBlocking {
        assertThrows<BucketExistsException> {
            buckets.createBucket(BucketSettings(bucket.name))
        }
    }

    @Test
    fun updateShouldOverrideWhenPresent(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            ramQuota = 100.mebibytes
        )) {
            assertEquals(100.mebibytes, ramQuota)
            val newQuota = 110.mebibytes
            buckets.updateBucket(copy(ramQuota = newQuota))

            Util.waitUntilCondition {
                runBlocking {
                    buckets.getBucket(name).ramQuota == newQuota
                }
            }
        }
    }

    @Test
    fun updateShouldFailIfAbsent(): Unit = runBlocking {
        assertThrows<BucketNotFoundException> {
            buckets.updateBucket(BucketSettings("does-not-exist"))
        }
    }

    @Test
    fun getShouldFailIfAbsent(): Unit = runBlocking {
        assertThrows<BucketNotFoundException> {
            buckets.getBucket("does-not-exist")
        }
    }

    @Test
    fun dropShouldFailIfAbsent(): Unit = runBlocking {
        assertThrows<BucketNotFoundException> {
            buckets.dropBucket("does-not-exist")
        }
    }

    @Test
    fun createWithMoreThanOneReplica(): Unit = runBlocking {
        createAndCheck(BucketSettings(
            name = randomName(),
            replicas = 3,
        )) {
            assertEquals(3, replicas)
        }
    }

    private suspend fun createBucket(settings: BucketSettings) {
        buckets.createBucket(settings)
        bucketsToDrop.add(settings.name)
        waitUntilHealthy(settings.name)
    }
}
