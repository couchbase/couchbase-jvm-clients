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
package com.couchbase.client.kotlin.manager.collection

import com.couchbase.client.core.error.FeatureNotAvailableException
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.kotlin.manager.bucket.BucketType
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import kotlin.time.Duration.Companion.minutes

internal class CollectionManagerErrorIntegrationTest : KotlinIntegrationTest() {
    private val manager: CollectionManager by lazy { bucket.collections }

    /**
     * Note that the mock is also ignored since it returns a different 404 content msg (none) than the real server,
     * so better to ignore it than to introduce special handling logic just for the mock.
     */
    @Test
    @IgnoreWhen(
        hasCapabilities = [Capabilities.COLLECTIONS],
        clusterTypes = [ClusterType.MOCKED],
    )
    fun `throws FeatureNotAvailableException if collections not supported`(): Unit = runBlocking {
        with(manager) {
            assertThrows<FeatureNotAvailableException> { getAllScopes() }
            assertThrows<FeatureNotAvailableException> { createScope("foo") }
            assertThrows<FeatureNotAvailableException> { dropScope("foo") }
            assertThrows<FeatureNotAvailableException> { createCollection("foo", "bar") }
            assertThrows<FeatureNotAvailableException> { dropCollection("foo", "bar") }
        }
    }

    @Test
    @IgnoreWhen(
        missesCapabilities = [Capabilities.COLLECTIONS],
        hasCapabilities = [Capabilities.SERVERLESS],
        clusterTypes = [ClusterType.CAPELLA],
    )
    fun `throws FeatureNotAvailableException for memcached buckets`(): Unit = runBlocking {
        val memcachedBucketName = "memcached-" + UUID.randomUUID().toString().substring(0, 6)
        try {
            cluster.waitUntilReady(1.minutes).buckets.createBucket(
                name = memcachedBucketName,
                bucketType = BucketType.MEMCACHED,
            )

            val memcachedBucket = cluster.bucket(memcachedBucketName).waitUntilReady(1.minutes)
            with(memcachedBucket.collections) {
                // getAllScopes is the only supported operation
                val scopes = getAllScopes()
                assertTrue(
                    scopes.any { it.name == DEFAULT_SCOPE },
                    "getAllScopes for new memcached bucket should have included scope '$DEFAULT_SCOPE', but got: $scopes",
                )

                assertThrows<FeatureNotAvailableException> { createScope("foo") }
                assertThrows<FeatureNotAvailableException> { dropScope("foo") }
                assertThrows<FeatureNotAvailableException> { createCollection("foo", "bar") }
                assertThrows<FeatureNotAvailableException> { dropCollection("foo", "bar") }
            }
        } finally {
            runCatching { cluster.buckets.dropBucket(memcachedBucketName) }
        }
    }
}
