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

import com.couchbase.client.core.error.CollectionExistsException
import com.couchbase.client.core.error.CollectionNotFoundException
import com.couchbase.client.core.error.ScopeExistsException
import com.couchbase.client.core.error.ScopeNotFoundException
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.util.waitUntil
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

@IgnoreWhen(missesCapabilities = [Capabilities.COLLECTIONS], clusterTypes = [ClusterType.CAVES])
internal class CollectionManagerIntegrationTest : KotlinIntegrationTest() {
    private val manager: CollectionManager by lazy { bucket.collections }

    private val scopesToDrop: MutableSet<String> = HashSet()

    @AfterEach
    fun dropScopes(): Unit = runBlocking {
        scopesToDrop.forEach { runCatching { manager.dropScope(it) } }
        scopesToDrop.clear()
    }

    private suspend fun createTempScope(name: String = randomString()): ScopeSpec {
        scopesToDrop += name
        manager.createScope(name)
        waitUntil { manager.scopeExists(name) }
        ConsistencyUtil.waitUntilScopePresent(cluster.core, bucket.name, name)
        return ScopeSpec(name, emptyList())
    }

    private suspend fun createTempCollection(
        scopeName: String,
        collectionName: String = randomString(),
        maxExpiry: Duration? = null,
    ): CollectionSpec {
        val spec = CollectionSpec(scopeName, collectionName, maxExpiry)
        manager.createCollection(spec)
        waitUntil { manager.collectionExists(spec.scopeName, spec.name) }
        ConsistencyUtil.waitUntilCollectionPresent(cluster.core, bucket.name, spec.scopeName, spec.name)
        return spec.copy(history = false)
    }

    @Test
    fun `createScope throws ScopeExistsException`(): Unit = runBlocking {
        val scopeName = randomString()
        assertFalse(manager.scopeExists(scopeName))
        createTempScope(scopeName)
        assertThrows<ScopeExistsException> { manager.createScope(scopeName) }
    }

    @Test
    fun `createCollection throws CollectionExistsException`(): Unit = runBlocking {
        val scopeName = createTempScope().name
        val collectionName = randomString()
        assertFalse(manager.collectionExists(scopeName, collectionName))
        createTempCollection(scopeName, collectionName)
        assertThrows<CollectionExistsException> { manager.createCollection(scopeName, collectionName) }
    }

    @Test
    fun `createCollection throws ScopeNotFoundException`(): Unit = runBlocking {
        assertFalse(manager.scopeExists("non-existent-scope"))
        assertThrows<ScopeNotFoundException> { manager.createCollection("non-existent-scope", "some-collection") }
    }

    @Test
    fun `dropScope throws ScopeNotFoundException`(): Unit = runBlocking {
        assertFalse(manager.scopeExists("non-existent-scope"))
        assertThrows<ScopeNotFoundException> { manager.dropScope("non-existent-scope") }
    }

    @Test
    fun `dropCollection throws ScopeNotFoundException`(): Unit = runBlocking {
        assertFalse(manager.scopeExists("non-existent-scope"))
        assertThrows<ScopeNotFoundException> { manager.dropCollection("non-existent-scope", "non-existent-collection") }
    }

    @Test
    fun `dropCollection throws CollectionNotFoundException`(): Unit = runBlocking {
        val scopeName = createTempScope().name
        assertThrows<CollectionNotFoundException> { manager.dropCollection(scopeName, "non-existent-collection") }
    }

    @Test
    fun `can create scope and collection`() = runBlocking {
        with(manager) {
            val scopeName: String = randomString()
            val collectionName: String = randomString()

            assertFalse(collectionExists(scopeName, collectionName))

            createTempScope(scopeName)
            assertEquals(ScopeSpec(scopeName, emptyList()), getScope(scopeName))

            val expectedCollections = listOf(createTempCollection(scopeName, collectionName))
            assertEquals(ScopeSpec(scopeName, expectedCollections), getScope(scopeName))
        }
    }

    @Test
    fun `can drop scope and collection`() = runBlocking {
        with(manager) {
            val scopeName = createTempScope().name
            val collectionSpec1 = createTempCollection(scopeName)
            val collectionSpec2 = createTempCollection(scopeName)

            collectionSpec1.let {
                dropCollection(it)
                waitUntil { !collectionExists(it) }
                ConsistencyUtil.waitUntilCollectionDropped(cluster.core, bucket.name, it.scopeName, it.name)
            }

            assertEquals(ScopeSpec(scopeName, listOf(collectionSpec2)), getScope(scopeName))

            collectionSpec2.let {
                dropCollection(it)
                waitUntil { !collectionExists(it) }
                ConsistencyUtil.waitUntilCollectionDropped(cluster.core, bucket.name, it.scopeName, it.name)
            }

            assertEquals(ScopeSpec(scopeName, emptyList()), getScope(scopeName))

            dropScope(scopeName)
            waitUntil { !scopeExists(scopeName) }
            ConsistencyUtil.waitUntilScopeDropped(cluster.core, bucket.name, scopeName)
        }
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.ENTERPRISE_EDITION])
    fun `can create collection with max expiry`() = runBlocking {
        with(manager) {
            val expiry = 30.days
            val scopeName = createTempScope().name
            val collectionSpec1 = createTempCollection(scopeName, maxExpiry = expiry)
            val collectionSpec2 = createTempCollection(scopeName)

            collectionSpec1.let { assertEquals(expiry, getCollectionOrNull(it.scopeName, it.name)!!.maxExpiry) }
            collectionSpec2.let { assertNull(getCollectionOrNull(it.scopeName, it.name)!!.maxExpiry) }
        }
    }

    private suspend fun CollectionManager.getScopeOrNull(scopeName: String): ScopeSpec? {
        return try {
            getScope(scopeName)
        } catch (e: ScopeNotFoundException) {
            null
        }
    }

    private suspend fun CollectionManager.getCollectionOrNull(
        scopeName: String,
        collectionName: String,
    ): CollectionSpec? {
        val scopeSpec = getScopeOrNull(scopeName) ?: return null
        return scopeSpec.collections.find { it.name == collectionName }
    }

    private suspend fun CollectionManager.collectionExists(collection: CollectionSpec): Boolean =
        collectionExists(collection.scopeName, collection.name)

    private suspend fun CollectionManager.collectionExists(scopeName: String, collectionName: String): Boolean =
        getCollectionOrNull(scopeName, collectionName) != null

    private suspend fun CollectionManager.scopeExists(scopeName: String): Boolean =
        getScopeOrNull(scopeName) != null

    /**
     * Returns a random string suitable for use as a scope or collection name.
     */
    private fun randomString(): String = UUID.randomUUID().toString().substring(0, 10)

}
