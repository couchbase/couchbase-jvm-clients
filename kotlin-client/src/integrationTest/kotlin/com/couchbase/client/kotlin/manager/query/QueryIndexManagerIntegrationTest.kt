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
package com.couchbase.client.kotlin.manager.query

import com.couchbase.client.core.error.IndexExistsException
import com.couchbase.client.core.error.IndexNotFoundException
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.internal.retry
import com.couchbase.client.kotlin.query.execute
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.util.waitUntil
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import com.couchbase.client.test.ManagementApiTest
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds


// Disabling against 5.5 as there appear to be several query bugs (SCBC-246, SCBC-251).  Hardcoding 5.5.6 as that's
// the current 5.5-release and it's unlikely to change.
private const val DISABLE_QUERY_TESTS_FOR_CLUSTER = "5.5.6"
// See QueryIndexManagerIntegrationTest for explanation
private const val REQUIRE_MB_50132 = "7.1.0"
private val watchTimeout = 15.seconds

@IgnoreWhen(
    clusterTypes = [ClusterType.MOCKED],
    missesCapabilities = [Capabilities.QUERY],
    clusterVersionEquals = DISABLE_QUERY_TESTS_FOR_CLUSTER,
    clusterVersionIsBelow = REQUIRE_MB_50132
)
@ManagementApiTest
internal class QueryIndexManagerIntegrationTest : KotlinIntegrationTest() {
    private val indexes: QueryIndexManager by lazy { cluster.queryIndexes }
    private val defaultCollection: Keyspace by lazy { Keyspace(bucket.name) }

    @BeforeAll
    fun waitForQueryIndexer(): Unit = runBlocking {
        waitForQueryIndexerToHaveKeyspace(bucket.name)
    }

    @AfterEach
    @BeforeEach
    fun dropAllIndexes(): Unit = runBlocking {
        indexes.getAllIndexes(bucket.name).forEach {
            indexes.dropIndex(it.keyspace, it.name, ignoreIfNotExists = true)
        }
    }


    private suspend fun QueryIndexManager.get(keyspace: Keyspace, indexName: String): QueryIndex =
        get(keyspace) { it.name == indexName }

    private suspend fun QueryIndexManager.get(keyspace: Keyspace, predicate: (QueryIndex) -> Boolean): QueryIndex =
        getAllIndexes(keyspace).find { it.keyspace == keyspace && predicate(it) }
            ?: throw AssertionError("No index in $keyspace matches predicate")

    private suspend fun QueryIndexManager.find(keyspace: Keyspace, predicate: (QueryIndex) -> Boolean): QueryIndex? =
        getAllIndexes(keyspace).find { it.keyspace == keyspace && predicate(it) }

    private suspend fun QueryIndexManager.exists(keyspace: Keyspace, predicate: (QueryIndex) -> Boolean): Boolean =
        find(keyspace, predicate) != null

    private suspend fun primaryIndexExists(keyspace: Keyspace, name: String = "#primary") =
        indexes.exists(keyspace) { it.name == name && it.primary }

    private suspend fun secondaryIndexExists(keyspace: Keyspace, name: String): Boolean =
        indexes.exists(keyspace) { it.name == name && !it.primary }

    @Test
    fun createDuplicatePrimaryIndex(): Unit = runBlocking {
        assertFalse(primaryIndexExists(defaultCollection))

        indexes.createPrimaryIndex(defaultCollection)
        assertTrue(primaryIndexExists(defaultCollection))

        assertThrows<IndexExistsException> { indexes.createPrimaryIndex(defaultCollection) }

        indexes.createPrimaryIndex(defaultCollection, ignoreIfExists = true)
    }

    @Test
    fun createDuplicateSecondaryIndex(): Unit = runBlocking {
        val indexName = "test-index-" + UUID.randomUUID()
        val fields = setOf("fieldA", "fieldB")

        assertFalse(secondaryIndexExists(defaultCollection, indexName))

        indexes.createIndex(defaultCollection, indexName = indexName, fields = fields)
        assertTrue(secondaryIndexExists(defaultCollection, indexName))

        assertThrows<IndexExistsException> {
            indexes.createIndex(defaultCollection,
                indexName = indexName,
                fields = fields)
        }

        indexes.createIndex(defaultCollection, indexName = indexName, fields = fields, ignoreIfExists = true)
    }

    @Test
    fun createIndex(): Unit = runBlocking {
        val indexName = "test-index-" + UUID.randomUUID()
        val fields = setOf("fieldB.foo", "`fieldB`.`bar`")

        indexes.createIndex(defaultCollection, indexName, fields)
        val index = indexes.get(defaultCollection) { it.name == indexName }
        assertFalse(index.primary)
        assertEquals("gsi", Mapper.decodeIntoTree(index.raw).path("using").textValue())
        assertNull(index.partition)
        assertEquals(
            setOf("(`fieldB`.`foo`)", "(`fieldB`.`bar`)"),
            index.indexKey.toSet()
        )
    }

    @Test
    fun dropPrimaryIndex(): Unit = runBlocking {
        assertThrows<IndexNotFoundException> { indexes.dropPrimaryIndex(defaultCollection) }

        indexes.dropPrimaryIndex(defaultCollection, ignoreIfNotExists = true)

        indexes.createPrimaryIndex(defaultCollection)
        assertTrue(indexes.getAllIndexes(defaultCollection).single().primary)

        indexes.dropPrimaryIndex(defaultCollection)
        assertNoIndexesPresent(defaultCollection)
    }

    @Test
    fun dropIndex(): Unit = runBlocking {
        assertThrows<IndexNotFoundException> { indexes.dropIndex(defaultCollection, indexName = "foo") }

        indexes.dropIndex(defaultCollection, indexName = "foo", ignoreIfNotExists = true)

        indexes.createIndex(defaultCollection, indexName = "foo", fields = setOf("a", "b"))
        assertFalse(indexes.get(defaultCollection, "foo").primary)

        indexes.dropIndex(defaultCollection, indexName = "foo")
        assertNoIndexesPresent(defaultCollection)
    }

    @Test
    fun dropNamedPrimaryIndex(): Unit = runBlocking {
        indexes.createPrimaryIndex(defaultCollection,
            indexName = "namedPrimary",
            common = CommonOptions(timeout = 2.minutes))

        assertTrue(indexes.get(defaultCollection, "namedPrimary").primary)
        indexes.dropIndex(defaultCollection, "namedPrimary")
        assertNoIndexesPresent(defaultCollection)
    }

    @Test
    fun buildZeroDeferredIndexes(): Unit = runBlocking {
        // nothing to do, but shouldn't fail
        indexes.buildDeferredIndexes(defaultCollection)
    }

    @Test
    fun buildOneDeferredIndex(): Unit = runBlocking {
        val indexName = "hyphenated-index-name"
        createDeferredIndex(defaultCollection, indexName)
        assertEquals("deferred", indexes.get(defaultCollection, indexName).state)

        indexes.buildDeferredIndexes(defaultCollection)
        assertAllIndexesComeOnline(defaultCollection)
    }

    @Test
    fun buildTwoDeferredIndexes(): Unit = runBlocking {
        createDeferredIndex(defaultCollection, "indexOne")
        createDeferredIndex(defaultCollection, "indexTwo")
        assertEquals("deferred", indexes.get(defaultCollection, "indexOne").state)
        assertEquals("deferred", indexes.get(defaultCollection, "indexTwo").state)

        indexes.buildDeferredIndexes(defaultCollection)
        assertAllIndexesComeOnline(defaultCollection)
    }

    @Test
    fun buildDeferredIndexOnAbsentBucket(): Unit = runBlocking {
        indexes.buildDeferredIndexes(Keyspace("noSuchBucket"))
    }

    @Test
    fun canWatchZeroIndexes(): Unit = runBlocking {
        indexes.watchIndexes(defaultCollection, indexNames = emptySet(), timeout = 3.seconds)
    }

    @Test
    fun watchingAbsentIndexThrowsException(): Unit = runBlocking {
        assertThrows<IndexNotFoundException> {
            indexes.watchIndexes(defaultCollection, indexNames = setOf("DoesNotExist"), timeout = 3.seconds)
        }
    }

    @Test
    fun watchingAbsentPrimaryIndexThrowsException(): Unit = runBlocking {
        assertThrows<IndexNotFoundException> {
            indexes.watchIndexes(defaultCollection, listOf(), timeout = 3.seconds, includePrimary = true)
        }
    }

    @Test
    fun canWatchAlreadyBuiltIndex(): Unit = runBlocking {
        indexes.createIndex(defaultCollection, "myIndex", setOf("someField"))
        assertAllIndexesComeOnline(defaultCollection)

        indexes.watchIndexes(defaultCollection, listOf("myIndex"), timeout = watchTimeout)
    }

    @Test
    fun watchTimesOutIfOneIndexStaysDeferred(): Unit = runBlocking {
        indexes.createIndex(defaultCollection, "indexOne", setOf("someField"))
        indexes.watchIndexes(defaultCollection, listOf("indexOne"), timeout = watchTimeout)

        createDeferredIndex(defaultCollection, "indexTwo")

        val e = assertThrows<UnambiguousTimeoutException> {
            indexes.watchIndexes(defaultCollection, listOf("indexOne", "indexTwo"), timeout = 0.seconds)
        }
        assertTrue((e.message ?: "").contains("indexTwo=deferred"));
    }

    @Test
    fun watchRetriesUntilIndexesComeOnline(): Unit = runBlocking {
        indexes.createPrimaryIndex(defaultCollection, indexName = "indexOne", deferred = true)
        createDeferredIndex(defaultCollection, indexName = "indexTwo")
        createDeferredIndex(defaultCollection, indexName = "indexThree")

        launch {
            // sleep first so the watch operation needs to poll more than once.
            delay(1.seconds)
            indexes.buildDeferredIndexes(defaultCollection)
        }

        val indexNames = setOf ("indexOne", "indexTwo", "indexThree")
        indexes.watchIndexes(
            keyspace = defaultCollection,
            indexNames = indexNames,
            timeout = watchTimeout,
            includePrimary = true, // redundant, since the primary index was explicitly named; make sure it works anyway
        )

        assertTrue(indexes.getAllIndexes(defaultCollection).all { it.state == "online" })
    }

    private suspend fun createDeferredIndex(keyspace: Keyspace, indexName: String) =
        indexes.createIndex(keyspace, indexName, setOf("someField"), deferred = true)

    private suspend fun assertAllIndexesComeOnline(keyspace: Keyspace) {
        waitUntil { indexes.getAllIndexes(keyspace).all { it.state == "online" } }
    }

    private suspend fun assertNoIndexesPresent(keyspace: Keyspace) {
        assertEquals(emptyList<QueryIndex>(), indexes.getAllIndexes(keyspace))
    }

    private suspend fun waitForQueryIndexerToHaveKeyspace(keyspaceName: String) {
        retry(10.seconds) {
            val present = cluster.query(
                statement = "SELECT COUNT(*) > 0 as present FROM system:keyspaces where name = '$keyspaceName';",
            )
                .execute()
                .valueAs<Boolean>("present")

            if (!present) throw RuntimeException("query indexer doesn't have keyspace yet")
        }
    }
}
