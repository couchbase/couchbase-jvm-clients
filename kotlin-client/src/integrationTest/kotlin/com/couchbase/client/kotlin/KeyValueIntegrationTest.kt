/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin

import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.kv.Durability.Companion.clientVerified
import com.couchbase.client.kotlin.kv.PersistTo
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.*

// Prevent timeouts against the Mock from interfering
// with Gerrit verification.  Remove when mock is fixed:
//    http://review.couchbase.org/c/CouchbaseMock/+/148081
@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class KeyValueIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `can upsert with client verified durability`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        val content = "some value"
        collection.upsert(id, content, durability = clientVerified(PersistTo.ACTIVE))
        assertEquals(content, collection.get(id).contentAs<String>())
    }

    @Test
    fun `upsert can insert and update`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        val insertResult = collection.upsert(id, mapOf("foo" to true))
        assertNotEquals(0, insertResult.cas)
        assertNotNull(insertResult.mutationToken)
        assertEquals(mapOf("foo" to true), collection.get(id).contentAs<Any>())

        val updateResult = collection.upsert(id, mapOf("foo" to false))

        assertNotEquals(0, updateResult.cas)
        assertNotNull(updateResult.mutationToken)

        assertNotEquals(insertResult.cas, updateResult.cas)
        assertNotEquals(insertResult.mutationToken, updateResult.mutationToken)
        assertEquals(mapOf("foo" to false), collection.get(id).contentAs<Any>())
    }

    @Test
    fun `upserting CommonOptions gives nice error message`(): Unit = runBlocking {
        val t = assertThrows<IllegalArgumentException> { collection.upsert("foo", CommonOptions()) }
        assertThat(t.message).startsWith("Expected document content")
    }

    @Test
    fun `can get with projections`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        collection.upsert(
            id, mapOf(
                "numbers" to mapOf(
                    "one" to 1,
                    "two" to 2,
                ),
                "fruit" to "apple",
            )
        )

        val expected = mapOf(
            "numbers" to mapOf(
                "one" to 1,
            )
        )

        val result = collection.get(id, project = listOf("numbers.one"))
        assertEquals(expected, result.contentAs<Any>())
    }

    @Test
    fun `getOrNull returns null for absent document`(): Unit = runBlocking {
        @OptIn(VolatileCouchbaseApi::class)
        assertNull(collection.getOrNull("does-not-exist")?.contentAs<String>())
    }

    @Test
    fun `getOrNull returns present document`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        collection.upsert(id, "foo")
        @OptIn(VolatileCouchbaseApi::class)
        assertEquals("foo", collection.getOrNull(id)?.contentAs<String>())
    }
}
