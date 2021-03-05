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

import com.couchbase.client.kotlin.kv.Durability.Companion.clientVerified
import com.couchbase.client.kotlin.kv.PersistTo
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.util.*

internal class KeyValueIntegrationTest : KotlinIntegrationTest() {

    private val cluster by lazy { connect() }
    private val collection by lazy {
        runBlocking {
            cluster.bucket(config().bucketname())
                // It should not take this long, but the Jenkins box...
                .waitUntilReady(Duration.ofSeconds(30))
                .defaultCollection()
        }
    }

    @AfterAll
    fun afterAll() = runBlocking {
        cluster.disconnect()
    }

    @Test
    fun `can upsert with client verified durability`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        collection.upsert(
            id, "some value",
            durability = clientVerified(PersistTo.ACTIVE)
        )
        // todo GET the result
    }


    @Test
    fun `upsert can insert and update`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        val insertResult = collection.upsert(id, mapOf("foo" to true))
        assertNotEquals(0, insertResult.cas)
        assertNotNull(insertResult.mutationToken)

        val updateResult = collection.upsert(id, mapOf("foo" to false))

        assertNotEquals(0, updateResult.cas)
        assertNotNull(updateResult.mutationToken)

        assertNotEquals(insertResult.cas, updateResult.cas)
        assertNotEquals(insertResult.mutationToken, updateResult.mutationToken)

        // todo GET the result
    }

    @Test
    fun `upserting CommonOptions gives nice error message`(): Unit = runBlocking {
        val t = assertThrows<IllegalArgumentException> {
            collection.upsert("foo", CommonOptions())
        }
        assertThat(t.message).startsWith("Expected document content")
    }

}
