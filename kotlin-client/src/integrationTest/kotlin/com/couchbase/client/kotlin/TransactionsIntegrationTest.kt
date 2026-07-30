/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.kotlin

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.kotlin.transactions.TransactionAttemptContext
import com.couchbase.client.kotlin.transactions.TransactionFailedException
import com.couchbase.client.kotlin.transactions.TransactionGetResult
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.fail
import java.util.UUID

/**
 * A smoke test to make sure the Kotlin SDK correctly invokes
 * the core implementation.
 */
@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class TransactionsIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `propagates DocumentNotFoundException`(): Unit = runBlocking {
        try {
            cluster.transactions.run {
                get(collection, "does-not-exist")
            }
            fail("Expected transaction to fail")
        } catch (e: TransactionFailedException) {
            assertInstanceOf<DocumentNotFoundException>(e.cause)
        }
    }

    @Test
    fun `serializes concurrent executions`(): Unit = runBlocking {
        suspend fun TransactionAttemptContext.incrementAndGet(
            collection: Collection,
            docId: String,
        ): TransactionGetResult = run {
            val doc = get(collection, docId)
            replace(doc, doc.contentAs<Int>() + 1)
        }

        val counterA = "counter-a-" + UUID.randomUUID()
        val counterB = "counter-b-" + UUID.randomUUID()
        collection.upsert(counterA, 0)
        collection.upsert(counterB, 0)

        val iterations = 50

        (0 until iterations).map { _ ->
            async(Dispatchers.IO) {
                cluster.transactions.run {
                    assertEquals(
                        incrementAndGet(collection, counterA).contentAs<Int>(),
                        incrementAndGet(collection, counterB).contentAs<Int>(),
                    )
                }
            }
        }.awaitAll()

        assertEquals(
            iterations,
            collection.get(counterA).contentAs<Int>()
        )

        assertEquals(
            iterations,
            collection.get(counterB).contentAs<Int>()
        )
    }

    @Test
    fun `can return value`(): Unit = runBlocking {
        val docId = "test-" + UUID.randomUUID()
        collection.upsert(docId, "hello")

        val greeting = cluster.transactions.run {
            get(collection, docId).contentAs<String>()
        }.value

        assertEquals("hello", greeting)
    }
}
