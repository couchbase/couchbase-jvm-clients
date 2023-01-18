package com.couchbase.client.kotlin.examples.connection

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.examples.util.NEW_CONTENT
import com.couchbase.client.kotlin.examples.util.TEST_BUCKET
import com.couchbase.client.kotlin.examples.util.TEST_CONTENT
import com.couchbase.client.kotlin.examples.util.TEST_ID
import com.couchbase.client.kotlin.kv.GetResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class SimpleOperationsTest {

    @Test
    @Order(1)
    fun `insert data to bucket`() {
        ConnectionUtils.withBucket(TEST_BUCKET) { _, bucket ->
            val collection: Collection = bucket.defaultCollection()
            val mutationResult = collection.insert(TEST_ID, TEST_CONTENT)
            assertTrue(mutationResult.cas > 0)
        }
    }

    @Test
    @Order(2)
    fun `get data from bucket`() {
        ConnectionUtils.withBucket(TEST_BUCKET) { _, bucket ->
            val collection: Collection = bucket.defaultCollection()
            val result: GetResult = collection.get(TEST_ID)
            val contentAsString = result.contentAs<String>()
            assertTrue(contentAsString.contains(TEST_CONTENT))
            assertEquals(TEST_ID, result.id)
            println(contentAsString)
        }
    }

    @Test
    @Order(3)
    fun `update data in bucket`() {
        ConnectionUtils.withBucket(TEST_BUCKET) { _, bucket ->
            val collection: Collection = bucket.defaultCollection()
            collection.upsert(TEST_ID, NEW_CONTENT)
            val result: GetResult = collection.get(TEST_ID)
            val contentAsString = result.contentAs<String>()
            assertFalse(contentAsString.contains(TEST_CONTENT))
            assertTrue(contentAsString.contains(NEW_CONTENT))
            println(contentAsString)
        }
    }

    @Test
    @Order(4)
    fun `delete data from bucket`() {
        ConnectionUtils.withBucket(TEST_BUCKET) { _, bucket ->
            val collection: Collection = bucket.defaultCollection()
            collection.remove(TEST_ID)
            assertThrows<DocumentNotFoundException> {
                collection.get(TEST_ID)
            }
        }
    }

}
