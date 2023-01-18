package com.couchbase.client.kotlin.examples.connection

import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.kv.GetResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test

class SimpleOperationsTest {

    @Test
    @Order(1)
    fun `insert data to bucket`() {
        ConnectionUtils.withBucket("travel-sample") { _, bucket ->
            val collection: Collection = bucket.defaultCollection()
            val mutationResult = collection.insert("airline_10", "Airline 10")
            assertTrue(mutationResult.cas > 0)
        }
    }

    @Test
    @Order(2)
    fun `get data from bucket`() {
        ConnectionUtils.withBucket("travel-sample") { _, bucket ->
            val collection: Collection = bucket.defaultCollection()
            val result: GetResult = collection.get("airline_10")
            val contentAsString = result.contentAs<String>()
            assertTrue(contentAsString.contains("Airline 10"))
            assertEquals("airline_10", result.id)
            println(contentAsString)
        }
    }

}
