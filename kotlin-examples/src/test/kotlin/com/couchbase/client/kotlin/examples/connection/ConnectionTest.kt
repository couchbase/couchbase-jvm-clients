package com.couchbase.client.kotlin.examples.connection

import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.kv.GetResult
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class ConnectionTest {

    companion object {
        private lateinit var cluster: Cluster

        @BeforeAll
        @JvmStatic
        fun setup() {
            cluster = Cluster.connect("couchbase://127.0.0.1", "Administrator", "password")
        }
    }

    @Test
    fun `get data from default collection bucket`() {
        runBlocking {
            val bucket: Bucket = cluster.bucket("travel-sample")
            val collection: Collection = bucket.defaultCollection()
            val mutationResult = collection.insert("airline_10", "nothing")
            assertTrue(mutationResult.cas > 0)
            val result: GetResult = collection.get("airline_10")
            val contentAsString = result.contentAs<String>()
            assertTrue(contentAsString.contains("Airline 10"))
            assertEquals("airline_10", result.id)
            println(contentAsString)
            cluster.disconnect()
        }
    }

}
