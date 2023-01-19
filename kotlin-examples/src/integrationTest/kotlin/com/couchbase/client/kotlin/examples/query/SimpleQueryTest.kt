package com.couchbase.client.kotlin.examples.query

import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.examples.util.TEST_BUCKET
import com.couchbase.client.kotlin.examples.util.TEST_CONTENT
import com.couchbase.client.kotlin.examples.util.TEST_ID
import com.couchbase.client.kotlin.examples.util.TEST_KEYSPACE
import com.couchbase.client.kotlin.query.execute
import com.google.gson.Gson
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class SimpleQueryTest {

    private val gson = Gson()
    companion object {
        @JvmStatic
        @BeforeAll
        fun setUp() {
            ConnectionUtils.withBucket(TEST_BUCKET) { cluster, bucket ->
                val collection: Collection = bucket.defaultCollection()
                collection.upsert(TEST_ID, TEST_CONTENT)
                cluster.queryIndexes.createPrimaryIndex(TEST_KEYSPACE)
            }
        }

        @JvmStatic
        @AfterAll
        fun clear() {
            ConnectionUtils.withCluster {
                it.queryIndexes.dropPrimaryIndex(TEST_KEYSPACE)
            }
        }
    }

    @Test
    fun `simple select by id`() {
        ConnectionUtils.withCluster { cluster ->
            val result = cluster.query("SELECT * FROM `$TEST_BUCKET` WHERE meta().id = '$TEST_ID'").execute()
            assertEquals(1, result.rows.size)
            val firstRow = result.rows[0]
            val content = gson.fromJson<Map<String, String>>(firstRow.content.decodeToString(), Map::class.java)
            assertTrue(content["travel-sample"].equals(TEST_CONTENT))
        }
    }

    @Test
    fun `simple select with metadata`() {
        ConnectionUtils.withCluster { cluster ->
            val result = cluster.query("SELECT META(`$TEST_BUCKET`).id, * FROM `$TEST_BUCKET` LIMIT 1").execute()

            assertEquals(1, result.rows.size)
            val firstRow = result.rows[0]

            val content = gson.fromJson<Map<String, String>>(firstRow.content.decodeToString(), Map::class.java)

            assertTrue(content["id"].equals(TEST_ID))
            assertTrue(content["travel-sample"].equals(TEST_CONTENT))
        }
    }

//    @Test
//    fun `simple insert`() {
//        // Use ConnectionUtils.withCluster method to create an insert query to a TEST_BUCKET
//        ConnectionUtils.withCluster { cluster ->
//            val result = cluster.query("INSERT INTO `$TEST_BUCKET` (KEY, VALUE) VALUES ('key', 'value')").execute()
//        }
//    }

}
