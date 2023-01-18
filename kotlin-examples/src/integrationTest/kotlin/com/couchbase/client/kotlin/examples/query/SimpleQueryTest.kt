package com.couchbase.client.kotlin.examples.query

import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.examples.util.TEST_BUCKET
import com.couchbase.client.kotlin.examples.util.TEST_CONTENT
import com.couchbase.client.kotlin.examples.util.TEST_ID
import com.couchbase.client.kotlin.query.execute
import com.google.gson.Gson
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
            ConnectionUtils
            ConnectionUtils.withBucket(TEST_BUCKET) { _, bucket ->
                val collection: Collection = bucket.defaultCollection()
                collection.upsert(TEST_ID, TEST_CONTENT)
            }
        }
    }

    @Test
    fun `simple select`() {
        ConnectionUtils.withCluster {
            val result = it.query("SELECT META(`$TEST_BUCKET`).id, * FROM `$TEST_BUCKET` LIMIT 1").execute()
            assertEquals(1, result.rows.size)
            val firstRow = result.rows[0]

            val content = gson.fromJson<Map<String, String>>(firstRow.content.decodeToString(), Map::class.java)

            assertTrue(content["id"].equals(TEST_ID))
            assertTrue(content["travel-sample"].equals(TEST_CONTENT))
        }
    }

}
