package com.couchbase.client.kotlin.examples.query

import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.execute
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

private const val BUCKET_NAME = "travel-sample-parametrized-query"
private const val LIMIT = 5

/**
 *
 * Two ways of creating a parametrized query: named and positional arguments.
 *
 */

class ParametrizedQueryExample {

    companion object {
        @BeforeAll
        @JvmStatic
        fun setup() {
            ConnectionUtils.withCluster { cluster ->
                cluster.buckets.createBucket(BUCKET_NAME)
                val bucket = cluster.bucket(BUCKET_NAME)
                val collection = bucket.defaultCollection()
                for(i in 1..LIMIT) {
                    runBlocking { collection.insert("airport_$i", mapOf("type" to "airport")) }
                }
                cluster.queryIndexes.createPrimaryIndex(Keyspace(BUCKET_NAME)) // queries won't work without a primary index
            }
        }
    }

    @Test
    fun `parametrized query with positional query parameters`() {
        ConnectionUtils.withCluster {
            val params = QueryParameters.positional(listOf("airport"))
            val result = it.query(
                "SELECT * FROM `$BUCKET_NAME` WHERE type = ? limit $LIMIT",
                parameters = params
            ).execute()
            assertEquals(LIMIT, result.rows.size)
        }
    }

    @Test
    fun `parametrized query with named query parameters`() {
        ConnectionUtils.withCluster {
            val params = QueryParameters.named("type" to "airport")
            val result = it.query(
                "SELECT * FROM `$BUCKET_NAME` WHERE type = \$type limit $LIMIT", // need to escape the $ in the query cause of Kotlin string interpolation
                parameters = params
            ).execute()
            assertEquals(LIMIT, result.rows.size)
        }
    }

}
