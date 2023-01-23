package com.couchbase.client.kotlin.examples.bucket

import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.examples.util.ConnectionUtils.Companion.getCluster
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.execute
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

private const val BUCKET_NAME = "temp-bucket-2"
private const val DOCUMENTS_AMOUNT = 100
private const val PER_DOCUMENT_DELAY = 100L

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class FlushBucketExample {

    companion object {

        /**
         *
         * We are inserting a small delay between the creation of the bucket and other operations
         * to give indexer time to catch up. The timing will vary depending on the machine and collection size.
         * This is not a recommended practice, but it is used here to keep the example simple.
         *
         * We also enable the flush operation for the bucket. This is not recommended for production
         *
         */

        @JvmStatic
        @BeforeAll
        fun setup() {
            ConnectionUtils.withCluster {
                it.buckets.createBucket(
                    name = BUCKET_NAME,
                    ramQuota = 128.mebibytes,
                    flushEnabled = true
                )
                val bucket = it.bucket(BUCKET_NAME)
                val collection = bucket.defaultCollection()
                it.queryIndexes.createPrimaryIndex(Keyspace(BUCKET_NAME))
                for (i in 1..DOCUMENTS_AMOUNT) {
                    collection.insert("key$i", "value$i")
                }
                delay(PER_DOCUMENT_DELAY * DOCUMENTS_AMOUNT) // wait for the index to be ready
            }
        }

    }

    @Test
    @Order(1)
    fun `check data in bucket`() {
        ConnectionUtils.withCluster {
            val result = getAllDocuments()
            assertEquals(DOCUMENTS_AMOUNT, result.rows.size)
        }
    }

    @Test
    @Order(2)
    fun `flush bucket`() {
        ConnectionUtils.withCluster {
            it.buckets.flushBucket(BUCKET_NAME)
            val resultAfterFlush = getAllDocuments()
            assertEquals(0, resultAfterFlush.rows.size)
        }
    }

    private fun getAllDocuments(): QueryResult {
        return runBlocking { getCluster().query("SELECT * FROM `$BUCKET_NAME`").execute() }
    }

}
