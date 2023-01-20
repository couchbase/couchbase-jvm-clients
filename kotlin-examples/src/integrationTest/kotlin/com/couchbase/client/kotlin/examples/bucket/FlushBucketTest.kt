package com.couchbase.client.kotlin.examples.bucket

import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.query.execute
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

private const val BUCKET_NAME = "temp-bucket-2"
private const val DOCUMENTS_AMOUNT = 10

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class FlushBucketTest {

    companion object {

        /**
         *
         * We are inserting a small delay between the creation of the bucket and other operations
         * to give indexer time to catch up. The timing will vary depending on the machine and collection size
         *
         */

        @JvmStatic
        @BeforeAll
        fun setup() {
            ConnectionUtils.withCluster {
                it.buckets.createBucket(
                    name = BUCKET_NAME,
                    ramQuota = 128.mebibytes,
                )
                val bucket = it.bucket(BUCKET_NAME)
                val collection = bucket.defaultCollection()
                it.queryIndexes.createPrimaryIndex(Keyspace(BUCKET_NAME))
                for (i in 1..DOCUMENTS_AMOUNT) {
                    collection.insert("key$i", "value$i")
                }
                delay(200L * DOCUMENTS_AMOUNT) // wait for index to be ready
            }
        }

    }

    @Test
    @Order(1)
    fun `check data in bucket`() {
        ConnectionUtils.withCluster {
            val result = it.query("SELECT * FROM `$BUCKET_NAME`.`_default`.`_default`").execute()
            assertEquals(DOCUMENTS_AMOUNT, result.rows.size)
        }
    }

}
