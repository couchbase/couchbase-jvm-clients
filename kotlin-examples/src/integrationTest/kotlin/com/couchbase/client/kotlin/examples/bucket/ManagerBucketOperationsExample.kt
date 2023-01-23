package com.couchbase.client.kotlin.examples.bucket

import com.couchbase.client.core.error.BucketNotFoundException
import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.assertThrows

private const val BUCKET_NAME = "temp-bucket-manager"

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class ManagerBucketOperationsExample {

    @Test
    @Order(1)
    fun `create bucket`() {
        ConnectionUtils.withCluster {
            it.buckets.createBucket(
                BUCKET_NAME,
                ramQuota = 256.mebibytes,
                replicas = 1
            )
        }
    }

    @Test
    @Order(2)
    fun `update bucket`() {
        ConnectionUtils.withCluster {
            it.buckets.updateBucket(
                BUCKET_NAME,
                ramQuota = 512.mebibytes,
                replicas = 2
            )
        }
    }

    @Test
    @Order(3)
    fun `drop bucket`() {
        ConnectionUtils.withCluster {
            it.buckets.dropBucket(BUCKET_NAME)
        }
    }

    @Test
    @Order(4)
    fun `throw an exception when bucket does not exist`() {
        ConnectionUtils.withCluster {
            assertThrows<BucketNotFoundException> {
                it.buckets.getBucket(BUCKET_NAME)
            }
        }
    }

}
