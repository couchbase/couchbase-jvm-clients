package com.couchbase.client.kotlin.examples.util

import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.Cluster
import kotlinx.coroutines.runBlocking

class ConnectionUtils {
    companion object {
        @JvmStatic
        fun getCluster(): Cluster {
            return Cluster.connect("couchbase://127.0.0.1", "Administrator", "password")
        }

        @JvmStatic
        fun withCluster(block: suspend (Cluster) -> Unit) {
            runBlocking {
                val cluster = getCluster()
                try {
                    block(cluster)
                } finally {
                    cluster.disconnect()
                }
            }
        }

        @JvmStatic
        fun withBucket(bucketName: String, block: suspend (Cluster, Bucket) -> Unit) {
            withCluster { cluster ->
                val bucket = cluster.bucket(bucketName)
                block(cluster, bucket)
            }
        }

    }

}
