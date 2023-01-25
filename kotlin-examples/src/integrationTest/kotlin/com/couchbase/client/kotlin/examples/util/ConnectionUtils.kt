package com.couchbase.client.kotlin.examples.util

import com.couchbase.client.kotlin.Bucket
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.http.CouchbaseHttpClient
import kotlinx.coroutines.runBlocking

/**
 *
 * Small utils for testing purposes.
 *
 */

class ConnectionUtils {
    companion object {
        @JvmStatic
        fun getCluster(): Cluster {
            return Cluster.connect("couchbase://127.0.0.1", "Administrator", "password")
        }

        /**
         *
         * Since most of the operations are suspendable, we need to wrap them in runBlocking
         *
         */

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

        @JvmStatic
        fun withDefaultCollection(bucketName: String, block: suspend (Cluster, com.couchbase.client.kotlin.Collection) -> Unit) {
            withBucket(bucketName) { cluster, bucket ->
                val collection = bucket.defaultCollection()
                block(cluster, collection)
            }
        }

        @JvmStatic
        fun withHttpClient(block: suspend (Cluster, CouchbaseHttpClient) -> Unit) {
            withCluster {
                block(it, it.httpClient)
            }
        }

    }

}
