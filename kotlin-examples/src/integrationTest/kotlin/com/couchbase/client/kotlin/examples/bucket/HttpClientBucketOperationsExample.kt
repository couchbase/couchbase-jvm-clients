package com.couchbase.client.kotlin.examples.bucket

import com.couchbase.client.kotlin.examples.util.ConnectionUtils
import com.couchbase.client.kotlin.http.HttpBody
import com.couchbase.client.kotlin.http.HttpTarget
import com.couchbase.client.kotlin.http.NameValuePairs
import com.couchbase.client.kotlin.http.formatPath
import com.couchbase.client.kotlin.manager.bucket.BucketSettings
import com.couchbase.client.kotlin.util.StorageSize
import com.couchbase.client.kotlin.util.StorageSizeUnit
import com.google.gson.Gson
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

private const val BUCKET_NAME = "temp-bucket-http-client"

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class HttpClientBucketOperationsExample {

    @Test
    @Order(1)
    fun `create bucket`() {
        ConnectionUtils.withHttpClient { cluster, httpClient ->
            httpClient.post(
                target = HttpTarget.manager(),
                path = "/pools/default/buckets",
                body = HttpBody.form(
                    "name" to BUCKET_NAME,
                    "ramQuotaMB" to "128",
                    "replicaNumber" to "1",
                    "bucketType" to "couchbase",
                    "flushEnabled" to "1",
                )
            )
            assertTrue(cluster.buckets.getAllBuckets().any { it.name == BUCKET_NAME })
        }
    }

    @Test
    @Order(2)
    fun `update bucket`() {
        ConnectionUtils.withHttpClient { cluster, httpClient ->
            httpClient.post(
                target = HttpTarget.manager(),
                path = formatPath("/pools/default/buckets/{}", BUCKET_NAME),
                body = HttpBody.form(
                    "ramQuotaMB" to "256",
                    "replicaNumber" to "2",
                )
            )
            val updatedBucketSettings = cluster.buckets.getAllBuckets().find { it.name == BUCKET_NAME }
            assertEquals(StorageSize(256, StorageSizeUnit.MEBIBYTES), updatedBucketSettings?.ramQuota)
            assertEquals(2, updatedBucketSettings?.replicas)
        }
    }

    @Test
    @Order(3)
    fun `get bucket info`() {
        ConnectionUtils.withHttpClient { _, httpClient ->
            val response = httpClient.get(
                target = HttpTarget.manager(),
                path = formatPath("/pools/default/buckets/{}", BUCKET_NAME)
            )
            val gson = Gson()
            val bucketData = gson.fromJson(response.contentAsString, Map::class.java)
            assertEquals(BUCKET_NAME, bucketData["name"])
        }
    }

    @Test
    @Order(4)
    fun `get all buckets info`() {
        ConnectionUtils.withHttpClient { _, httpClient ->
            val response = httpClient.get(
                target = HttpTarget.manager(),
                path = "/pools/default/buckets"
            )
            val gson = Gson()
            val bucketSettingsList = gson.fromJson(response.contentAsString, Array<BucketSettings>::class.java)
            assertTrue(bucketSettingsList.any { it.name == BUCKET_NAME })
        }
    }

    @Test
    @Order(5)
    fun `get bucket statistics`() {
        ConnectionUtils.withHttpClient { _, httpClient ->
            val response = httpClient.get(
                target = HttpTarget.manager(),
                path = formatPath("/pools/default/buckets/{}/stats", BUCKET_NAME),
                queryString = NameValuePairs.of(
                    "zoom" to "minute"
                )
            )
            val gson = Gson()
            val bucketStats = gson.fromJson(response.contentAsString, Map::class.java)
            assertTrue(bucketStats.containsKey("op"))
        }
    }

    @Test
    @Order(6)
    fun `flush bucket`() {
        ConnectionUtils.withHttpClient { _, httpClient ->
            httpClient.post(
                target = HttpTarget.manager(),
                path = formatPath("/pools/default/buckets/{}/controller/doFlush", BUCKET_NAME)
            )
        }
    }

    @Test
    @Order(7)
    fun `delete bucket`() {
        ConnectionUtils.withHttpClient { cluster, httpClient ->
            httpClient.delete(
                target = HttpTarget.manager(),
                path = formatPath("/pools/default/buckets/{}", BUCKET_NAME)
            )
            assertTrue(cluster.buckets.getAllBuckets().none { it.name == BUCKET_NAME })
        }
    }

}
