/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.http.CouchbaseHttpClient
import com.couchbase.client.kotlin.http.HttpBody
import com.couchbase.client.kotlin.http.HttpTarget
import com.couchbase.client.kotlin.http.NameValuePairs
import com.couchbase.client.kotlin.http.formatPath
import kotlinx.coroutines.runBlocking

internal fun httpClientGetWithQueryParameters(httpClient: CouchbaseHttpClient) {
    runBlocking {
        val response = httpClient.get(
            target = HttpTarget.manager(), // port 8091 (or equivalent)
            path = formatPath("/some/arbitrary/path"),
            queryString = NameValuePairs.of(
                "color" to "green",
                "number" to 37,
                "ingredients" to "sugar & spice" // values are automatically url-encoded
            )
        )
        println(response)
    }
}

internal fun httpClientPostWithFormData(httpClient: CouchbaseHttpClient) {
    runBlocking {
        val response = httpClient.post(
            target = HttpTarget.manager(), // port 8091 (or equivalent)
            path = "/some/arbitrary/path",
            body = HttpBody.form(
                "color" to "green",
                "number" to 37,
                "ingredients" to "sugar & spice" // values are automatically url-encoded
            )
        )
        println(response)
    }
}

internal fun httpClientPostWithJsonBody(httpClient: CouchbaseHttpClient) {
    runBlocking {
        val response = httpClient.post(
            target = HttpTarget.manager(), // port 8091 (or equivalent)
            path = formatPath("/some/arbitrary/path"),
            body = HttpBody.json("{\"foo\":\"bar\"}")
        )
        println(response)
    }
}

internal fun httpClientGetBucketStats() {
    // Using the Couchbase HTTP Client to interact with the
    // Couchbase REST API.

    // Assumes you have Couchbase running locally
    // and the travel-sample bucket installed.

    val cluster = Cluster.connect("127.0.0.1", "Administrator", "password")
    val httpClient = cluster.httpClient
    val bucketName = "travel-sample"
    try {
        runBlocking {
            // get bucket stats
            val response = httpClient.get(
                target = HttpTarget.manager(), // port 8091 (or equivalent)
                path = formatPath("/pools/default/buckets/{}/stats", bucketName)
            )

            with(response) {
                check(success) { "Request failed with status code $statusCode and content $contentAsString" }
                println(contentAsString)
            }
        }
    } finally {
        runBlocking { cluster.disconnect() }
    }
}
