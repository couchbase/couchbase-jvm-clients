/*
 * Copyright (c) 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.examples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.kv.GetResult

/**
 * This example connects to a bucket, opens a collection, and performs a simple operation.
 * <p>
 * If this program does not run properly, make sure the "travel-sample" bucket is
 * present in your cluster. Also check the address in the connection string,
 * and the username and password.
 */

private const val HOST: String = "couchbase://127.0.0.1"
private const val USER: String = "Administrator"
private const val PASSWORD: String = "password"
private const val BUCKET: String = "travel-sample"
private const val DOC_ID: String = "airport_1291"
suspend fun main() {
    // Connect to the cluster with a connection string and credentials.
    val cluster = Cluster.connect(
        connectionString = HOST,
        username = USER,
        password = PASSWORD
    )
    // Open an existing bucket.
    val bucket = cluster.bucket(BUCKET)
    // Open a collection. Here it's the default collection, which also works with
    // old versions of Couchbase Server that do not support user-defined collections.
    val collection: Collection = bucket.defaultCollection()
    // Fetch a document from the travel-sample bucket.
    val airport: GetResult = collection.get(DOC_ID)
    // Print the fetched document.
    System.err.println("*** Got document: $airport")

    // Close network connections and release resources.
    cluster.disconnect()
}

