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

import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.kv.Expiry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.seconds


@OptIn(VolatileCouchbaseApi::class)
internal fun counterRateLimiting(collection: Collection) {
    // Limiting the rate of requests using an expiring counter.

    val requestLimit = 2u
    val period = 3.seconds
    val counter = collection.counter("requests", expiry = Expiry.of(period))

    runBlocking {
        repeat(30) {
            // simulate the arrival of a request
            val requestCount = counter.incrementAndGet()

            if (requestCount > requestLimit) {
                println("REJECTING request; received more than $requestLimit requests in $period")
            } else {
                println("servicing request $requestCount in this period")
            }

            delay(1000)
        }
        println("done")
    }
}

@OptIn(VolatileCouchbaseApi::class)
internal fun counterGenerateDocumentIds(collection: Collection) {
    // Generate unique document IDs using a counter.

    // Include the name of the current datacenter to ensure IDs are
    // globally unique when using Cross-Datacenter Replication (XDCR).
    val datacenter = "dc1"

    val idCounter = collection.counter("widgetIdCounter-$datacenter")

    runBlocking {
        for (i in 1..5) {
            val docId = "widget-${datacenter}-${idCounter.incrementAndGet()}"
            println(docId)
            collection.insert(docId, content = "My ID is $docId")
        }
    }
}
