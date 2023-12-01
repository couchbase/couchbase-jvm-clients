/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin

import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.kotlin.http.CouchbaseHttpClient
import com.couchbase.client.kotlin.http.CouchbaseHttpResponse
import com.couchbase.client.kotlin.http.HttpBody
import com.couchbase.client.kotlin.http.HttpTarget
import com.couchbase.client.kotlin.http.formatPath
import com.couchbase.client.kotlin.kv.MutationState
import com.couchbase.client.kotlin.search.SearchQuery.Companion.queryString
import com.couchbase.client.kotlin.search.SearchScanConsistency.Companion.consistentWith
import com.couchbase.client.kotlin.search.execute
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.util.waitForService
import com.couchbase.client.test.Capabilities.SEARCH
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds


@Disabled("due to flakiness in CI environment")
@IgnoreWhen(missesCapabilities = [SEARCH], clusterVersionIsBelow = ConsistencyUtil.CLUSTER_VERSION_MB_50101)
internal class SearchIntegrationTest : KotlinIntegrationTest() {

    private val indexName by lazy { "idx-" + config().bucketname() }

    @BeforeAll
    fun setup(): Unit = runBlocking {
        bucket.waitForService(ServiceType.SEARCH)

        cluster.httpClient.uploadSearchIndex(
            name = indexName,
            indexJson = Mapper.encodeAsString(mapOf(
                "name" to indexName,
                "sourceName" to bucket.name,
                "type" to "fulltext-index",
                "sourceType" to "couchbase",
            ))
        )
        cluster.waitForIndex(indexName)
    }


    @AfterAll
    fun tearDown(): Unit = runBlocking {
        cluster.httpClient.dropSearchIndex(indexName)
    }

    @Test
    fun `simple search`(): Unit = runBlocking {
        val docId = UUID.randomUUID().toString()
        retry {
            val tokens = MutationState()
            tokens.add(collection.upsert(docId, mapOf("name" to "michael")))
            try {
                val searchResult = cluster.searchQuery(
                    indexName = indexName,
                    query = queryString("michael"),
                    consistency = consistentWith(tokens),
                ).execute()

                assertEquals(listOf(docId), searchResult.rows.map { it.id })

            } finally {
                collection.remove(docId)
            }
        }
    }
}

// doesn't really work :-/
private suspend fun Cluster.waitForIndex(
    indexName: String,
    timeout: Duration = 1.minutes,
): Cluster = withTimeout(timeout) {
    ConsistencyUtil.waitUntilSearchIndexPresent(core, indexName)
    while (true) {
        val result = httpClient.get(
            HttpTarget.search(),
            formatPath("api/index/{}", indexName),
        )
        val planPIndexes = Mapper.decodeIntoTree(result.content).path("planPIndexes")
        if (planPIndexes.isArray) return@withTimeout this@waitForIndex
        delay(1.seconds)
    }
    @Suppress("UNREACHABLE_CODE")
    this@waitForIndex
}

private suspend fun CouchbaseHttpClient.uploadSearchIndex(name: String, indexJson: String) {
    put(
        target = HttpTarget.search(),
        path = formatPath("api/index/{}", name),
        body = HttpBody.json(indexJson),
    ).checkStatus()
}

private suspend fun CouchbaseHttpClient.dropSearchIndex(name: String) {
    delete(
        target = HttpTarget.search(),
        path = formatPath("api/index/{}", name),
    ).checkStatus()
}

private fun CouchbaseHttpResponse.checkStatus(): CouchbaseHttpResponse {
    if (!success) throw RuntimeException("$statusCode : $contentAsString")
    return this
}

internal suspend fun <T> retry(
    timeout: Duration = 1.minutes,
    block: suspend () -> T,
): T {
    return withTimeout(timeout) {
        var result: T
        while (true) {
            try {
                result = block()
                break
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                println("Retrying due to: $t")
                delay(1.seconds)
            }
        }
        result
    }
}
