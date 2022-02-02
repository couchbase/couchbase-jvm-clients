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

package com.couchbase.client.kotlin.manager.http

import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class CouchbaseHttpClientIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `formatPath URL-encodes arguments`() {
        assertEquals(
            "/foo/hello%20world/bar/a%2Fb",
            formatPath("/foo/{}/bar/{}", "hello world", "a/b")
        )

        assertThrows<IllegalArgumentException> { formatPath("{}") }
        assertThrows<IllegalArgumentException> { formatPath("foo", "bar") }
    }

    @Test
    fun `formatPath validates argument count`() {
        assertThrows<IllegalArgumentException> { formatPath("{}") }
        assertThrows<IllegalArgumentException> { formatPath("foo", "bar") }
    }

    @Test
    fun `formatPath can also build query string if you really want`() {
        assertEquals(
            "/foo/bar?greeting=hello%20world",
            formatPath("/foo/{}?greeting={}", "bar", "hello world")
        )
    }

    @Test
    fun `NameValuePairs URL-encodes values`() {
        assertEquals("greeting=hello%20world", NameValuePairs.of("greeting" to "hello world").urlEncoded)
    }

    @Test
    fun `can pre-encoded NameValuePairs`() {
        assertEquals("greeting=hello%20world", NameValuePairs.ofPreEncoded("greeting=hello%20world").urlEncoded)
    }

    @Test
    fun `can get bucket`(): Unit = runBlocking {
        val response = cluster.httpClient.get(
            target = HttpTarget.manager(),
            path = formatPath("/pools/default/buckets/{}", config().bucketname()),
        )

        with(response) {
            assertTrue(success, "Request failed with status code $statusCode and content: $contentAsString")
            assertEquals(
                config().bucketname(),
                Mapper.decodeIntoTree(content).path("name").textValue()
            )
        }
    }
}
