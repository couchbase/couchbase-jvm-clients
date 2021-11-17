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

import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.util.use
import com.couchbase.client.kotlin.util.withSystemProperty
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// Prevent timeouts against the Mock from interfering
// with Gerrit verification.  Remove when mock is fixed:
//    http://review.couchbase.org/c/CouchbaseMock/+/148081
@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
private class ClusterIntegrationTest : KotlinIntegrationTest() {

    @Test
    fun `connection string parameters override cluster environment`() = runBlocking {
        Cluster.connect("$connectionString?io.captureTraffic=KV", authenticator) {
            io { captureTraffic(ServiceType.QUERY) }
        }.use { cluster ->
            assertEquals(
                setOf(ServiceType.KV),
                cluster.env.ioConfig().servicesToCapture()
            )
        }
    }

    @Test
    fun `system properties override cluster environment`() = runBlocking {
        withSystemProperty("com.couchbase.env.io.captureTraffic", "KV") {
            connect {
                io { captureTraffic(ServiceType.QUERY) }
            }.use { cluster ->
                assertEquals(
                    setOf(ServiceType.KV),
                    cluster.env.ioConfig().servicesToCapture()
                )
            }
        }
    }

    @Test
    fun `system properties override connection string`() = runBlocking {
        withSystemProperty("com.couchbase.env.io.captureTraffic", "KV") {
            Cluster.connect("$connectionString?io.captureTraffic=QUERY", authenticator) {
                io { captureTraffic(ServiceType.ANALYTICS) }
            }.use { cluster ->
                assertEquals(
                    setOf(ServiceType.KV),
                    cluster.env.ioConfig().servicesToCapture()
                )
            }
        }
    }

    @Test
    fun `can wait until ready`(): Unit = runBlocking {
        connect().use { cluster ->
            cluster
                .waitUntilReady(30.seconds)
                .bucket(config().bucketname())
                .waitUntilReady(30.seconds)
        }
    }

    @Test
    fun `cluster waitUntilReady throws UnambiguousTimeoutException`(): Unit = runBlocking {
        Cluster.connect("127.0.0.1", "bogusUsername", "bogusPassword").use { cluster ->
            assertThrows<UnambiguousTimeoutException> {
                cluster.waitUntilReady(Duration.ZERO)
            }
        }
    }

    @Test
    fun `bucket waitUntilReady throws UnambiguousTimeoutException`(): Unit = runBlocking {
        connect().use { cluster ->
            assertThrows<UnambiguousTimeoutException> {
                cluster.bucket("does-not-exist")
                    .waitUntilReady(Duration.ZERO)
            }
        }
    }

    @Test
    fun `bucket instances are cached`() {
        connect().use { cluster ->
            config().bucketname().let {
                assertSame(cluster.bucket(it), cluster.bucket(it))
            }
        }
    }
}

