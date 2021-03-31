/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.samples

import com.couchbase.client.core.env.NetworkResolution
import com.couchbase.client.core.env.SecurityConfig
import com.couchbase.client.core.retry.FailFastRetryStrategy
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.codec.RawJsonTranscoder
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.env.dsl.TrustSource
import com.couchbase.client.kotlin.query.execute
import kotlinx.coroutines.runBlocking
import java.nio.file.Paths
import java.time.Duration
import java.util.*

@Suppress("UNUSED_VARIABLE")
internal fun quickstart() {
    // Assumes you have Couchbase running locally
    // and the "travel-sample" sample bucket loaded.

    // Connect and open a bucket
    val cluster = Cluster.connect("127.0.0.1", "Administrator", "password")
    try {
        val bucket = cluster.bucket("travel-sample")
        val collection = bucket.defaultCollection()

        runBlocking {
            // Perform a N1QL query
            val queryResult = cluster
                .query("select * from `travel-sample` limit 3")
                .execute()
            queryResult.rows.forEach { println(it) }
            println(queryResult.metadata)

            // Get a document from the K/V service
            val getResult = collection.get("airline_10")
            println(getResult)
            println(getResult.contentAs<Map<String, Any?>>())
        }
    } finally {
        runBlocking { cluster.disconnect() }
    }
}

@Suppress("UNUSED_VARIABLE")
internal fun configureTlsUsingDsl() {
    val cluster = Cluster.connect("localhost", "Administrator", "password") {
        security {
            enableTls = true

            // see TrustSource for more ways to specify trusted certificates
            trust = TrustSource.trustStore(
                Paths.get("/path/to/truststore.jks"),
                "password"
            )
        }
    }
}

@Suppress("UNUSED_VARIABLE")
internal fun configureTlsUsingBuilder() {
    // connect() has overloads that accept ClusterEnvironment.Builder
    // in case you don't want to use the cluster environment config DSL.
    val cluster = Cluster.connect(
        "localhost", "Administrator", "password",
        ClusterEnvironment.builder()
            .securityConfig(
                SecurityConfig
                    .enableTls(true)
                    .trustStore(
                        Paths.get("/path/to/truststore.jks"),
                        "password",
                        Optional.empty()
                    )
            )
    )
}

@Suppress("UNUSED_VARIABLE")
internal fun configureManyThingsUsingDsl() {
    val cluster = Cluster.connect("localhost", "Administrator", "password") {
        transcoder = RawJsonTranscoder

        ioEnvironment {
            enableNativeIo = false
        }

        io {
            enableDnsSrv = false
            networkResolution = NetworkResolution.EXTERNAL
            tcpKeepAliveTime = Duration.ofSeconds(45)

            captureTraffic(ServiceType.KV, ServiceType.QUERY)

            // specify defaults before customizing individual breakers
            allCircuitBreakers {
                enabled = true
                volumeThreshold = 30
                errorThresholdPercentage = 20
                rollingWindow = Duration.ofSeconds(30)
            }

            kvCircuitBreaker {
                enabled = false
            }

            queryCircuitBreaker {
                rollingWindow = Duration.ofSeconds(10)
            }
        }

        timeout {
            kvTimeout = Duration.ofSeconds(3)
            kvDurableTimeout = Duration.ofSeconds(20)
            connectTimeout = Duration.ofSeconds(15)
        }

        orphanReporter {
            emitInterval = Duration.ofSeconds(20)
        }
    }
}

@Suppress("UNUSED_VARIABLE")
internal fun preconfigureBuilderUsingDsl() {
    val builder = ClusterEnvironment.builder {
        retryStrategy = FailFastRetryStrategy.INSTANCE
        io {
            maxHttpConnections = 16
        }
    }
}

@Suppress("UNUSED_VARIABLE")
internal fun createBuilderWithDefaultSettings() {
    val builder = ClusterEnvironment.builder()
}
