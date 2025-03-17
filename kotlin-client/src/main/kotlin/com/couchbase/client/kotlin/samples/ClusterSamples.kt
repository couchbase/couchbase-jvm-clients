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

// several examples declare unused variables for illustrative purposes.
@file:Suppress("UNUSED_VARIABLE")

package com.couchbase.client.kotlin.samples

import com.couchbase.client.core.env.NetworkResolution
import com.couchbase.client.core.retry.FailFastRetryStrategy
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.codec.RawJsonTranscoder
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.env.dsl.TrustSource
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.query.execute
import kotlinx.coroutines.runBlocking
import java.nio.file.Paths
import java.util.Optional
import kotlin.time.Duration.Companion.seconds

internal fun quickstart() {
    // Quickstart

    // Assumes you have Couchbase running locally
    // and the "travel-sample" sample bucket loaded.

    // Connect and open a bucket
    val cluster = Cluster.connect("127.0.0.1", "Administrator", "password")
    try {
        val bucket = cluster.bucket("travel-sample")
        val collection = bucket.defaultCollection()

        runBlocking {
            // Perform a SQL++ query
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

internal fun configureTlsUsingDsl() {
    // Configure TLS using DSL
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

internal fun configureTlsUsingBuilder() {
    // Configure TLS using builder

    // connect() has overloads that accept ClusterEnvironment.Builder
    // in case you don't want to use the cluster environment config DSL.
    val cluster = Cluster.connect(
        "localhost", "Administrator", "password",
        ClusterEnvironment.builder()
            .securityConfig { security ->
                security
                    .enableTls(true)
                    .trustStore(
                        Paths.get("/path/to/truststore.jks"),
                        "password",
                        Optional.empty()
                    )
            }
    )
}

internal fun configurePreferredServerGroup() {
    // Configure the preferred server group
    val cluster = Cluster.connect("127.0.0.1", "Administrator", "password") {
        preferredServerGroup = "Group 1"
    }
}

internal fun configureManyThingsUsingDsl() {
    // Configure many things using DSL
    val cluster = Cluster.connect("localhost", "Administrator", "password") {
        applyProfile("wan-development")

        transcoder = RawJsonTranscoder

        ioEnvironment {
            enableNativeIo = false
        }

        io {
            enableDnsSrv = false
            networkResolution = NetworkResolution.EXTERNAL
            tcpKeepAliveTime = 45.seconds

            // To see traffic, must also set "com.couchbase.io" logging category to TRACE level.
            captureTraffic(ServiceType.KV, ServiceType.QUERY)

            // Specify defaults before customizing individual breakers
            allCircuitBreakers {
                enabled = true
                volumeThreshold = 30
                errorThresholdPercentage = 20
                rollingWindow = 30.seconds
            }

            kvCircuitBreaker {
                enabled = false
            }

            queryCircuitBreaker {
                rollingWindow = 10.seconds
            }
        }

        timeout {
            kvTimeout = 3.seconds
            kvDurableTimeout = 20.seconds
            connectTimeout = 15.seconds
        }

        transactions {
            durabilityLevel = Durability.majorityAndPersistToActive()

            cleanup {
                cleanupWindow = 30.seconds
            }
        }

        orphanReporter {
            emitInterval = 20.seconds
        }
    }
}

internal fun preconfigureBuilderUsingDsl() {
    // Preconfigure builder using DSL
    val builder = ClusterEnvironment.builder {
        retryStrategy = FailFastRetryStrategy.INSTANCE
        io {
            maxHttpConnections = 16
        }
    }
}

internal fun createBuilderWithDefaultSettings() {
    // Create builder with default settings
    val builder = ClusterEnvironment.builder()
}
