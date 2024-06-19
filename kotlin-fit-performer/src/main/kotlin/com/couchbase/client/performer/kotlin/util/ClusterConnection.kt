/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.performer.kotlin.util

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory
import com.couchbase.client.core.env.SecurityConfig
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.env.dsl.TrustSource
import com.couchbase.client.protocol.shared.ClusterConfig
import com.couchbase.client.protocol.shared.ClusterConnectionCreateRequest
import com.couchbase.client.protocol.shared.DocLocation
import kotlin.io.path.Path
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import com.couchbase.client.protocol.shared.Collection as FitCollection

class ClusterConnection(req: ClusterConnectionCreateRequest) {
    val cluster = Cluster.connect(
        connectionString = req.clusterHostname,
        username = req.clusterUsername,
        password = req.clusterPassword,
    ) {
        with(req.clusterConfig) {
            security {
                enableTls = useTls
                trust = trustSource
            }

            timeout {
                if (hasKvConnectTimeoutSecs()) connectTimeout = kvConnectTimeoutSecs.seconds
                if (hasKvTimeoutMillis()) kvTimeout = kvTimeoutMillis.milliseconds
                if (hasKvDurableTimeoutMillis()) kvTimeout = kvDurableTimeoutMillis.milliseconds
                if (hasViewTimeoutSecs()) viewTimeout = viewTimeoutSecs.seconds
                if (hasQueryTimeoutSecs()) queryTimeout = queryTimeoutSecs.seconds
                if (hasSearchTimeoutSecs()) searchTimeout = searchTimeoutSecs.seconds
                if (hasManagementTimeoutSecs()) managementTimeout = managementTimeoutSecs.seconds

                // [if:1.1.6]
                if (hasKvScanTimeoutSecs()) kvScanTimeout = kvScanTimeoutSecs.seconds
                // [end]
            }
        }
    }

    fun collection(loc: DocLocation): Collection {
        val coll: FitCollection = when {
            loc.hasPool() -> loc.pool.collection
            loc.hasSpecific() -> loc.specific.collection
            loc.hasUuid() -> loc.uuid.collection
            else -> throw UnsupportedOperationException("Unknown DocLocation type: $loc")
        }

        return collection(coll)
    }

    fun collection(coll: FitCollection): Collection {
        return cluster.bucket(coll.bucketName)
            .scope(coll.scopeName)
            .collection(coll.collectionName)
    }
}

private val ClusterConfig.trustSource: TrustSource
    get() = when {
        hasInsecure() && insecure -> TrustSource.factory(InsecureTrustManagerFactory.INSTANCE)
        hasCertPath() -> TrustSource.certificate(Path(certPath))
        hasCert() -> TrustSource.certificates(SecurityConfig.decodeCertificates(listOf(cert)))
        else -> TrustSource.certificates(SecurityConfig.defaultCaCertificates())
    }
