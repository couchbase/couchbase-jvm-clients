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

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.protocol.shared.ClusterConnectionCreateRequest
import com.couchbase.client.protocol.shared.DocLocation
import com.couchbase.client.protocol.shared.Collection as FitCollection

class ClusterConnection(req: ClusterConnectionCreateRequest) {
    val cluster = Cluster.connect(
        connectionString = req.clusterHostname,
        username = req.clusterUsername,
        password = req.clusterPassword,
    )

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
