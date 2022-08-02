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
import com.couchbase.client.protocol.shared.ClusterConnectionCreateRequest
import com.couchbase.client.protocol.shared.DocLocation

class ClusterConnection(req: ClusterConnectionCreateRequest) {
    private val hostname = "couchbase://" + req.clusterHostname
    private val cluster = Cluster.connect(hostname, req.clusterUsername, req.clusterPassword)
    private val bucketCache: MutableMap<String, com.couchbase.client.kotlin.Bucket> = mutableMapOf()

    fun collection(loc: DocLocation): com.couchbase.client.kotlin.Collection {
        var coll: com.couchbase.client.protocol.shared.Collection?
        if (loc.hasPool()) coll = loc.pool.collection
        else if (loc.hasSpecific()) coll = loc.specific.collection
        else if (loc.hasUuid()) coll = loc.uuid.collection
        else throw UnsupportedOperationException("Unknown DocLocation type")

        val bucket = bucketCache.getOrPut(coll.bucketName) { cluster.bucket(coll.bucketName) }
        return bucket.scope(coll.scopeName).collection(coll.collectionName)
    }
}