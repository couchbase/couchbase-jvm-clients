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

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.execute

@Suppress("UNUSED_VARIABLE")
@OptIn(VolatileCouchbaseApi::class)
internal suspend fun singleValueQueryAnonymous(cluster: Cluster) {
    val count = cluster
        .query("select count(*) from `travel-sample`")
        .execute()
        .valueAs<Long>() // uses default name "$1"
}

@Suppress("UNUSED_VARIABLE")
@OptIn(VolatileCouchbaseApi::class)
internal suspend fun singleValueQueryNamed(cluster: Cluster) {
    val count = cluster
        .query("select count(*) as count from `travel-sample`")
        .execute()
        .valueAs<Long>("count")
}

@Suppress("UNUSED_VARIABLE")
internal suspend fun bufferedQuery(cluster: Cluster) {
    val result: QueryResult = cluster
        .query("select * from `travel-sample` limit 10")
        .execute()
    result.rows.forEach { println(it) }
    println(result.metadata)
}

@Suppress("UNUSED_VARIABLE")
internal suspend fun streamingQuery(cluster: Cluster) {
    val metadata: QueryMetadata = cluster
        .query("select * from `travel-sample`")
        .execute { row -> println(row) }
    println(metadata)
}
