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

// Some examples declare unused variables for illustrative purposes
@file:Suppress("UNUSED_VARIABLE")

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.query.QueryMetadata
import com.couchbase.client.kotlin.query.QueryParameters
import com.couchbase.client.kotlin.query.QueryResult
import com.couchbase.client.kotlin.query.execute
import kotlinx.serialization.Serializable

internal suspend fun singleValueQueryAnonymous(cluster: Cluster) {
    // Single-value query with anonymous result
    val count = cluster
        .query("select count(*) from `travel-sample`")
        .execute()
        .valueAs<Long>() // uses default name "$1"
}

internal suspend fun singleValueQueryNamed(cluster: Cluster) {
    // Single-value query with named result
    val count = cluster
        .query("select count(*) as count from `travel-sample`")
        .execute()
        .valueAs<Long>("count")
}

internal suspend fun bufferedQuery(cluster: Cluster) {
    // Buffered query, for when results are known to fit in memory
    val result: QueryResult = cluster
        .query("select * from `travel-sample` limit 10")
        .execute()
    result.rows.forEach { println(it) }
    println(result.metadata)
}

internal suspend fun streamingQuery(cluster: Cluster) {
    // Streaming query, for when result size is large or unbounded
    val metadata: QueryMetadata = cluster
        .query("select * from `travel-sample`")
        .execute { row -> println(row) }
    println(metadata)
}

internal suspend fun queryWithNamedParameters(cluster: Cluster) {
    // Query with named parameters
    val result: QueryResult = cluster
        .query(
            "select * from `travel-sample` where type = @type limit @limit",
            parameters = QueryParameters.named {
                param("type", "airline")
                param("limit", 3)
            }
        )
        .execute()

    result.rows.forEach { println(it) }
}

internal suspend fun queryWithPositionalParameters(cluster: Cluster) {
    // Query with positional parameters
    val result: QueryResult = cluster
        .query(
            "select * from `travel-sample` where type = ? limit ?",
            parameters = QueryParameters.positional {
                param("airline")
                param(3)
            }
        )
        .execute()

    result.rows.forEach { println(it) }
}

internal suspend fun queryWithNamedParameterBlock(cluster: Cluster) {
    // Query with named parameters from a parameter block
    @Serializable // (or whatever annotation your JsonSerializer requires)
    data class MyParameters(val type: String, val limit: Int)

    val result: QueryResult = cluster
        .query(
            "select * from `travel-sample` where type = @type limit @limit",
            parameters = QueryParameters.namedFrom(
                MyParameters(type = "airline", limit = 3)
            )
        )
        .execute()

    result.rows.forEach { println(it) }
}
