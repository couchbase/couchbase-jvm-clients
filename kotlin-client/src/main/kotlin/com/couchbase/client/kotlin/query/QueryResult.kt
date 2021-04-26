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

package com.couchbase.client.kotlin.query

import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect

public class QueryResult(
    public val rows: List<QueryRow>,
    public val metadata: QueryMetadata,
) {
    override fun toString(): String {
        return "QueryResult(rows=$rows, metadata=$metadata)"
    }

    /**
     * Returns a field value from a result with exactly one row.
     * Useful for getting the result of an aggregating function.
     *
     * @param name the name of the field to extract.
     *
     * @param serializer for converting the field value to the requested type.
     * Defaults to the [QueryRow]'s serializer.
     *
     * @param T the serializer reads the field value as this type.
     *
     * @throws IllegalArgumentException if there is more than one result row
     * @throws NoSuchElementException if there is no field with the given name
     *
     * @sample com.couchbase.client.kotlin.samples.singleValueQueryAnonymous
     * @sample com.couchbase.client.kotlin.samples.singleValueQueryNamed
     */
    @VolatileCouchbaseApi
    public inline fun <reified T> valueAs(name: String = "$1", serializer: JsonSerializer? = null): T {
        val row = rows.single()

        val map = row.contentAs<Map<String, Any?>>()
        if (!map.containsKey(name)) throw NoSuchElementException("Result row does not have field $name ; specify one of ${map.keys}")
        val value = map[name]

        val valueJson = Mapper.encodeAsBytes(value)
        return (serializer ?: row.defaultSerializer).deserialize(valueJson, typeRef())
    }
}

/**
 * Collects a query Flow into a QueryResult. Should only be called
 * if the query results are expected to fit in memory.
 */
public suspend fun Flow<QueryFlowItem>.execute(): QueryResult {
    val rows = ArrayList<QueryRow>()
    val meta = execute { rows.add(it) }
    return QueryResult(rows, meta);
}

/**
 * Collects a query Flow, passing each result row to the given lambda.
 * Returns metadata about the query.
 */
public suspend inline fun Flow<QueryFlowItem>.execute(
    crossinline rowAction: suspend (QueryRow) -> Unit
): QueryMetadata {

    var meta: QueryMetadata? = null

    collect { item ->
        when (item) {
            is QueryRow -> rowAction.invoke(item)
            is QueryMetadata -> meta = item
        }
    }

    check(meta != null) { "Expected query flow to have metadata, but none found." }
    return meta!!
}
