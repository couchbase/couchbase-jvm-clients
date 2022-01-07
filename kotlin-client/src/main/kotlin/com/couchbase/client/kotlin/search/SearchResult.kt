/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.search

import kotlinx.coroutines.flow.Flow

public sealed interface FacetResultAccessor {
    public val facets: List<FacetResult<*>>

    /**
     * Returns the [TermFacetResult] with the same name as [facet], or null if
     * none of the search results matched the facet.
     */
    public operator fun get(facet: TermFacet): TermFacetResult?

    /**
     * Returns the [NumericFacetResult] with the same name as [facet], or null if
     * none of the search results matched the facet.
     */
    public operator fun get(facet: NumericFacet): NumericFacetResult?

    /**
     * Returns the [DateFacetResult] with the same name as [facet], or null if
     * none of the search results matched the facet.
     */
    public operator fun get(facet: DateFacet): DateFacetResult?
}

public class SearchResult(
    public val rows: List<SearchRow>,
    public val metadata: SearchMetadata,
) : FacetResultAccessor by metadata {
    override fun toString(): String = "SearchResult(rows=$rows, metadata=$metadata)"
}

/**
 * Collects a search Flow into a QueryResult. Should only be called
 * if the search results are expected to fit in memory.
 */
public suspend fun Flow<SearchFlowItem>.execute(): SearchResult {
    val rows = ArrayList<SearchRow>()
    val meta = execute { rows.add(it) }
    return SearchResult(rows, meta);
}

/**
 * Collects a search Flow, passing each result row to the given lambda.
 * Returns metadata about the query.
 */
public suspend inline fun Flow<SearchFlowItem>.execute(
    crossinline rowAction: suspend (SearchRow) -> Unit,
): SearchMetadata {

    var meta: SearchMetadata? = null

    collect { item ->
        when (item) {
            is SearchRow -> rowAction.invoke(item)
            is SearchMetadata -> meta = item
        }
    }

    check(meta != null) { "Expected search flow to have metadata, but none found." }
    return meta!!
}
