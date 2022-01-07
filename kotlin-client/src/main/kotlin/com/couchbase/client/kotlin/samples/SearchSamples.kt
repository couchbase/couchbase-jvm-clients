/*
 * Copyright 2022 Couchbase, Inc.
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
import com.couchbase.client.kotlin.search.Highlight
import com.couchbase.client.kotlin.search.NumericRange
import com.couchbase.client.kotlin.search.SearchFacet
import com.couchbase.client.kotlin.search.SearchQuery
import com.couchbase.client.kotlin.search.SearchResult
import com.couchbase.client.kotlin.search.SearchSort
import com.couchbase.client.kotlin.search.SearchSort.Companion.byField
import com.couchbase.client.kotlin.search.SearchSort.Companion.byId
import com.couchbase.client.kotlin.search.execute

internal suspend fun searchQuerySimple(cluster: Cluster) {
    // A simple search query against the "travel-sample" sample bucket.
    // Requires the "description" field to be indexed (stored).
    val result = cluster.searchQuery(
        indexName = "travel-sample-index-hotel-description",

        fields = listOf("*"), // return all stored fields

        query = SearchQuery.match(
            match = "beautiful", // look for this term
            field = "description", // limit the query to this field
        ),

        highlight = Highlight.html(), // return matching fragments
    ).execute()

    checkSearchQueryResultForPartialFailure(result)  // see other sample

    println(result.metadata)
    result.rows.forEach { row ->
        println(row.id)
        println(row.score)
        println(row.locations)
        println(row.fragments)
        println(row.fieldsAs<Map<String, Any?>>())
        println()
    }
}

internal suspend fun searchQueryWithFacets(cluster: Cluster) {
    // Searching with facets.

    // Assumes the "beer-sample" sample bucket is installed,
    // with a search index named "beer-search" where
    // "abv" and "category" are indexed as stored fields.

    // Count results that fall into these "alcohol by volume" ranges.
    // Optionally assign names to the ranges.
    val low = NumericRange.bounds(min = 0, max = 3.5, name = "low")
    val high = NumericRange.lowerBound(3.5, name = "high")
    val abv = SearchFacet.numeric(
        field = "abv",
        ranges = listOf(low, high),
        name = "Alcohol by volume",
    )

    // Find the 5 most frequent values in the "category" field.
    val beerType = SearchFacet.term("category", size = 5)

    val result = cluster.searchQuery(
        indexName = "beer-search",
        query = SearchQuery.matchAll(),
        facets = listOf(abv, beerType),
    ).execute()

    // Print all facet results. Results do not include empty facets
    // or ranges. Categories are ordered by size, descending.
    result.facets.forEach { facet ->
        println(facet.name)
        facet.categories.forEach { println("  $it") }
        facet.other.let { if (it > 0) println("  <other> ($it)") }
        println()
    }

    // Alternatively, print results for a specific facet:
    val abvResult = result[abv]
    if (abvResult == null) {
        println("No search results matched any of the 'abv' facet ranges.")
    } else {
        println("Alcohol by volume (again)")
        println(" low (${abvResult[low]?.count ?: 0})")
        println(" high (${abvResult[high]?.count ?: 0})")
        println()
    }
}

@SuppressWarnings("unused")
internal fun searchTieredSort() {
    // tiered sort
    val sort = byField("foo") then byId()

    // the same sort, but built from a list
    val sameSort = SearchSort.of(listOf(byField("foo"), byId()))
}

internal fun checkSearchQueryResultForPartialFailure(searchResult: SearchResult) {
    // Checking a SearchResult for partial failure
    val errors = searchResult.metadata.errors
    val metrics = searchResult.metadata.metrics

    if (errors.isNotEmpty()) {
        if (metrics.failedPartitions == metrics.totalPartitions)
            throw RuntimeException("Total failure. Errors: $errors")

        // Proceed with partial results (or fail if you want)
        println("Partial success. Errors: $errors")
    }
}
