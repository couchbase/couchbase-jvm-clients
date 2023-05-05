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

import com.couchbase.client.core.api.search.CoreSearchMetaData
import com.couchbase.client.core.api.search.result.CoreDateRangeSearchFacetResult
import com.couchbase.client.core.api.search.result.CoreNumericRangeSearchFacetResult
import com.couchbase.client.core.api.search.result.CoreSearchFacetResult
import com.couchbase.client.core.api.search.result.CoreSearchMetrics
import com.couchbase.client.core.api.search.result.CoreSearchRow
import com.couchbase.client.core.api.search.result.CoreSearchRowLocations
import com.couchbase.client.core.api.search.result.CoreTermSearchFacetResult
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

public sealed class SearchFlowItem

/**
 * One row of a Full-Text Search result.
 *
 * Represents a document matching the query condition.
 *
 * @property fragments A multimap from field name to snippets of the field content with search terms highlighted.
 */
public class SearchRow internal constructor(
    /**
     * Name of this index this row came from.
     */
    public val index: String,

    /**
     * Document ID.
     */
    public val id: String,

    /**
     * Score assigned to this document (higher means better match).
     */
    public val score: Double,

    /**
     * Bytes of a JSON Object explaining how the score was calculated,
     * or an empty array if the `explain` parameter was false.
     *
     * The structure of the explanation JSON is unspecified,
     * and is not part of the public committed API.
     */
    public val explanation: ByteArray,

    /**
     * Where the match terms appear in the document.
     */
    public val locations: List<SearchRowLocation>,

    /**
     * Identifies this row's position in the full, unpaginated search results.
     * In order for this to be useful, the query's sort must impose a total
     * ordering on the result set. This is typically done by including `byId()`
     * as the last sort tier.
     *
     * @sample com.couchbase.client.kotlin.samples.searchTieredSort
     *
     * @see SearchPage.searchAfter
     * @see SearchPage.searchBefore
     */
    public val keyset: SearchKeyset,

    /**
     * A map from field name to a list of text fragments from that field containing
     * one or more highlighted match terms.
     *
     * @return the fragments as a {@link Map}. Keys are the fields.
     */
    public val fragments: Map<String, List<String>>,

    /**
     * The bytes of a JSON Object that has the requested document fields,
     * or null if no fields were requested (or none of the requested fields are stored).
     *
     * @see fieldsAs
     */
    public val fields: ByteArray?,

    @property:PublishedApi internal val defaultSerializer: JsonSerializer,
) : SearchFlowItem() {

    public companion object {
        internal fun from(core: CoreSearchRow, defaultSerializer: JsonSerializer): SearchRow {
            return SearchRow(
                index = core.index(),
                id = core.id(),

                score = core.score(),
                explanation = core.explanation(),

                locations = parseLocations(core.locations().orElse(null)),

                keyset = SearchKeyset(core.keyset().keys()),

                fragments = core.fragments(),

                fields = core.fields(),
                defaultSerializer = defaultSerializer,
            )
        }

        private fun parseLocations(core: CoreSearchRowLocations?): List<SearchRowLocation> {
            if (core == null) return emptyList()
            return core.all.map { SearchRowLocation(it) }
        }
    }

    /**
     * Returns [fields] deserialized into an object of the requested type,
     * or null if there are no fields.
     *
     * @param serializer Serializer to use for the conversion. Defaults to the
     * serialized specified in the call to `searchQuery`.
     */
    public inline fun <reified T> fieldsAs(serializer: JsonSerializer? = null): T? {
        if (fields == null) return null
        return (serializer ?: defaultSerializer).deserialize(fields, typeRef())
    }

    override fun toString(): String {
        return "SearchRow(" +
                "index='$index'" +
                ", id='$id'" +
                ", score=$score" +
                ", explanation=${String(explanation)}" +
                ", locations=$locations" +
                ", keyset=$keyset" +
                ", fragments=$fragments" +
                ", fields=${fields?.let { String(fields) } ?: "null"}" +
                ")"
    }
}

public class SearchMetrics internal constructor(
    core: CoreSearchMetrics,
) {
    public val took: Duration = core.took().toKotlinDuration()
    public val totalRows: Long = core.totalRows()
    public val maxScore: Double = core.maxScore()

    public val successfulPartitions: Int = core.successPartitionCount().toInt()
    public val failedPartitions: Int = core.errorPartitionCount().toInt()
    public val totalPartitions: Int = core.totalPartitionCount().toInt()

    override fun toString(): String {
        return "SearchMetrics(took=$took, totalRows=$totalRows, maxScore=$maxScore, successfulPartitions=$successfulPartitions, failedPartitions=$failedPartitions, totalPartitions=$totalPartitions)"
    }
}

/**
 * Metadata about query execution. Always the last item in the flow.
 */
public class SearchMetadata(
    meta: CoreSearchMetaData,
    coreFacets: Map<String, CoreSearchFacetResult>,
) : SearchFlowItem(), FacetResultAccessor {

    public val metrics: SearchMetrics = SearchMetrics(meta.metrics())

    /**
     * Map from failed partition name to error message.
     */
    public val errors: Map<String, String> = meta.errors()

    public override val facets: List<FacetResult<*>> = coreFacets.values.map { core ->
        when (core) {
            is CoreDateRangeSearchFacetResult -> DateFacetResult(BaseFacetResult(
                core,
                core.dateRanges().map { CategoryResult(DateRange(it.start(), it.end(), it.name()), it.count()) }
            ))

            is CoreNumericRangeSearchFacetResult -> NumericFacetResult(BaseFacetResult(
                core,
                core.numericRanges().map { CategoryResult(NumericRange(it.min(), it.max(), it.name()), it.count()) }
            ))

            is CoreTermSearchFacetResult -> TermFacetResult(BaseFacetResult(
                core,
                core.terms().map { CategoryResult(FrequentTerm(it.name()), it.count()) }
            ))

            else -> throw RuntimeException("Unexpected facet result type: $core")
        }
    }

    public override operator fun get(facet: TermFacet): TermFacetResult? =
        facets.find { it.name == facet.name } as? TermFacetResult

    public override operator fun get(facet: NumericFacet): NumericFacetResult? =
        facets.find { it.name == facet.name } as? NumericFacetResult

    public override operator fun get(facet: DateFacet): DateFacetResult? =
        facets.find { it.name == facet.name } as? DateFacetResult

    override fun toString(): String {
        return "SearchMetadata(metrics=$metrics, errors=$errors, facets=$facets)"
    }
}
