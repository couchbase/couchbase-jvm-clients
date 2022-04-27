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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.search.SearchChunkHeader
import com.couchbase.client.core.msg.search.SearchChunkTrailer
import com.couchbase.client.core.util.Bytes.EMPTY_BYTE_ARRAY
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef
import com.couchbase.client.kotlin.internal.toStringUtf8
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds

public sealed class SearchFlowItem

/**
 * One row of a Full-Text Search result.
 *
 * Represents a document matching the query condition.
 *
 * @property fragments A multimap from field name to snippets of the field content with search terms highlighted.
 */
public class SearchRow internal constructor(
    internal val raw: ByteArray,

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
     * Bytes of JSON Object explaining how the score was calculated,
     * or an empty array if the `explain` parameter was false.
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
        internal fun from(data: ByteArray, defaultSerializer: JsonSerializer): SearchRow {
            val json = Mapper.decodeIntoTree(data) as ObjectNode
            val scoreNode = json.path("score")

            return SearchRow(
                raw = data,
                index = json.path("index").asText(),
                id = json.path("id").asText(),

                score = scoreNode.asDouble(),
                explanation = json.get("explanation")?.let { Mapper.encodeAsBytes(it) } ?: EMPTY_BYTE_ARRAY,

                locations = parseLocations(json.path("locations")),

                keyset = SearchKeyset(json.get("sort")
                    ?.let { Mapper.convertValue(it, LIST_OF_STRINGS) }
                    ?.map { if (it == "_score") scoreNode.asText() else it }
                    ?: emptyList()), // shouldn't happen

                fragments = json.get("fragments")
                    ?.let { Mapper.convertValue(it, MULTIMAP_FROM_STRING_TO_STRING) }
                    ?: emptyMap(), // expected if no highlighting, or field term vectors weren't stored

                fields = json.get("fields")?.let { Mapper.encodeAsBytes(it) },
                defaultSerializer = defaultSerializer,
            )
        }

        private fun JsonNode.forEachField(consumer: (name: String, value: JsonNode) -> Unit) {
            fields().forEachRemaining { (key, value): Map.Entry<String, JsonNode> -> consumer(key, value) }
        }

        private fun parseLocations(root: JsonNode): List<SearchRowLocation> {
            if (root.isMissingNode) return emptyList()

            val result = mutableListOf<SearchRowLocation>()

            root.forEachField { field, value ->
                value.forEachField { term, locations ->
                    locations.forEach {
                        result.add(parseLocation(field, term, it as ObjectNode))
                    }
                }
            }

            return result
        }

        private fun ArrayNode.toIntList(): List<Int> = map { element -> element.intValue() }

        private fun parseLocation(field: String, term: String, node: ObjectNode): SearchRowLocation {
            return SearchRowLocation(
                field = field,
                term = term,
                position = node.get("pos").intValue(),
                start = node.get("start").intValue(),
                end = node.get("end").intValue(),
                arrayPositions = (node.get("array_positions") as? ArrayNode)?.toIntList() ?: emptyList()
            )
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
        return "SearchRow(content=${raw.toStringUtf8()})"
    }
}

internal data class ParsedSearchHeader(
    val failedPartitions: Int,
    val successfulPartitions: Int,
    val errors: Map<String, String>,
) {
    val totalPartitions: Int = failedPartitions + successfulPartitions
}

internal fun SearchChunkHeader.parse(): ParsedSearchHeader {
    val json = Mapper.decodeIntoTree(status)
    return ParsedSearchHeader(
        failedPartitions = json.path("failed").asInt(),
        successfulPartitions = json.path("successful").asInt(),
        errors = json.get("errors")
            ?.let { errorsNode -> Mapper.convertValue(errorsNode, MAP_FROM_STRING_TO_STRING) }
            ?: emptyMap()
    )
}

internal fun SearchChunkTrailer.parseFacets(): List<FacetResult<*>> {
    if (facets() == null || facets().isEmpty()) return emptyList()

    val json = Mapper.decodeIntoTree(facets()) as? ObjectNode ?: return emptyList()

    val result = mutableListOf<FacetResult<*>>()

    json.fields().forEach { (facetName, facetJson) ->
        val field = facetJson.path("field").asText()
        val total = facetJson.path("total").asLong()
        val missing = facetJson.path("missing").asLong()
        val other = facetJson.path("other").asLong()

        if (facetJson.has("terms")) {
            val categories = mutableListOf<CategoryResult<FrequentTerm>>()
            val terms = facetJson.get("terms") as ArrayNode
            terms.forEach {
                categories.add(
                    CategoryResult(
                        category = FrequentTerm(it.path("term").asText()),
                        count = it.path("count").asLong()),
                )
            }

            result.add(TermFacetResult(BaseFacetResult(facetName, field, total, missing, other, categories)))

        } else if (facetJson.has("numeric_ranges")) {
            val categories = mutableListOf<CategoryResult<NumericRange>>()
            val ranges = facetJson.get("numeric_ranges") as ArrayNode
            ranges.forEach {
                categories.add(
                    CategoryResult(
                        // server echoes the range from the request
                        category = NumericRange(
                            name = it.path("name").asText(),
                            min = it.get("min")?.asDouble(),
                            max = it.get("max")?.asDouble(),
                        ),
                        count = it.path("count").asLong()),
                )
            }

            result.add(NumericFacetResult(BaseFacetResult(facetName, field, total, missing, other, categories)))

        } else if (facetJson.has("date_ranges")) {
            val categories = mutableListOf<CategoryResult<DateRange>>()
            val ranges = facetJson.get("date_ranges") as ArrayNode
            ranges.forEach {
                categories.add(
                    CategoryResult(
                        // server echoes the range from the request
                        category = DateRange(
                            name = it.path("name").asText(),
                            start = it.get("start")?.textValue()?.let { timestamp -> Instant.parse(timestamp) },
                            end = it.get("end")?.textValue()?.let { timestamp -> Instant.parse(timestamp) },
                        ),
                        count = it.path("count").asLong()),
                )
            }

            result.add(DateFacetResult(BaseFacetResult(facetName, field, total, missing, other, categories)))
        }
    }

    return result
}


public class SearchMetrics internal constructor(
    header: ParsedSearchHeader,
    trailer: SearchChunkTrailer,
) {
    public val took: Duration = trailer.took().nanoseconds
    public val totalRows: Long = trailer.totalRows()
    public val maxScore: Double = trailer.maxScore()

    public val successfulPartitions: Int = header.successfulPartitions
    public val failedPartitions: Int = header.failedPartitions
    public val totalPartitions: Int = header.totalPartitions

    override fun toString(): String {
        return "SearchMetrics(took=$took, totalRows=$totalRows, maxScore=$maxScore, successfulPartitions=$successfulPartitions, failedPartitions=$failedPartitions, totalPartitions=$totalPartitions)"
    }
}

/**
 * Metadata about query execution. Always the last item in the flow.
 */
public class SearchMetadata(
    private val header: SearchChunkHeader,
    private val trailer: SearchChunkTrailer,
) : SearchFlowItem(), FacetResultAccessor {

    private val parsedHeader = header.parse()

    public val metrics: SearchMetrics = SearchMetrics(parsedHeader, trailer)

    /**
     * Map from failed partition name to error message.
     */
    public val errors: Map<String, String> = parsedHeader.errors

    public override val facets: List<FacetResult<*>> = trailer.parseFacets()

    public override operator fun get(facet: TermFacet): TermFacetResult? =
        facets.find { it.name == facet.name } as? TermFacetResult

    public override operator fun get(facet: NumericFacet): NumericFacetResult? =
        facets.find { it.name == facet.name } as? NumericFacetResult

    public override operator fun get(facet: DateFacet): DateFacetResult? =
        facets.find { it.name == facet.name } as? DateFacetResult

    override fun toString(): String {
        return "SearchMetadata(metrics=$metrics, errors=$errors, facets=$facets, rawHeader=$header, rawTrailer=$trailer)"
    }
}

private val LIST_OF_STRINGS = object : TypeReference<List<String>>() {}
private val MAP_FROM_STRING_TO_STRING = object : TypeReference<Map<String, String>>() {}
private val MULTIMAP_FROM_STRING_TO_STRING = object : TypeReference<Map<String, List<String>>>() {}
