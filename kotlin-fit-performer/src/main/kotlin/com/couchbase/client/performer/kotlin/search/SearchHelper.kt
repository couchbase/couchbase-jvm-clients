/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.client.performer.kotlin.search

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.util.NanoTimestamp
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.Scope
import com.couchbase.client.kotlin.search.DateRange
import com.couchbase.client.kotlin.search.Direction
import com.couchbase.client.kotlin.search.FacetResult
import com.couchbase.client.kotlin.search.FieldType
import com.couchbase.client.kotlin.search.GeoDistance
import com.couchbase.client.kotlin.search.GeoDistanceUnit
import com.couchbase.client.kotlin.search.GeoPoint
import com.couchbase.client.kotlin.search.GeoShape
import com.couchbase.client.kotlin.search.Highlight
import com.couchbase.client.kotlin.search.Missing
import com.couchbase.client.kotlin.search.Mode
import com.couchbase.client.kotlin.search.NumericRange
import com.couchbase.client.kotlin.search.SearchFacet
import com.couchbase.client.kotlin.search.SearchMetadata
import com.couchbase.client.kotlin.search.SearchPage
import com.couchbase.client.kotlin.search.SearchQuery
import com.couchbase.client.kotlin.search.SearchQuery.Companion.MatchOperator
import com.couchbase.client.kotlin.search.SearchResult
import com.couchbase.client.kotlin.search.SearchRow
import com.couchbase.client.kotlin.search.SearchRowLocation
import com.couchbase.client.kotlin.search.SearchScanConsistency
import com.couchbase.client.kotlin.search.SearchSort
import com.couchbase.client.kotlin.search.SearchSpec
import com.couchbase.client.kotlin.search.VectorQuery
import com.couchbase.client.kotlin.search.VectorSearchSpec
import com.couchbase.client.kotlin.search.execute
import com.couchbase.client.performer.kotlin.manager.toByteString
import com.couchbase.client.performer.kotlin.util.ClusterConnection
import com.couchbase.client.performer.kotlin.util.convert
import com.couchbase.client.performer.kotlin.util.toKotlin
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.ClusterLevelCommand
import com.couchbase.client.protocol.sdk.ScopeLevelCommand
import com.couchbase.client.protocol.sdk.search.HighlightStyle
import com.couchbase.client.protocol.sdk.search.SearchFacetResult
import com.couchbase.client.protocol.sdk.search.SearchFacets
import com.couchbase.client.protocol.sdk.search.SearchMetrics
import com.couchbase.client.protocol.sdk.search.VectorQueryCombination
import com.couchbase.client.protocol.shared.ContentAs
import com.google.protobuf.Timestamp
import kotlinx.coroutines.runBlocking
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.Any
import kotlin.Boolean
import kotlin.Int
import kotlin.Number
import kotlin.String
import kotlin.Suppress
import kotlin.TODO
import kotlin.apply
import kotlin.getOrThrow
import kotlin.let
import kotlin.recoverCatching
import kotlin.runCatching
import kotlin.time.Duration.Companion.milliseconds
import kotlin.with
import com.couchbase.client.protocol.sdk.Result as FitResult
import com.couchbase.client.protocol.sdk.search.BlockingSearchResult as FitBlockingSearchResult
import com.couchbase.client.protocol.sdk.search.DateRange as FitDateRange
import com.couchbase.client.protocol.sdk.search.Highlight as FitHighlight
import com.couchbase.client.protocol.sdk.search.Location as FitLocation
import com.couchbase.client.protocol.sdk.search.MatchOperator as FitMatchOperator
import com.couchbase.client.protocol.sdk.search.NumericRange as FitNumericRange
import com.couchbase.client.protocol.sdk.search.SearchFacet as FitSearchFacet
import com.couchbase.client.protocol.sdk.search.SearchFacets as FitSearchFacets
import com.couchbase.client.protocol.sdk.search.SearchFragments as FitSearchFragments
import com.couchbase.client.protocol.sdk.search.SearchGeoDistanceUnits as FitSearchGeoDistanceUnits
import com.couchbase.client.protocol.sdk.search.SearchMetaData as FitSearchMetaData
import com.couchbase.client.protocol.sdk.search.SearchOptions as FitSearchOptions
import com.couchbase.client.protocol.sdk.search.SearchQuery as FitSearchQuery
import com.couchbase.client.protocol.sdk.search.SearchRow as FitSearchRow
import com.couchbase.client.protocol.sdk.search.SearchRowLocation as FitSearchRowLocation
import com.couchbase.client.protocol.sdk.search.SearchSort as FitSearchSort
import com.couchbase.client.protocol.sdk.search.VectorQuery as FitVectorQuery
import com.couchbase.client.protocol.sdk.search.VectorSearch as FitVectorSearch

private val FitSearchOptions.consistency: SearchScanConsistency
    get() =
        if (hasConsistentWith()) SearchScanConsistency.consistentWith(consistentWith.toKotlin())
        else SearchScanConsistency.notBounded()

typealias JsonObject = Map<String, Any?>
typealias JsonArray = List<Any?>

private fun FitSearchFacet.toSdk(name: String): SearchFacet {
    return when {
        hasTerm() -> with(term) {
            SearchFacet.term(
                field = field,
                size = size,
                name = name,
            )
        }

        hasDateRange() -> with(dateRange) {
            SearchFacet.date(
                field = field,
                ranges = dateRangesList.map { it.toSdk() },
                name = name,
            )
        }

        hasNumericRange() -> with(numericRange) {
            SearchFacet.numeric(
                field = field,
                ranges = numericRangesList.map { it.toSdk() },
                name = name,
            )
        }

        else -> TODO("unrecognized facet type: $this")
    }
}

private fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

private fun FitDateRange.toSdk() = when {
    hasStart() && hasEnd() -> DateRange.bounds(start = start.toInstant(), end = end.toInstant(), name = name)
    hasStart() -> DateRange.lowerBound(start = start.toInstant(), name = name)
    hasEnd() -> DateRange.upperBound(end = end.toInstant(), name = name)
    else -> TODO("unrecognized facet date range")
}

private fun FitNumericRange.toSdk() = when {
    hasMin() && hasMax() -> NumericRange.bounds(min = min, max = max, name = name)
    hasMin() -> NumericRange.lowerBound(min = min, name = name)
    hasMax() -> NumericRange.upperBound(max = max, name = name)
    else -> TODO("unrecognized facet numeric range")
}

private data class SearchParams(
    val indexName: String,
    val spec: SearchSpec,
    val options: FitSearchOptions?,
    val fieldsAs: ContentAs?,
) {
    val common: CommonOptions
        get() = options?.let { if (it.hasTimeoutMillis()) CommonOptions(timeout = it.timeoutMillis.milliseconds) else null } ?: CommonOptions.Default

    val page: SearchPage
        get() = options?.let { if (it.hasSkip()) SearchPage.startAt(it.skip) else null } ?: SearchPage.startAt(0)

    val limit: Int?
        get() = options?.let { if (it.hasLimit()) it.limit else null }

    val sort: SearchSort
        get() = options?.sortList
            ?.map { it.toSdk() }
            ?.let { SearchSort.of(it) }
            ?: SearchSort.byScore()

    val highlight: Highlight
        get() = options?.highlight?.toSdk() ?: Highlight.none()

    val includeLocations: Boolean
        get() = options?.hasIncludeLocations() == true && options.includeLocations

    val facets: List<SearchFacet>
        get() = options?.facetsMap?.entries?.map { it.value.toSdk(it.key) } ?: emptyList()

    val explain: Boolean
        get() = options?.let { if (it.hasExplain()) it.explain else null } ?: false

    val consistency: SearchScanConsistency
        get() = options?.consistency ?: SearchScanConsistency.notBounded()

    val fields: List<String>
        get() = options?.fieldsList ?: emptyList()

    fun convertToFit(sdkResult: SearchResult, start: NanoTimestamp): Result.Builder {
        val out = Result.newBuilder()
        out.setSdk(
            FitResult.newBuilder()
                .setSearchBlockingResult(convertResult(sdkResult, fieldsAs))
        )
        out.elapsedNanos = start.elapsed().toNanos()
        return out
    }

    @Suppress("DuplicatedCode")
    companion object {
        fun from(clc: ClusterLevelCommand): SearchParams {
            if (clc.hasSearchV2()) with(clc.searchV2.search) {
                val searchQuery = if (request.hasSearchQuery()) request.searchQuery.toSdk() else null
                val vectorQuery = if (request.hasVectorSearch()) request.vectorSearch.toSdk() else null

                val spec = if (searchQuery != null && vectorQuery != null) {
                    SearchSpec.mixedMode(searchQuery, vectorQuery)
                } else searchQuery ?: vectorQuery ?: throw InvalidArgumentException.fromMessage("must specify at least one of searchQuery or vectorQuery")

                return SearchParams(
                    indexName = indexName,
                    spec = spec,
                    options = if (hasOptions()) options else null,
                    fieldsAs = if (clc.searchV2.hasFieldsAs()) clc.searchV2.fieldsAs else null
                )

            } else with(clc.search) {
                return SearchParams(
                    indexName = indexName,
                    spec = query.toSdk(),
                    options = if (hasOptions()) options else null,
                    fieldsAs = if (hasFieldsAs()) fieldsAs else null
                )
            }
        }

        fun from(slc: ScopeLevelCommand): SearchParams {
            if (slc.hasSearchV2()) with(slc.searchV2.search) {
                val searchQuery = if (request.hasSearchQuery()) request.searchQuery.toSdk() else null
                val vectorQuery = if (request.hasVectorSearch()) request.vectorSearch.toSdk() else null

                val spec = if (searchQuery != null && vectorQuery != null) {
                    SearchSpec.mixedMode(searchQuery, vectorQuery)
                } else searchQuery ?: vectorQuery ?: throw InvalidArgumentException.fromMessage("must specify at least one of searchQuery or vectorQuery")

                return SearchParams(
                    indexName = indexName,
                    spec = spec,
                    options = if (hasOptions()) options else null,
                    fieldsAs = if (slc.searchV2.hasFieldsAs()) slc.searchV2.fieldsAs else null
                )

            } else with(slc.search) {
                return SearchParams(
                    indexName = indexName,
                    spec = query.toSdk(),
                    options = if (hasOptions()) options else null,
                    fieldsAs = if (hasFieldsAs()) fieldsAs else null
                )
            }
        }
    }
}

class SearchHelper {
    companion object {
        fun handleScopeSearch(
            scope: Scope,
            slc: ScopeLevelCommand,
        ): Result.Builder {
            val start = NanoTimestamp.now()
            val params = SearchParams.from(slc)

            val sdkResult = runBlocking {
                scope.search(
                    indexName = params.indexName,
                    spec = params.spec,
                    common = params.common,
                    page = params.page,
                    limit = params.limit,
                    fields = params.fields,
                    consistency = params.consistency,
                    sort = params.sort,
                    highlight = params.highlight,
                    includeLocations = params.includeLocations,
                    facets = params.facets,
                    explain = params.explain,
                ).execute()
            }

            return params.convertToFit(sdkResult, start)
        }

        fun handleClusterSearch(
            connection: ClusterConnection,
            clc: ClusterLevelCommand,
        ): Result.Builder {
            val start = NanoTimestamp.now()
            val cluster = connection.cluster
            val params = SearchParams.from(clc)

            val sdkResult = runBlocking {
                if (clc.hasSearchV2()) {
                    cluster.search(
                        indexName = params.indexName,
                        spec = params.spec,
                        common = params.common,
                        page = params.page,
                        limit = params.limit,
                        fields = params.fields,
                        consistency = params.consistency,
                        sort = params.sort,
                        highlight = params.highlight,
                        includeLocations = params.includeLocations,
                        facets = params.facets,
                        explain = params.explain,
                    ).execute()
                } else {
                    cluster.searchQuery(
                        indexName = params.indexName,
                        query = params.spec as SearchQuery,
                        common = params.common,
                        page = params.page,
                        limit = params.limit,
                        fields = params.fields,
                        consistency = params.consistency,
                        sort = params.sort,
                        highlight = params.highlight,
                        includeLocations = params.includeLocations,
                        facets = params.facets,
                        explain = params.explain,
                    ).execute()
                }
            }

            return params.convertToFit(sdkResult, start)
        }
    }
}

private fun FitHighlight.toSdk(): Highlight =
    if (hasStyle()) {
        when (style) {
            HighlightStyle.HIGHLIGHT_STYLE_ANSI -> Highlight.ansi(fieldsList)
            HighlightStyle.HIGHLIGHT_STYLE_HTML -> Highlight.html(fieldsList)
            else -> TODO("unknown style")
        }
    } else Highlight.html(fieldsList)


private fun FitSearchSort.toSdk(): SearchSort = when {
    hasField() -> with(field) {
        SearchSort.byField(
            field = field,
            type = if (hasType()) FieldType.valueOf(type.uppercase()) else FieldType.AUTO,
            mode = if (hasMode()) Mode.valueOf(mode.uppercase()) else Mode.DEFAULT,
            missing = if (hasMissing()) Missing.valueOf(missing.uppercase()) else Missing.LAST,
            direction = if (hasDesc() && desc) Direction.DESCENDING else Direction.ASCENDING,
        )
    }

    hasId() -> with(id) {
        SearchSort.byId(
            direction = if (hasDesc() && desc) Direction.DESCENDING else Direction.ASCENDING
        )
    }

    hasScore() -> with(score) {
        // When sorting by score, Kotlin's default direction is descending.
        SearchSort.byScore(
            direction = if (hasDesc() && !desc) Direction.ASCENDING else Direction.DESCENDING
        )
    }

    hasGeoDistance() -> with(geoDistance) {
        SearchSort.byGeoDistance(
            field = field,
            location = location.toSdk(),
            unit = if (hasUnit()) unit.toSDK() else GeoDistanceUnit.METERS,
            direction = if (hasDesc() && desc) Direction.DESCENDING else Direction.ASCENDING
        )
    }

    hasRaw() -> SearchSort.by(raw)

    else -> TODO("Unrecognized search sort.")
}


private fun FitSearchGeoDistanceUnits.toSDK(): GeoDistanceUnit =
    GeoDistanceUnit.valueOf(name.removePrefix("SEARCH_GEO_DISTANCE_UNITS_"))

private fun convertResult(result: SearchResult, fieldsAs: ContentAs?): FitBlockingSearchResult {
    return FitBlockingSearchResult.newBuilder()
        .addAllRows(result.rows.map { convertRow(it, fieldsAs) })
        .setMetaData(convertMetaData(result.metadata))
        .setFacets(convertFacets(result.facets))
        .build()
}

fun convertFacets(facets: List<FacetResult<*>>): FitSearchFacets {
    val fitFacets = facets.map { sdk ->
        SearchFacetResult.newBuilder().apply {
            name = sdk.name
            field = sdk.field
            total = sdk.total
            missing = sdk.missing
            other = sdk.other
            // There's more to a facet result, but this is all FIT uses at the moment.
        }.build()
    }.associateBy { it.name }

    return SearchFacets.newBuilder()
        .putAllFacets(fitFacets)
        .build()
}

fun convertMetaData(metadata: SearchMetadata): FitSearchMetaData {
    return FitSearchMetaData.newBuilder()
        .setMetrics(SearchMetrics.newBuilder().apply {
            totalRows = metadata.metrics.totalRows
            maxScore = metadata.metrics.maxScore
            tookMsec = metadata.metrics.took.inWholeMilliseconds
            totalPartitionCount = metadata.metrics.totalPartitions.toLong()
            errorPartitionCount = metadata.metrics.failedPartitions.toLong()
            successPartitionCount = metadata.metrics.successfulPartitions.toLong()
        })
        .putAllErrors(metadata.errors)
        .build()
}

fun convertRow(row: SearchRow, fieldsAs: ContentAs?): FitSearchRow {
    return FitSearchRow.newBuilder().apply {
        index = row.index
        id = row.id
        score = row.score
        explanation = row.explanation.toByteString()

        putAllFragments(row.fragments.mapValues { entry ->
            FitSearchFragments.newBuilder().addAllFragments(entry.value).build()
        })

        addAllLocations(row.locations.map { it.toFit() })

        if (fieldsAs != null) fields = fieldsAs.convert(row)
    }.build()
}

private fun SearchRowLocation.toFit(): FitSearchRowLocation {
    val sdk = this
    return FitSearchRowLocation.newBuilder().apply {
        field = sdk.field
        term = sdk.term
        position = sdk.position
        start = sdk.start
        end = sdk.end
        addAllArrayPositions(sdk.arrayPositions)
    }.build()
}

private fun FitVectorSearch.toSdk(): VectorSearchSpec {
    val sdkQueries = vectorQueryList.map { it.toSdk() }
    val operator =
        if (hasOptions() && options.hasVectorQueryCombination()) options.vectorQueryCombination
        else VectorQueryCombination.OR

    return when {
        sdkQueries.size == 1 -> sdkQueries.single()
        operator == VectorQueryCombination.AND -> SearchSpec.allOf(sdkQueries)
        else -> SearchSpec.anyOf(sdkQueries)
    }
}

private fun FitVectorQuery.toSdk(): VectorQuery {
    val query: VectorQuery

    if (hasBase64VectorQuery()) {
        query = if (!hasOptions() || !options.hasNumCandidates())
            SearchSpec.vector(vectorFieldName, base64VectorQuery)
        else
            SearchSpec.vector(
                field = vectorFieldName,
                vector = base64VectorQuery,
                numCandidates = options.numCandidates,
            )
    } else {
        query = if (!hasOptions() || !options.hasNumCandidates())
            SearchSpec.vector(vectorFieldName, vectorQueryList.toFloatArray())
        else
            SearchSpec.vector(
                field = vectorFieldName,
                vector = vectorQueryList.toFloatArray(),
                numCandidates = options.numCandidates,
            )
    }

    return query.maybeBoost(hasOptions() && options.hasBoost(), options.boost)
}

private fun FitMatchOperator.toSdk(): MatchOperator {
    return when (this) {
        FitMatchOperator.SEARCH_MATCH_OPERATOR_OR -> MatchOperator.OR
        FitMatchOperator.SEARCH_MATCH_OPERATOR_AND -> MatchOperator.AND
        FitMatchOperator.UNRECOGNIZED -> TODO("unrecognized match operator")
    }
}

private fun FitSearchQuery.toSdk(): SearchQuery = when {
    hasMatchAll() -> SearchSpec.matchAll()

    hasMatchNone() -> SearchSpec.matchNone()

    hasMatch() -> with(match) {
        val query = if (!hasField() && !hasOperator())
            SearchSpec.match(match)
        else
            SearchSpec.match(
                match = match,
                field = if (hasField()) field else "_all",
                operator = if (hasOperator()) operator.toSdk() else MatchOperator.OR,
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasMatchPhrase() -> with(matchPhrase) {
        val query = if (!hasField() && !hasAnalyzer())
            SearchSpec.matchPhrase(matchPhrase)
        else
            SearchSpec.matchPhrase(
                matchPhrase = matchPhrase,
                field = if (hasField()) field else "_all",
                analyzer = if (hasAnalyzer()) analyzer else null,
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasTerm() -> with(term) {
        val query = if (!hasField() && !hasFuzziness() && !hasPrefixLength())
            SearchSpec.term(term)
        else
            SearchSpec.term(
                term = term,
                field = if (hasField()) field else "_all",
                fuzziness = if (hasFuzziness()) fuzziness else 0,
                prefixLength = if (hasPrefixLength()) prefixLength else 0,
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasPhrase() -> with(phrase) {
        val query = if (!hasField())
            SearchSpec.phrase(termsList)
        else
            SearchSpec.phrase(
                terms = termsList,
                field = if (hasField()) field else "_all",
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasPrefix() -> with(prefix) {
        val query = if (!hasField())
            SearchSpec.prefix(prefix)
        else
            SearchSpec.prefix(
                prefix = prefix,
                field = if (hasField()) field else "_all",
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasRegexp() -> with(regexp) {
        val query = if (!hasField())
            SearchSpec.regexp(regexp)
        else
            SearchSpec.regexp(
                regexp = regexp,
                field = if (hasField()) field else "_all",
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasWildcard() -> with(wildcard) {
        val query = if (!hasField())
            SearchSpec.wildcard(wildcard)
        else
            SearchSpec.wildcard(
                term = wildcard, // irregular param name; see KCBC-152
                field = if (hasField()) field else "_all",
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasQueryString() -> with(queryString) {
        SearchSpec.queryString(queryString = query)
            .maybeBoost(hasBoost(), boost)
    }

    hasSearchBooleanField() -> with(searchBooleanField) {
        SearchSpec.booleanField(
            bool = bool,
            field = if (hasField()) field else "_all",
        ).maybeBoost(hasBoost(), boost)
    }

    hasDocId() -> with(docId) {
        SearchSpec.documentId(ids = idsList)
            .maybeBoost(hasBoost(), boost)
    }

    hasTermRange() -> with(termRange) {
        val query = if (!hasField() && !hasMin() && !hasInclusiveMin() && !hasMax() && !hasInclusiveMax())
            SearchSpec.termRange()
        else if (!hasInclusiveMin())
        // test a few combinations to verify correct defaults for `inclusiveMin` and `inclusiveMax`.
            SearchSpec.termRange(
                field = if (hasField()) field else "_all",
                min = if (hasMin()) min else null,
                max = if (hasMax()) max else null,
                inclusiveMax = if (hasInclusiveMax()) inclusiveMax else false,
            )
        else if (!hasInclusiveMax())
            SearchSpec.termRange(
                field = if (hasField()) field else "_all",
                min = if (hasMin()) min else null,
                inclusiveMin = if (hasInclusiveMin()) inclusiveMin else true,
                max = if (hasMax()) max else null,
            )
        else SearchSpec.termRange(
            field = if (hasField()) field else "_all",
            min = if (hasMin()) min else null,
            inclusiveMin = inclusiveMin,
            max = if (hasMax()) max else null,
            inclusiveMax = inclusiveMax,
        )

        query.maybeBoost(hasBoost(), boost)
    }

    hasNumericRange() -> with(numericRange) {
        val query = if (!hasField() && !hasMin() && !hasInclusiveMin() && !hasMax() && !hasInclusiveMax())
            SearchSpec.termRange()
        else if (!hasInclusiveMin())
        // test a few combinations to verify correct defaults for `inclusiveMin` and `inclusiveMax`.
            SearchSpec.numericRange(
                field = if (hasField()) field else "_all",
                min = if (hasMin()) min else null,
                max = if (hasMax()) max else null,
                inclusiveMax = if (hasInclusiveMax()) inclusiveMax else false,
            )
        else if (!hasInclusiveMax())
            SearchSpec.numericRange(
                field = if (hasField()) field else "_all",
                min = if (hasMin()) min else null,
                inclusiveMin = if (hasInclusiveMin()) inclusiveMin else true,
                max = if (hasMax()) max else null,
            )
        else SearchSpec.numericRange(
            field = if (hasField()) field else "_all",
            min = if (hasMin()) min else null,
            inclusiveMin = inclusiveMin,
            max = if (hasMax()) max else null,
            inclusiveMax = inclusiveMax,
        )

        query.maybeBoost(hasBoost(), boost)
    }

    hasDateRange() -> with(dateRange) {
        val query = if (!hasField() && !hasStart() && !hasInclusiveStart() && !hasEnd() && !hasInclusiveEnd())
            SearchSpec.dateRange()
        else if (!hasInclusiveStart())
        // test a few combinations to verify correct defaults for `inclusiveStart` and `inclusiveEnd`.
            SearchSpec.dateRange(
                field = if (hasField()) field else "_all",
                start = if (hasStart()) parseFtsTimestamp(start) else null,
                end = if (hasEnd()) parseFtsTimestamp(end) else null,
                inclusiveEnd = if (hasInclusiveEnd()) inclusiveEnd else false,
            )
        else if (!hasInclusiveEnd())
            SearchSpec.dateRange(
                field = if (hasField()) field else "_all",
                start = if (hasStart()) parseFtsTimestamp(start) else null,
                inclusiveStart = if (hasInclusiveStart()) inclusiveStart else true,
                end = if (hasEnd()) parseFtsTimestamp(end) else null,
            )
        else SearchSpec.dateRange(
            field = if (hasField()) field else "_all",
            start = if (hasStart()) parseFtsTimestamp(start) else null,
            inclusiveStart = inclusiveStart,
            end = if (hasEnd()) parseFtsTimestamp(end) else null,
            inclusiveEnd = inclusiveEnd
        )

        query.maybeBoost(hasBoost(), boost)
    }

    hasGeoDistance() -> with(geoDistance) {
        val query = if (!hasField())
            SearchSpec.geoDistance(location.toSdk(), GeoDistance.parse(distance))
        else
            SearchSpec.geoDistance(
                location = location.toSdk(),
                distance = GeoDistance.parse(distance),
                field = if (hasField()) field else "_all",
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasGeoBoundingBox() -> with(geoBoundingBox) {
        val shape = GeoShape.rectangle(
            topLeft = topLeft.toSdk(),
            bottomRight = bottomRight.toSdk(),
        )
        val query = if (!hasField())
            SearchSpec.geoShape(shape)
        else
            SearchSpec.geoShape(
                shape = shape,
                field = if (hasField()) field else "_all",
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasConjunction() -> with(conjunction) {
        SearchSpec.conjunction(
            conjuncts = conjunctsList.map { it.toSdk() }
        ).maybeBoost(hasBoost(), boost)
    }

    hasDisjunction() -> with(disjunction) {
        val query = if (!hasMin())
            SearchSpec.disjunction(disjunctsList.map { it.toSdk() })
        else
            SearchSpec.disjunction(
                disjuncts = disjunctsList.map { it.toSdk() },
                min = min
            )

        query.maybeBoost(hasBoost(), boost)
    }

    hasBoolean() -> with(boolean) {
        val query = SearchSpec.boolean(
            must = if (mustCount == 0) null else SearchSpec.conjunction(mustList.map { it.toSdk() }),
            should = if (shouldCount == 0) null else SearchSpec.disjunction(
                disjuncts = shouldList.map { it.toSdk() },
                min = if (hasShouldMin()) shouldMin else 1,
            ),
            mustNot = if (mustNotCount == 0) null else SearchSpec.disjunction(
                disjuncts = mustNotList.map { it.toSdk() },
            )
        )

        query.maybeBoost(hasBoost(), boost)
    }

    else -> TODO("unrecognized search query: $this")
}


private fun FitLocation.toSdk() = GeoPoint.coordinates(lat = lat.toDouble(), lon = lon.toDouble())

private fun SearchQuery.maybeBoost(hasBoost: Boolean, boost: Number): SearchQuery {
    return if (hasBoost) (this boost boost) else this
}

private fun VectorQuery.maybeBoost(hasBoost: Boolean, boost: Number): VectorQuery {
    return if (hasBoost) (this boost boost) else this
}

/**
 * Couchbase Server allows date range queries to use
 * [a variety of timestamp formats](https://docs.couchbase.com/cloud/search/default-date-time-parsers-reference.html),
 * some of which omit time-of-day and zone offset information. When these components
 * are omitted, the server assumes start of day (T00:00:00) and UTC.
 *
 * The FIT protocol for FTS uses strings to represent timestamps in date range queries,
 * so it can convey the full range of supported user inputs.
 *
 * Meanwhile, the Kotlin SDK requires users to specify a date range query using Instants,
 * so there's no ambiguity about hour-of-day or offsets.
 *
 * This method converts the following input formats to Instants,
 * using the same strategy as Couchbase Server.
 *
 * * Local date (`"2024-01-02"`) -> `2024-01-02T00:00:00Z`
 * * Local datetime (`"2024-01-02T01:02:03.456"`) -> `2024-01-02T01:02:03.456Z`
 * * Offset datetime (`"2024-01-02T00:00:00-07:00"`) -> `2024-01-02T07:00:00Z`
 * * Zulu datetime (`"2024-01-02T01:02:03Z"`) -> `2024-01-02T01:02:03Z` (same as input)
 *
 * Does not handle these formats accepted by FTS:
 *
 * * `"2024-01-02 01:02:03.456"`
 * * `"2023-09-15 14:24:50 +0530"`
 */
private fun parseFtsTimestamp(timestamp: String): Instant {
    return runCatching { Instant.parse(timestamp) }
        .recoverCatching { LocalDateTime.parse(timestamp).toInstant(ZoneOffset.UTC) }
        .recoverCatching { LocalDate.parse(timestamp).atStartOfDay().toInstant(ZoneOffset.UTC) }
        .getOrThrow()
}
