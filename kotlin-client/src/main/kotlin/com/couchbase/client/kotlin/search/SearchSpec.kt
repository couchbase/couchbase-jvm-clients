/*
 * Copyright 2024 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.search.queries.CoreSearchRequest
import com.couchbase.client.core.api.search.vector.CoreVectorQueryCombination
import com.couchbase.client.kotlin.annotations.UncommittedCouchbaseApi
import com.couchbase.client.kotlin.search.SearchQuery.Companion.MatchOperator
import java.time.Instant

/**
 * A search specification ("spec" for short) that tells the server
 * what to look for during a Full-Text Search.
 *
 * Generally speaking, there are two different types of spec:
 * 1. A non-vector query represented by a [SearchQuery].
 * 2. A vector query represented by a [VectorQuery].
 *
 * Create instances of either type by calling [SearchSpec]
 * companion factory methods.
 *
 * To create a mixed-mode search that combines a vector search
 * with a non-vector search, use [SearchSpec.mixedMode].
 *
 * @sample com.couchbase.client.kotlin.samples.searchSpecSimpleSearchQuery
 * @sample com.couchbase.client.kotlin.samples.searchSpecSimpleVectorQuery
 * @sample com.couchbase.client.kotlin.samples.searchSpecVectorAnyOf
 * @sample com.couchbase.client.kotlin.samples.searchSpecVectorAllOf
 * @sample com.couchbase.client.kotlin.samples.searchSpecMixedMode
 */
@UncommittedCouchbaseApi
public sealed class SearchSpec {
    internal abstract val coreRequest: CoreSearchRequest

    public companion object {

        /**
         * Combines the non-vector [searchQuery] with a vector query, using logical OR.
         *
         * @sample com.couchbase.client.kotlin.samples.searchSpecMixedMode
         */
        @SinceCouchbase("7.6")
        public fun mixedMode(
            searchQuery: SearchQuery,
            vectorQuery: VectorSearchSpec,
        ): SearchSpec = MixedModeSearchSpec(searchQuery, vectorQuery)

        /**
         * A [Match query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match.html).
         *
         * Analyzes [match] and uses the result to query the index.
         *
         * Analysis means if the field is indexed with an appropriate language-specific
         * analyzer, then "beauty" will match "beautiful", "swim" will match "swimming", etc.
         *
         * The [match] input may contain multiple terms. By default, at least one of the terms
         * must be present in the document. To require all terms be present, set [operator]
         * to [MatchOperator.AND]. The order and position of the terms do not
         * matter for this query.
         *
         * ## Similar queries
         *
         * If you want exact matches without analysis, use [term].
         *
         * If the order and position of the terms are significant, use [matchPhrase].
         *
         * @param match The input string to analyze and match against.
         *
         * @param analyzer Analyzer to apply to [match]. Defaults to
         * the analyzer set for [field] during index creation.
         *
         * @param field The field to search. Defaults to `_all`, whose content
         * is specified during index creation.
         *
         * @param operator Determines whether a [match] value of "location hostel"
         * means "location AND hostel" or "location OR hostel".
         *
         * @param fuzziness Maximum allowable Levenshtein distance for a match against an analyzed term.
         *
         * @param prefixLength To be considered a match, an input term and the matched text
         * must have a common prefix of this length.
         */
        public fun match(
            match: String,
            field: String = "_all",
            analyzer: String? = null,
            operator: MatchOperator = MatchOperator.OR,
            fuzziness: Int = 0,
            prefixLength: Int = 0,
        ): SearchQuery = SearchQuery.match(match, field, analyzer, operator, fuzziness, prefixLength)

        /**
         * A [Match Phrase query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-phrase.html)
         *
         * Analyzes [matchPhrase] and uses the result to search for terms in the target
         * that occur in the same order and position.
         *
         * The target field must be indexed with "include term vectors".
         *
         * ## Similar queries
         *
         * If you want exact matches without analysis, use [phrase].
         *
         * If the order and proximity of the terms are not significant, use [match].
         *
         * @param matchPhrase The input string to analyze and match against.
         *
         * @param analyzer Analyzer to apply to [matchPhrase]. Defaults to
         * the analyzer set for [field] during index creation.
         *
         * @param field The field to search. Defaults to `_all`, whose content
         * is specified during index creation.
         */
        public fun matchPhrase(
            matchPhrase: String,
            field: String = "_all",
            analyzer: String? = null,
        ): SearchQuery = SearchQuery.matchPhrase(matchPhrase, field, analyzer)

        /**
         * A [Term query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-term.html)
         *
         * Searches for a single term, without using an analyzer.
         *
         * NOTE: This is a non-analytic query, meaning it won’t perform any text analysis on the query text.
         *
         * ## Similar queries
         *
         * If you want the terms to be analyzed for linguistic similarity,
         * use [match].
         *
         * To search for multiple terms that must appear in a certain order,
         * use [phrase]
         *
         * @param term The exact term to search for, without using an analyzer.
         *
         * @param field The field to search. Defaults to `_all`, whose content
         * is specified during index creation.
         *
         * @param fuzziness Maximum allowable Levenshtein distance for a match.
         *
         * @param prefixLength To be considered a match, an input term and the matched text
         * must have a common prefix of this length.
         */
        public fun term(
            term: String,
            field: String = "_all",
            fuzziness: Int = 0,
            prefixLength: Int = 0,
        ): SearchQuery = SearchQuery.term(term, field, fuzziness, prefixLength)

        /**
         * A [Phrase query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-phrase.html)
         *
         * Searches for the [terms] in the same order and position, without analyzing
         * them.
         *
         * The target field must be indexed with "include term vectors".
         *
         * NOTE: This is a non-analytic query, meaning it won’t perform any text analysis on the query text.
         *
         * ## Similar queries
         *
         * If you want to analyze the terms for linguistic similarity, use [matchPhrase].
         *
         * To search for a single term without analysis, use [term]
         *
         * @param terms The input terms to search for in, the same order and position.
         *
         * @param field The field to search. Defaults to `_all`, whose content
         * is specified during index creation.
         */
        public fun phrase(
            terms: List<String>,
            field: String = "_all",
        ): SearchQuery = SearchQuery.phrase(terms, field)

        /**
         * A [Prefix query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-prefix-query.html)
         *
         * Searches for terms that start with [prefix].
         *
         * NOTE: This is a non-analytic query, meaning it won’t perform any text analysis on the query text.
         *
         * @see regexp
         * @see wildcard
         */
        public fun prefix(
            prefix: String,
            field: String = "_all",
        ): SearchQuery = SearchQuery.prefix(prefix, field)

        /**
         * A [Regexp query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-regexp.html)
         *
         * Searches for terms that match [regexp].
         *
         * NOTE: This is a non-analytic query, meaning it won’t perform any text analysis on the query text.
         *
         * @param regexp A Go regular expression
         *
         * @see prefix
         * @see wildcard
         */
        public fun regexp(
            regexp: String,
            field: String = "_all",
        ): SearchQuery = SearchQuery.regexp(regexp, field)

        /**
         * A [Wildcard query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-wildcard.html)
         *
         * A wildcard query uses a wildcard expression to search within individual terms for matches.
         * Wildcard expressions can be any single character (?) or zero to many characters (*).
         * Wildcard expressions can appear in the middle or end of a term, but not at the beginning.
         *
         * NOTE: This is a non-analytic query, meaning it won’t perform any text analysis on the query text.
         *
         * @see regexp
         */
        public fun wildcard(
            term: String,
            field: String = "_all",
        ): SearchQuery = SearchQuery.wildcard(term, field)

        /**
         * A [Query String query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-query-string-query.html).
         *
         * Search using the [FTS query string syntax](https://docs.couchbase.com/server/current/fts/fts-query-string-syntax.html)
         * supported by the Couchbase admin console UI.
         *
         * @param queryString Example: `"+name:info* +country:france"`
         */
        public fun queryString(
            queryString: String,
        ): SearchQuery = SearchQuery.queryString(queryString)


        /**
         * A [Boolean Field query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-boolean-field-query.html).
         * Searches for documents where [field] has the value [bool].
         */
        public fun booleanField(
            bool: Boolean,
            field: String,
        ): SearchQuery = SearchQuery.booleanField(bool, field)

        /**
         * A [DocId query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-DocID-query.html).
         * Searches for documents whose ID is one of [ids].
         */
        public fun documentId(
            ids: List<String>,
        ): SearchQuery = SearchQuery.documentId(ids)

        /**
         * A [Term Range query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-term-range.html)
         */
        public fun termRange(
            field: String = "_all",
            min: String? = null,
            inclusiveMin: Boolean = true,
            max: String? = null,
            inclusiveMax: Boolean = false,
        ): SearchQuery = SearchQuery.termRange(field, min, inclusiveMin, max, inclusiveMax)

        /**
         * A [Numeric Range query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-numeric-range.html)
         */
        public fun numericRange(
            field: String = "_all",
            min: Number? = null,
            inclusiveMin: Boolean = true,
            max: Number? = null,
            inclusiveMax: Boolean = false,
        ): SearchQuery = SearchQuery.numericRange(field, min, inclusiveMin, max, inclusiveMax)

        /**
         * A [Date Range query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-date-range.html)
         */
        public fun dateRange(
            field: String = "_all",
            start: Instant? = null,
            inclusiveStart: Boolean = true,
            end: Instant? = null,
            inclusiveEnd: Boolean = false,
        ): SearchQuery = SearchQuery.dateRange(field, start, inclusiveStart, end, inclusiveEnd)

        /**
         * A [Geo Point Distance query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-geo-point-distance.html)
         *
         * Alias for [geoShape] using a circle centered at [location] with a radius of [distance].
         */
        public fun geoDistance(
            location: GeoPoint,
            distance: GeoDistance,
            field: String = "_all",
        ): SearchQuery = geoShape(GeoShape.circle(location, distance), field)

        /**
         * A [Geo Bounded Rectangle query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-geo-bounded-rectangle.html)
         * or [Geo Bounded Polygon query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-geo-bounded-polygon.html).
         * May also be used with [GeoCircle].
         *
         * Searches for documents where [field] is bounded by [shape].
         */
        public fun geoShape(
            shape: GeoShape,
            field: String = "_all",
        ): SearchQuery = SearchQuery.geoShape(shape, field)

        /**
         * Searches for documents that don't match [query].
         */
        public fun negation(query: SearchQuery): SearchQuery = boolean(mustNot = disjunction(query))

        /**
         * A [Conjunction query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-conjuncts-disjuncts.html)
         *
         * Searches for documents that match all the conjuncts (child queries joined by AND).
         */
        public fun conjunction(
            firstConjunct: SearchQuery,
            vararg remainingConjuncts: SearchQuery,
        ): ConjunctionQuery = SearchQuery.conjunction(firstConjunct, *remainingConjuncts)

        /**
         * A [Conjunction query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-conjuncts-disjuncts.html)
         *
         * Searches for documents that match all the [conjuncts] (child queries joined by AND).
         */
        public fun conjunction(conjuncts: List<SearchQuery>): ConjunctionQuery = SearchQuery.conjunction(conjuncts)

        /**
         * A [Disjunction query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-conjuncts-disjuncts.html)
         *
         * Searches for documents that match at least [min] of the disjuncts
         * (child queries joined by OR).
         */
        public fun disjunction(
            firstDisjunct: SearchQuery,
            vararg remainingDisjuncts: SearchQuery,
            min: Int = 1,
        ): DisjunctionQuery = SearchQuery.disjunction(firstDisjunct, remainingDisjuncts = remainingDisjuncts, min)

        /**
         * A [Disjunction query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-conjuncts-disjuncts.html)
         *
         * Searches for documents that match at least [min] of the [disjuncts]
         * (child queries joined by OR).
         */
        public fun disjunction(
            disjuncts: List<SearchQuery>,
            min: Int = 1,
        ): DisjunctionQuery = SearchQuery.disjunction(disjuncts, min)

        /**
         * A [Boolean query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-boolean-query.html)
         *
         * Searches for documents that match all of the [must] conditions and
         * none of the [mustNot] conditions, giving precedence to documents
         * matching the [should] conditions.
         */
        public fun boolean(
            must: ConjunctionQuery? = null,
            should: DisjunctionQuery? = null,
            mustNot: DisjunctionQuery? = null,
        ): SearchQuery = SearchQuery.boolean(must, should, mustNot)

        /**
         * [A Match All query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-all.html)
         *
         * A query that matches all indexed documents.
         */
        public fun matchAll(): SearchQuery = SearchQuery.matchAll()

        /**
         * [A Match None query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-none.html)
         *
         * A query that matches nothing.
         */
        public fun matchNone(): SearchQuery = SearchQuery.matchNone()

        /**
         * A Vector query.
         *
         * Vector queries can be ANDed or ORed together to create a compound vector query by passing
         * them to [SearchSpec.allOf] or [SearchSpec.anyOf].
         *
         * To combine the results of a vector query with the results of a non-vector query,
         * use [SearchSpec.mixedMode].
         *
         * @param vector The vector to compare against.
         * @param field The document field to search.
         *
         * @sample com.couchbase.client.kotlin.samples.searchSpecSimpleVectorQuery
         * @sample com.couchbase.client.kotlin.samples.searchSpecVectorAnyOf
         * @sample com.couchbase.client.kotlin.samples.searchSpecMixedMode
         */
        @SinceCouchbase("7.6")
        public fun vector(
            field: String,
            vector: FloatArray,
            numCandidates: Int = 3,
        ): VectorQuery = InternalVectorQuery(vector.clone(), field, numCandidates)

        /**
         * Combines vector queries using logical OR.
         *
         * **CAVEAT:** Nested compound vector queries like `"A or (B and C)"` are not supported.
         *
         * @sample com.couchbase.client.kotlin.samples.searchSpecVectorAnyOf
         */
        @SinceCouchbase("7.6")
        public fun anyOf(
            vectorQueries: List<VectorQuery>,
        ): VectorSearchSpec = CompoundVectorSearchSpec(vectorQueries, CoreVectorQueryCombination.OR)

        /**
         * Combines vector queries using logical OR.
         *
         * **CAVEAT:** Nested compound vector queries like `"A or (B and C)"` are not supported.
         *
         * @sample com.couchbase.client.kotlin.samples.searchSpecVectorAnyOf
         */
        @SinceCouchbase("7.6")
        public fun anyOf(
            first: VectorQuery,
            vararg remaining: VectorQuery,
        ): VectorSearchSpec = anyOf(listOf(first, *remaining))

        /**
         * Combines vector queries using logical AND.
         *
         * **CAVEAT:** Nested compound vector queries like `"A or (B and C)"` are not supported.
         *
         * @sample com.couchbase.client.kotlin.samples.searchSpecVectorAllOf
         */
        @SinceCouchbase("7.6")
        public fun allOf(
            vectorQueries: List<VectorQuery>,
        ): VectorSearchSpec = CompoundVectorSearchSpec(vectorQueries, CoreVectorQueryCombination.AND)

        /**
         * Combines vector queries using logical AND.
         *
         * **CAVEAT:** Nested compound vector queries like `"A or (B and C)"` are not supported.
         *
         * @sample com.couchbase.client.kotlin.samples.searchSpecVectorAllOf
         */
        @SinceCouchbase("7.6")
        public fun allOf(
            first: VectorQuery,
            vararg remaining: VectorQuery,
        ): VectorSearchSpec = allOf(listOf(first, *remaining))

    }
}

internal class MixedModeSearchSpec(
    private val searchQuery: SearchQuery,
    private val vectorQuery: VectorSearchSpec,
) : SearchSpec() {
    override val coreRequest: CoreSearchRequest
        get() = CoreSearchRequest(
            searchQuery.coreRequest.searchQuery,
            vectorQuery.coreRequest.vectorSearch,
        )
}
