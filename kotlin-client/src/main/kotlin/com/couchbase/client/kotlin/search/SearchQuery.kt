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

import com.couchbase.client.core.api.search.CoreSearchQuery
import com.couchbase.client.core.api.search.queries.CoreBooleanFieldQuery
import com.couchbase.client.core.api.search.queries.CoreBooleanQuery
import com.couchbase.client.core.api.search.queries.CoreConjunctionQuery
import com.couchbase.client.core.api.search.queries.CoreCustomQuery
import com.couchbase.client.core.api.search.queries.CoreDateRangeQuery
import com.couchbase.client.core.api.search.queries.CoreDisjunctionQuery
import com.couchbase.client.core.api.search.queries.CoreDocIdQuery
import com.couchbase.client.core.api.search.queries.CoreGeoBoundingBoxQuery
import com.couchbase.client.core.api.search.queries.CoreGeoDistanceQuery
import com.couchbase.client.core.api.search.queries.CoreGeoPolygonQuery
import com.couchbase.client.core.api.search.queries.CoreMatchAllQuery
import com.couchbase.client.core.api.search.queries.CoreMatchNoneQuery
import com.couchbase.client.core.api.search.queries.CoreMatchOperator
import com.couchbase.client.core.api.search.queries.CoreMatchPhraseQuery
import com.couchbase.client.core.api.search.queries.CoreMatchQuery
import com.couchbase.client.core.api.search.queries.CoreNumericRangeQuery
import com.couchbase.client.core.api.search.queries.CorePhraseQuery
import com.couchbase.client.core.api.search.queries.CorePrefixQuery
import com.couchbase.client.core.api.search.queries.CoreQueryStringQuery
import com.couchbase.client.core.api.search.queries.CoreRegexpQuery
import com.couchbase.client.core.api.search.queries.CoreTermQuery
import com.couchbase.client.core.api.search.queries.CoreTermRangeQuery
import com.couchbase.client.core.api.search.queries.CoreWildcardQuery
import com.couchbase.client.kotlin.search.GeoShape.Companion.circle
import com.couchbase.client.kotlin.search.SearchQuery.Companion.matchPhrase
import com.couchbase.client.kotlin.search.SearchQuery.Companion.term
import java.time.Instant

/**
 * A [Full-Text Search query](https://docs.couchbase.com/server/current/fts/fts-supported-queries.html)
 */
public sealed class SearchQuery {
    internal abstract val core: CoreSearchQuery
    internal abstract fun withBoost(boost: Double?): SearchQuery

    /**
     * Returns a new query that decorates this one with the given boost multiplier.
     * Has no effect unless this query is used in a disjunction or conjunction.
     */
    public infix fun boost(boost: Number): SearchQuery = this.withBoost(boost.toDouble())

    /**
     * Returns the JSON representation of this query condition.
     */
    public override fun toString(): String = core.export().toString()

    public companion object {
        public enum class MatchOperator(internal val core: CoreMatchOperator) {
            AND(CoreMatchOperator.AND),
            OR(CoreMatchOperator.OR),
            ;
        }

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
        ): SearchQuery = MatchQuery(match, field, analyzer, operator, fuzziness, prefixLength)

        internal data class MatchQuery(
            val match: String,
            val field: String,
            val analyzer: String?,
            val operator: MatchOperator,
            val fuzziness: Int,
            val prefixLength: Int,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreMatchQuery(match, field, analyzer, prefixLength, fuzziness, operator.core, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): SearchQuery = MatchPhraseQuery(matchPhrase, field, analyzer)

        internal data class MatchPhraseQuery(
            val matchPhrase: String,
            val field: String = "_all",
            val analyzer: String? = null,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreMatchPhraseQuery(matchPhrase, field, analyzer, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): SearchQuery = TermQuery(term, field, fuzziness, prefixLength)

        internal data class TermQuery(
            val term: String,
            val field: String,
            val fuzziness: Int,
            val prefixLength: Int,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreTermQuery(term, field, fuzziness, prefixLength, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): SearchQuery = PhraseQuery(terms, field)

        internal data class PhraseQuery(
            val terms: List<String>,
            val field: String,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CorePhraseQuery(terms, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): SearchQuery = PrefixQuery(prefix, field)

        internal data class PrefixQuery(
            val prefix: String,
            val field: String,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CorePrefixQuery(prefix, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): SearchQuery = RegexpQuery(regexp, field)

        internal data class RegexpQuery(
            val regexp: String,
            val field: String,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreRegexpQuery(regexp, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): SearchQuery = WildcardQuery(term, field)

        internal data class WildcardQuery(
            val term: String,
            val field: String,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreWildcardQuery(term, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): SearchQuery = QueryStringQuery(queryString)

        internal data class QueryStringQuery(
            val queryString: String,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreQueryStringQuery(queryString, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * A [Boolean Field query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-boolean-field-query.html).
         * Searches for documents where [field] has the value [bool].
         */
        public fun booleanField(
            bool: Boolean,
            field: String,
        ): SearchQuery = BooleanFieldQuery(bool, field)

        internal data class BooleanFieldQuery(
            val bool: Boolean,
            val field: String,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreBooleanFieldQuery(bool, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * A [DocId query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-DocID-query.html).
         * Searches for documents whose ID is one of [ids].
         */
        public fun documentId(
            ids: List<String>,
        ): SearchQuery = DocumentIdQuery(ids)

        internal data class DocumentIdQuery(
            val ids: List<String>,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreDocIdQuery(boost, ids)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * A [Term Range query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-term-range.html)
         */
        public fun termRange(
            field: String = "_all",
            min: String? = null,
            inclusiveMin: Boolean = true,
            max: String? = null,
            inclusiveMax: Boolean = false,
        ): SearchQuery = TermRangeQuery(field, min, inclusiveMin, max, inclusiveMax)

        internal data class TermRangeQuery(
            val field: String,
            val min: String?,
            val inclusiveMin: Boolean,
            val max: String?,
            val inclusiveMax: Boolean,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreTermRangeQuery(min, max, inclusiveMin, inclusiveMax, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * A [Numeric Range query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-numeric-range.html)
         */
        public fun numericRange(
            field: String = "_all",
            min: Number? = null,
            inclusiveMin: Boolean = true,
            max: Number? = null,
            inclusiveMax: Boolean = false,
        ): SearchQuery = NumericRangeQuery(field, min, inclusiveMin, max, inclusiveMax)

        internal data class NumericRangeQuery(
            val field: String,
            val min: Number?,
            val inclusiveMin: Boolean,
            val max: Number?,
            val inclusiveMax: Boolean,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreNumericRangeQuery(min?.toDouble(), max?.toDouble(), inclusiveMin, inclusiveMax, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * A [Date Range query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-date-range.html)
         */
        public fun dateRange(
            field: String = "_all",
            start: Instant? = null,
            inclusiveStart: Boolean = true,
            end: Instant? = null,
            inclusiveEnd: Boolean = false,
        ): SearchQuery = DateRangeQuery(field, start, inclusiveStart, end, inclusiveEnd)

        internal data class DateRangeQuery(
            val field: String,
            val start: Instant?,
            val inclusiveStart: Boolean,
            val end: Instant?,
            val inclusiveEnd: Boolean,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreDateRangeQuery(start?.toString(), end?.toString(), inclusiveStart, inclusiveEnd, null, field, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * A [Geo Point Distance query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-geo-point-distance.html)
         *
         * Alias for [geoShape] using a circle centered at [location] with a radius of [distance].
         */
        public fun geoDistance(
            location: GeoPoint,
            distance: GeoDistance,
            field: String = "_all",
        ): SearchQuery = geoShape(circle(location, distance), field)

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
        ): SearchQuery = GeoShapeQuery(shape, field)

        internal data class GeoShapeQuery(
            val shape: GeoShape,
            val field: String,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = when (shape) {
                is GeoCircle -> CoreGeoDistanceQuery(shape.center.core, shape.radius.serialize(), field, boost)
                is GeoPolygon -> CoreGeoPolygonQuery(shape.vertices.map { it.core }, field, boost)
                is GeoRectangle -> CoreGeoBoundingBoxQuery(shape.topLeft.core, shape.bottomRight.core, field, boost)
            }

            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

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
        ): ConjunctionQuery = conjunction(listOf(firstConjunct, *remainingConjuncts))

        /**
         * A [Conjunction query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-conjuncts-disjuncts.html)
         *
         * Searches for documents that match all the [conjuncts] (child queries joined by AND).
         */
        public fun conjunction(conjuncts: List<SearchQuery>): ConjunctionQuery = ConjunctionQuery(conjuncts)

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
        ): DisjunctionQuery = disjunction(listOf(firstDisjunct, *remainingDisjuncts), min)

        /**
         * A [Disjunction query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-conjuncts-disjuncts.html)
         *
         * Searches for documents that match at least [min] of the [disjuncts]
         * (child queries joined by OR).
         */
        public fun disjunction(
            disjuncts: List<SearchQuery>,
            min: Int = 1,
        ): DisjunctionQuery = DisjunctionQuery(disjuncts, min)

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
        ): SearchQuery = BooleanQuery(must, should, mustNot)

        internal data class BooleanQuery(
            val must: ConjunctionQuery?,
            val should: DisjunctionQuery?,
            val mustNot: DisjunctionQuery?,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreBooleanQuery(must?.core, mustNot?.core, should?.core, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * [A Match All query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-all.html)
         *
         * A query that matches all indexed documents.
         */
        public fun matchAll(): SearchQuery = MatchAllQuery()

        internal data class MatchAllQuery(
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreMatchAllQuery(boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * [A Match None query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-none.html)
         *
         * A query that matches nothing.
         */
        public fun matchNone(): SearchQuery = MatchNoneQuery()

        internal data class MatchNoneQuery(
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreMatchNoneQuery(boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }

        /**
         * Escape hatch for specifying a custom query condition supported
         * by Couchbase Server but not by this version of the SDK.
         *
         * The [customizer] lambda populates a map that gets converted to JSON.
         *
         * Example:
         * ```
         * val query = custom {
         *     put("wildcard", "foo?ball)
         *     put("field", "sport")
         * }
         * ```
         * yields the query JSON:
         * ```
         * {
         *     "wildcard": "foo?ball",
         *     "field": "sport"
         * }
         * ```
         */
        public fun custom(customizer: MutableMap<String, Any?>.() -> Unit): SearchQuery {
            val params: MutableMap<String, Any?> = mutableMapOf()
            params.customizer()
            return CustomQuery(params)
        }

        internal data class CustomQuery(
            val params: Map<String, Any?>,
            val boost: Double? = null,
        ) : SearchQuery() {
            override val core = CoreCustomQuery(params, boost)
            override fun withBoost(boost: Double?) = copy(boost = boost)
        }
    }
}

/**
 * Create an instance using [SearchQuery.conjunction].
 */
public class ConjunctionQuery internal constructor(
    private val conjuncts: List<SearchQuery>,
    boost: Double? = null,
) : SearchQuery() {
    override val core = CoreConjunctionQuery(conjuncts.map { it.core }, boost)
    override fun withBoost(boost: Double?) = ConjunctionQuery(conjuncts, boost)
}

/**
 * Create an instance using [SearchQuery.disjunction].
 */
public class DisjunctionQuery internal constructor(
    private val disjuncts: List<SearchQuery>,
    private val min: Int = 1,
    boost: Double? = null,
) : SearchQuery() {
    override val core = CoreDisjunctionQuery(disjuncts.map { it.core }, min, boost)
    override fun withBoost(boost: Double?) = DisjunctionQuery(disjuncts, min, boost)
}
