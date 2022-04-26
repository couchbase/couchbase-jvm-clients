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

import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.internal.putIfFalse
import com.couchbase.client.kotlin.internal.putIfNotNull
import com.couchbase.client.kotlin.internal.putIfNotZero
import com.couchbase.client.kotlin.internal.putIfTrue
import com.couchbase.client.kotlin.search.GeoShape.Companion.circle
import com.couchbase.client.kotlin.search.SearchQuery.Companion.matchPhrase
import com.couchbase.client.kotlin.search.SearchQuery.Companion.term
import java.time.Instant

/**
 * A [Full-Text Search query](https://docs.couchbase.com/server/current/fts/fts-supported-queries.html)
 */
public sealed class SearchQuery {

    /**
     * Returns a new query that decorates this one with the given boost multiplier.
     * Has no effect unless this query is used in a disjunction or conjunction.
     */
    public infix fun boost(boost: Number): SearchQuery = build {
        inject(this)
        put("boost", boost)
    }

    /**
     * Returns the JSON representation of this query condition.
     */
    public override fun toString(): String = Mapper.encodeAsString(toMap())

    internal fun toMap(): Map<String, Any?> {
        val map = mutableMapOf<String, Any?>()
        inject(map)
        return map
    }

    internal abstract fun inject(json: MutableMap<String, Any?>)

    public companion object {
        public enum class MatchOperator {
            AND,
            OR,
            ;

            internal val wireName: String = name.lowercase()
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
        ): SearchQuery = build {
            putField(field)
            put("match", match)
            putIfNotNull("analyzer", analyzer)
            putIfNotZero("fuzziness", fuzziness)
            putIfNotZero("prefix_length", prefixLength)
            if (operator != MatchOperator.OR) put("operator", operator.wireName)
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
        ): SearchQuery = build {
            putField(field)
            put("match_phrase", matchPhrase)
            putIfNotNull("analyzer", analyzer)
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
        ): SearchQuery = build {
            putField(field)
            put("term", term)
            putIfNotZero("fuzziness", fuzziness)
            putIfNotZero("prefix_length", prefixLength)
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
        ): SearchQuery {
            require(terms.isNotEmpty()) { "Phrase query 'terms' must not be empty." }
            return build {
                putField(field)
                put("terms", terms)
            }
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
        ): SearchQuery = build {
            putField(field)
            put("prefix", prefix)
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
        ): SearchQuery = build {
            putField(field)
            put("regexp", regexp)
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
        ): SearchQuery = build {
            put("wildcard", term)
            putField(field)
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
        ): SearchQuery = build { put("query", queryString) }

        /**
         * A [Boolean Field query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-boolean-field-query.html).
         * Searches for documents where [field] has the value [bool].
         */
        public fun booleanField(
            bool: Boolean,
            field: String,
        ): SearchQuery = build {
            put("bool", bool)
            putField(field)
        }

        /**
         * A [DocId query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-DocID-query.html).
         * Searches for documents whose ID is one of [ids].
         */
        public fun documentId(
            ids: List<String>,
        ): SearchQuery {
            require(ids.isNotEmpty()) { "Document ID query 'ids' must not be empty." }
            return build { put("ids", ids) }
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
        ): SearchQuery {
            require(min != null || max != null) { "Term range query needs at least one of 'min' or 'max'." }
            return build {
                putIfNotNull("min", min)
                putIfFalse("inclusive_min", inclusiveMin)
                putIfNotNull("max", max)
                putIfTrue("inclusive_max", inclusiveMax)
                putField(field)
            }
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
        ): SearchQuery {
            require(min != null || max != null) { "Numeric range query needs at least one of 'min' or 'max'." }
            return build {
                putIfNotNull("min", min)
                putIfFalse("inclusive_min", inclusiveMin)
                putIfNotNull("max", max)
                putIfTrue("inclusive_max", inclusiveMax)
                putField(field)
            }
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
        ): SearchQuery {
            require(start != null || end != null) { "Date range query needs at least one of 'start' or 'end'." }
            return build {
                putIfNotNull("start", start?.toString())
                putIfFalse("inclusive_start", inclusiveStart)
                putIfNotNull("end", end?.toString())
                putIfTrue("inclusive_end", inclusiveEnd)
                putField(field)
            }
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
        ): SearchQuery = build {
            shape.inject(this)
            putField(field)
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
        ): SearchQuery {
            require(must != null || should != null || mustNot != null) { "Boolean query must have at least one of 'must', 'should', or 'mustNot'." }
            return build {
                putIfNotNull("must", must?.toMap())
                putIfNotNull("should", should?.toMap())
                putIfNotNull("must_not", mustNot?.toMap())
            }
        }

        /**
         * [A Match All query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-all.html)
         *
         * A query that matches all indexed documents.
         */
        public fun matchAll(): SearchQuery = build { put("match_all", null) }

        /**
         * [A Match None query](https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-none.html)
         *
         * A query that matches nothing.
         */
        public fun matchNone(): SearchQuery = build { put("match_none", null) }

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
        public fun custom(customizer: MutableMap<String, Any?>.() -> Unit): SearchQuery = build(customizer)

        private fun build(customizer: MutableMap<String, Any?>.() -> Unit): SearchQuery {
            return BuiltQuery(customizer)
        }
    }
}

internal class BuiltQuery(
    private val customizer: MutableMap<String, Any?>.() -> Unit,
) : SearchQuery() {
    override fun inject(json: MutableMap<String, Any?>) {
        json.customizer()
    }
}

/**
 * Create an instance using [SearchQuery.conjunction].
 */
public class ConjunctionQuery internal constructor(
    private val conjuncts: List<SearchQuery>,
) : SearchQuery() {

    init {
        require(conjuncts.isNotEmpty()) { "Conjunction query must have at least 1 conjunct." }
    }

    override fun inject(json: MutableMap<String, Any?>) {
        with(json) {
            put("conjuncts", conjuncts.map { it.toMap() })
        }
    }
}

/**
 * Create an instance using [SearchQuery.disjunction].
 */
public class DisjunctionQuery internal constructor(
    private val disjuncts: List<SearchQuery>,
    private val min: Int = 1,
) : SearchQuery() {

    init {
        require(min > 0) { "Disjunction query 'min' must be > 0." }
        require(disjuncts.size >= min) { "Disjunction query must have at least $min disjuncts when 'min' is $min." }
    }

    override fun inject(json: MutableMap<String, Any?>) {
        with(json) {
            if (min > 1) put("min", min)
            put("disjuncts", disjuncts.map { it.toMap() })
        }
    }
}

private fun MutableMap<String, Any?>.putField(field: String) {
    if (field != "_all") put("field", field)
}
