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
import com.couchbase.client.kotlin.internal.putIfNotNull
import com.couchbase.client.kotlin.search.SearchFacet.Companion.date
import com.couchbase.client.kotlin.search.SearchFacet.Companion.numeric
import com.couchbase.client.kotlin.search.SearchFacet.Companion.term
import java.time.Instant

/**
 * A facet is like a histogram. For each document matching the search query,
 * the server inspects a field of the document to see which bin (or "category")
 * the field value belongs to.
 *
 * For [numeric] and [date] facets, you specify the categories up front as
 * value ranges. Common use cases include counting the number of documents in
 * certain price ranges, like: $1 to $5, $5 to $20, $20+, or time ranges like:
 * "today", "yesterday", or "more than a week ago".
 *
 * Unlike a histogram, it's okay if the ranges overlap. If a field value
 * matches more than one range, each matching range has its count incremented.
 *
 * For [term] facets, the server creates one category for each distinct value
 * it sees in the field. For example, let's say your documents have a "color"
 * field where the value is one of "red", "green", or "blue". The result of
 * a term facet targeting the "color" field tells you the number of times
 * each color appears as the field value.
 *
 * Facets have a `size` parameter, an upper bound on the number of categories
 * reported in the facet result. For example, if you request a size of 3,
 * the server will do its best to return the 3 largest categories. To be more
 * precise, it will select the top 3 categories from _each partition_ executing
 * the query, and then merge each partition's result into the final result.
 *
 * CAVEAT: If you are using multiple partitions and require an exact result,
 * the size must be >= the number of categories; otherwise the result should
 * be considered an estimate.
 *
 * Facet results are not affected by query pagination.
 *
 * To create a facet, use one of the companion factory methods.
 * To retrieve the result in a type-safe way, pass the facet to
 * [SearchResult.get] (or [SearchMetadata.get]). Alternatively, iterate over
 * [SearchResult.facets] (or [SearchMetadata.facets]) and cast each
 * [FacetResult] to the appropriate type.
 *
 * CAVEAT: Facets and/or ranges with no matching documents are omitted
 * from the results.
 *
 * @see numeric
 * @see date
 * @see term
 *
 * @sample com.couchbase.client.kotlin.samples.searchQueryWithFacets
 */
public sealed class SearchFacet(
    public val name: String,
    internal val field: String,
    internal val size: Int,
) {

    /**
     * Returns the JSON representation of this search facet.
     */
    public override fun toString(): String = Mapper.encodeAsString(toMap())

    internal fun toMap(): Map<String, Any?> {
        val map = mutableMapOf<String, Any?>()
        inject(map)
        return map
    }

    internal abstract fun inject(json: MutableMap<String, Any?>)

    public companion object {

        /**
         * Finds the [size] most frequent values for [field] among
         * all documents matching the search query.
         *
         * @see TermFacetResult
         *
         * @param field Name of the document field to inspect. The field should be
         * indexed using the `text` type and the `keyword` analyzer, otherwise multi-term
         * values are tokenized which might cause unexpected results.
         *
         * @param name An arbitrary name to assign to this facet. Can be used to
         * identify the associated [TermFacetResult] from the search response.
         */
        public fun term(
            field: String,
            size: Int,
            name: String = field,
        ): TermFacet = TermFacet(field, size, name)

        /**
         * Counts the search results whose value for [field] is within one of
         * the pre-defined [ranges].
         *
         * Reports the [size] most populous ranges and their counts.
         *
         * @see NumericFacetResult
         *
         * @param field Name of the document field to inspect. The field must be
         * indexed using the `number` type.
         *
         * @param size Maximum number of categories to report.
         *
         * @param ranges Pre-defined categories to assign documents to, based on whether
         * the field value matches one of the ranges.
         *
         * @param name An arbitrary name to assign to this facet. Can be used to
         * identify the associated [NumericFacetResult] from the search response.
         */
        public fun numeric(
            field: String,
            ranges: List<NumericRange>,
            size: Int = ranges.size,
            name: String = field,
        ): NumericFacet = NumericFacet(field, size, name, ranges)

        /**
         * Counts the search results whose value for [field] is within one of
         * the pre-defined [ranges].
         *
         * Reports the [size] most populous ranges and their counts.
         *
         * @see DateFacetResult
         *
         * @param field Name of the document field to inspect. The field must be
         * indexed using the `datetime` type.
         *
         * @param size Maximum number of categories to report.
         *
         * @param ranges Pre-defined categories to assign documents to, based on whether
         * the field value matches one of the ranges.
         *
         * @param name An arbitrary name to assign to this facet. Can be used to
         * identify the associated [DateFacetResult] from the search response.
         */
        public fun date(
            field: String,
            ranges: List<DateRange>,
            size: Int = ranges.size,
            name: String = field,
        ): DateFacet = DateFacet(field, size, name, ranges)
    }
}

/**
 * A named date range. May be unbounded at the start or end, but not both.
 *
 * @property start Lower bound inclusive, or null if unbounded.
 * @property end Upper bound exclusive, or null if unbounded.
 * @property name Arbitrary name to identify this range in the facet result.
 */
public class DateRange internal constructor(
    public val start: Instant? = null,
    public val end: Instant? = null,
    public override val name: String = "${start ?: "-∞"}<=x<${end ?: "∞"}",
) : Category {
    init {
        require(start != null || end != null) { "Date range requires at least one of 'start' or 'end'." }
        if (start != null && end != null) require(start < end) { "Date range 'start' must be <= 'end'." }
    }

    public companion object {
        /**
         * @param start Lower bound of the range, inclusive,
         * @param name Arbitrary name to identify this range in the facet result.
         */
        public fun lowerBound(
            start: Instant,
            name: String = "$start<=x<∞}",
        ): DateRange = DateRange(start = start, end = null, name = name)

        /**
         * @param end Upper bound of the range, exclusive,
         * @param name Arbitrary name to identify this range in the facet result.
         */
        public fun upperBound(
            end: Instant,
            name: String = "-∞<=x<$end}",
        ): DateRange = DateRange(start = null, end = end, name = name)

        /**
         * @param start Lower bound of the range, inclusive.
         * @param end Upper bound of the range, exclusive.
         * @param name Arbitrary name to identify this range in the facet result.
         */
        public fun bounds(
            start: Instant,
            end: Instant,
            name: String = "$start<=x<$end",
        ): DateRange = DateRange(start = start, end = end, name = name)
    }

    override fun toString(): String {
        return "DateRange(name='$name', start=$start, end=$end)"
    }

    internal fun toMap(): Map<String, Any?> {
        return mutableMapOf<String, Any?>().apply {
            put("name", name)
            putIfNotNull("start", start?.toString())
            putIfNotNull("end", end?.toString())
        }
    }
}

/**
 * A named numeric range. May be unbounded on one end or the other, but not both.
 *
 * @property min Lower bound inclusive, or null if unbounded.
 * @property max Upper bound exclusive, or null if unbounded.
 * @property name Arbitrary name to identify this range in the facet result.
 */
public class NumericRange internal constructor(
    public val min: Double?,
    public val max: Double?,
    public override val name: String,
) : Category {
    private constructor(min: Number?, max: Number?, name: String) : this(min.toDoubleSafe(), max.toDoubleSafe(), name)

    init {
        require(min != null || max != null) { "Numeric range requires at least one of 'min' or 'max'." }
        if (min != null && max != null) require(min < max) { "Numeric range 'min' must be < 'max'." }
    }

    public companion object {
        /**
         * @param min Lower bound of the range, inclusive.
         * @param name Arbitrary name to identify this range in the facet result.
         */
        public fun lowerBound(
            min: Number,
            name: String = "$min<=x<∞",
        ): NumericRange = NumericRange(min = min, max = null, name = name)

        /**
         * @param max Upper bound of the range, exclusive.
         * @param name Arbitrary name to identify this range in the facet result.
         */
        public fun upperBound(
            max: Number,
            name: String = "-∞<=x<$max",
        ): NumericRange = NumericRange(min = null, max = max, name = name)

        /**
         * @param min Lower bound of the range, inclusive.
         * @param max Upper bound of the range, exclusive.
         * @param name Arbitrary name to identify this range in the facet result.
         */
        public fun bounds(
            min: Number,
            max: Number,
            name: String = "$min<=x<$max",
        ): NumericRange = NumericRange(min = min, max = max, name = name)
    }

    internal fun toMap(): Map<String, Any?> {
        return mutableMapOf<String, Any?>().apply {
            put("name", name)
            putIfNotNull("min", min)
            putIfNotNull("max", max)
        }
    }

    override fun toString(): String = "NumericRange(name='$name', min=$min, max=$max)"
}

public class TermFacet internal constructor(
    field: String,
    size: Int,
    name: String,
) : SearchFacet(name, field, size) {
    override fun inject(json: MutableMap<String, Any?>) {
        json["field"] = field
        json["size"] = size
    }
}

public class NumericFacet internal constructor(
    field: String,
    size: Int,
    name: String,
    public val categories: List<NumericRange>,
) : SearchFacet(name, field, size) {
    init {
        require(categories.isNotEmpty()) { "Range facet must specify at least one range." }
    }

    override fun inject(json: MutableMap<String, Any?>) {
        json["field"] = field
        json["size"] = size
        json["numeric_ranges"] = categories.map { it.toMap() }
    }
}

public class DateFacet internal constructor(
    field: String,
    size: Int,
    name: String,
    public val categories: List<DateRange>,
) : SearchFacet(name, field, size) {
    init {
        require(categories.isNotEmpty()) { "Range facet must specify at least one range." }
    }

    override fun inject(json: MutableMap<String, Any?>) {
        json["field"] = field
        json["size"] = size
        json["date_ranges"] = categories.map { it.toMap() }
    }
}

public sealed interface Category {
    public val name: String
}

/**
 * A term facet category returned by the server.
 */
public class FrequentTerm(override val name: String) : Category {
    override fun toString(): String = name
}

/**
 * @param T For a numeric facet, this is [NumericRange].
 * For a date facet, it's [DateRange].
 * For a term facet, it's [FrequentTerm].
 *
 * @property category The category definition.
 *
 * @property count the number of search results whose facet field
 * belong to the associated category.
 */
public class CategoryResult<T : Category> internal constructor(
    public val category: T,
    public val count: Long,
) {
    public val name: String get() = category.name
    override fun toString(): String = "$name ($count)"
}

/**
 * @param T For a numeric facet, this is [NumericRange].
 * For a date facet, it's [DateRange].
 * For a term facet, it's [FrequentTerm].
 */
public sealed interface FacetResult<T : Category> {
    /**
     * The arbitrary name associated with this facet.
     */
    public val name: String

    /**
     * The name of the field the facet was built on.
     */
    public val field: String

    /**
     * The number of query results that had a value for the facet field.
     */
    public val total: Long

    /**
     * The number of query results that did not have a value for the facet field.
     */
    public val missing: Long

    /**
     * The number of query results that had a value for the facet field, but
     * whose value was not in any of the returned categories.
     */
    public val other: Long

    /**
     * The categories returned by the server, and the number of query results
     * whose facet field matched each category.
     */
    public val categories: List<CategoryResult<T>>
}

internal class BaseFacetResult<T : Category>(
    override val name: String,
    override val field: String,
    override val total: Long,
    override val missing: Long,
    override val other: Long,
    override val categories: List<CategoryResult<T>>,
) : FacetResult<T> {
    override fun toString(): String {
        return "FacetResult(name='$name', field='$field', total=$total, missing=$missing, other=$other, categories=$categories)"
    }
}

public class TermFacetResult internal constructor(
    private val base: BaseFacetResult<FrequentTerm>,
) : FacetResult<FrequentTerm> by base {
    override fun toString(): String = "Term$base"
}

public class NumericFacetResult internal constructor(
    private val base: BaseFacetResult<NumericRange>,
) : FacetResult<NumericRange> by base {

    override fun toString(): String = "Numeric$base"

    /**
     * Returns results for [range], or null if no documents matched the range,
     * or if the range was excluded due to the facet's size.
     *
     * @param range Range to fetch.
     */
    public operator fun get(range: NumericRange): CategoryResult<NumericRange>? {
        return categories.find { it.category.name == range.name }
    }

    /**
     * Returns results for the range named [name], or null if no documents matched the range,
     * or if the range was excluded due to the facet's size.
     *
     * @param name Name of the range to return.
     */
    public operator fun get(name: String): CategoryResult<NumericRange>? {
        return categories.find { it.category.name == name }
    }
}

public class DateFacetResult internal constructor(
    private val base: BaseFacetResult<DateRange>,
) : FacetResult<DateRange> by base {

    override fun toString(): String = "Date$base"

    /**
     * Returns results for [range], or null if no documents matched the range,
     * or if the range was excluded due to the facet's size.
     *
     * @param range Range to fetch.
     */
    public operator fun get(range: DateRange): CategoryResult<DateRange>? = get(range.name)

    /**
     * Returns results for the range named [name], or null if no documents matched the range,
     * or if the range was excluded due to the facet's size.
     *
     * @param name Name of the range to return.
     */
    public operator fun get(name: String): CategoryResult<DateRange>? =
        categories.find { it.category.name == name }
}

private const val MAX_SAFE_INTEGER = 9007199254740991
private const val MIN_SAFE_INTEGER = -9007199254740991
private val SAFE_INTEGERS = MIN_SAFE_INTEGER..MAX_SAFE_INTEGER

private fun Number?.toDoubleSafe(): Double? = when {
    this == null || this is Double -> this as? Double
    this is Int -> this.toDouble()
    this is Long && this in SAFE_INTEGERS -> this.toDouble()
    else -> throw IllegalArgumentException("Expected null, Double, Int, or a Long in $SAFE_INTEGERS, but got $this")
}
