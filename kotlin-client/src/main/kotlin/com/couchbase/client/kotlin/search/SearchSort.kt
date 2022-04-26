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

public enum class Direction { ASCENDING, DESCENDING }

public enum class FieldType {
    AUTO,
    STRING,
    NUMBER,
    DATE,
    ;

    internal val value = name.lowercase()
}

public enum class Mode {
    DEFAULT,
    MIN,
    MAX,
    ;

    internal val value = name.lowercase()
}

public enum class Missing {
    FIRST,
    LAST,
    ;

    internal val value = name.lowercase()
}

/**
 * Specifies how to sort the results of a search query.
 *
 * Create instances using the companion factory methods.
 *
 * For tiered sorting, chain using [then] or [of].
 *
 * @sample com.couchbase.client.kotlin.samples.searchTieredSort
 */
public class SearchSort internal constructor(private val components: List<SearchSortComponent>) {
    internal constructor(e: SearchSortComponent) : this(listOf(e))

    internal fun inject(params: MutableMap<String, Any?>) {
        if (components.isNotEmpty()) {
            params["sort"] = components.map { it.encode() }
        }
    }

    /**
     * Returns a tiered sort consisting of this sort followed by [other].
     * @sample com.couchbase.client.kotlin.samples.searchTieredSort
     */
    public infix fun then(other: SearchSort) : SearchSort = SearchSort(components + other.components)

    override fun toString(): String {
        return "SearchSort($components)"
    }

    public companion object {

        public fun byScore(
            direction: Direction = Direction.DESCENDING,
        ): SearchSort = SearchSort(SearchSortScore(direction))

        public fun byId(
            direction: Direction = Direction.ASCENDING,
        ): SearchSort = SearchSort(SearchSortId(direction))

        /**
         * @param field Specifies the name of a field on which to sort.
         *
         * @param mode Specifies the search-order for index-fields that
         * contain multiple values (as a consequence of arrays or multi-token
         * analyzer-output). The default order is undefined but deterministic,
         * allowing the paging of results from `from (offset)` with reliable
         * ordering. To sort using the minimum or maximum value, the value
         * of mode should be set to either min or max.
         *
         * @param type Specifies the type of the search-order field value.
         * For example, string for text fields, date for DateTime fields,
         * or number for numeric/geo fields.
         *
         * @param missing Specifies the sort-procedure for documents with
         * a missing value in a field specified for sorting. The value of
         * missing can be first, in which case results with missing values
         * appear before other results, or last (the default), in which case
         * they appear after.
         */
        public fun byField(
            field: String,
            type: FieldType = FieldType.AUTO,
            mode: Mode = Mode.DEFAULT,
            missing: Missing = Missing.LAST,
            direction: Direction = Direction.ASCENDING,
        ): SearchSort = SearchSort(SearchSortField(field, type, mode, missing, direction))

        public fun byGeoDistance(
            field: String,
            location: GeoPoint,
            unit: GeoDistanceUnit = GeoDistanceUnit.METERS,
            direction: Direction = Direction.ASCENDING,
        ): SearchSort = SearchSort(SearchSortGeoDistance(field, location, unit, direction))

        /**
         * Specifies the sort order using strings.
         * Syntax is described by [Sorting With Strings](https://docs.couchbase.com/server/current/fts/fts-search-response.html#sorting-with-strings)
         */
        public fun by(firstSort: String, vararg remainingSorts: String): SearchSort =
            by(listOf(firstSort, *remainingSorts))

        /**
         * Specifies the sort order using strings.
         * Syntax is described by [Sorting With Strings](https://docs.couchbase.com/server/current/fts/fts-search-response.html#sorting-with-strings)
         */
        public fun by(sorts: List<String>): SearchSort =
            SearchSort(sorts.map { SearchSortFromString(it) })

        /**
         * Concatenates [sorts] into a single SearchSort,
         * for when it's convenient to build a sort from a list
         * instead of using [then].
         */
        public fun of(sorts: List<SearchSort>): SearchSort = SearchSort(sorts.flatMap { it.components })
    }
}

internal sealed class SearchSortComponent {
    internal abstract fun encode(): Any
    override fun toString(): String = encode().toString()
}

internal class SearchSortFromString internal constructor(private val sort: String) : SearchSortComponent() {
    override fun encode(): Any = sort
}

internal class SearchSortScore internal constructor(private val direction: Direction) : SearchSortComponent() {
    override fun encode(): Any {
        val result = mutableMapOf<String, Any?>("by" to "score")
        if (direction == Direction.DESCENDING) result["desc"] = true
        return result
    }
}

internal class SearchSortId internal constructor(private val direction: Direction) : SearchSortComponent() {
    override fun encode(): Any {
        val result = mutableMapOf<String, Any?>("by" to "id")
        if (direction == Direction.DESCENDING) result["desc"] = true
        return result
    }
}

internal class SearchSortField internal constructor(
    private val field: String,
    private val type: FieldType,
    private val mode: Mode,
    private val missing: Missing,
    private val direction: Direction,
) : SearchSortComponent() {
    override fun encode(): Any {
        val result = mutableMapOf<String, Any?>(
            "by" to "field",
            "field" to field,
        )

        if (direction == Direction.DESCENDING) result["desc"] = true
        if (type != FieldType.AUTO) result["type"] = type.value
        if (mode != Mode.DEFAULT) result["mode"] = mode.value
        if (missing != Missing.LAST) result["missing"] = missing.value

        return result
    }
}

internal class SearchSortGeoDistance internal constructor(
    private val field: String,
    private val location: GeoPoint,
    private val unit: GeoDistanceUnit,
    private val direction: Direction,
) : SearchSortComponent() {
    override fun encode(): Any {
        val result = mutableMapOf<String, Any?>(
            "by" to "geo_distance",
            "field" to field,
            "location" to location.serialize(),
        )

        if (direction == Direction.DESCENDING) result["desc"] = true
        if (unit != GeoDistanceUnit.METERS) result["unit"] = unit.value

        return result
    }
}
