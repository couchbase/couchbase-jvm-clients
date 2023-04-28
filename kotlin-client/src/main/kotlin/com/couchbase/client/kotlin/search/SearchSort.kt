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

import com.couchbase.client.core.api.search.sort.CoreSearchFieldMissing
import com.couchbase.client.core.api.search.sort.CoreSearchFieldMode
import com.couchbase.client.core.api.search.sort.CoreSearchFieldType
import com.couchbase.client.core.api.search.sort.CoreSearchSort
import com.couchbase.client.core.api.search.sort.CoreSearchSortField
import com.couchbase.client.core.api.search.sort.CoreSearchSortGeoDistance
import com.couchbase.client.core.api.search.sort.CoreSearchSortId
import com.couchbase.client.core.api.search.sort.CoreSearchSortScore
import com.couchbase.client.core.api.search.sort.CoreSearchSortString
import com.couchbase.client.kotlin.search.SearchSort.Companion.of

public enum class Direction { ASCENDING, DESCENDING }

public enum class FieldType(internal val core: CoreSearchFieldType) {
    AUTO(CoreSearchFieldType.AUTO),
    STRING(CoreSearchFieldType.STRING),
    NUMBER(CoreSearchFieldType.NUMBER),
    DATE(CoreSearchFieldType.DATE),
    ;
}

public enum class Mode(internal val core: CoreSearchFieldMode) {
    DEFAULT(CoreSearchFieldMode.DEFAULT),
    MIN(CoreSearchFieldMode.MIN),
    MAX(CoreSearchFieldMode.MAX),
    ;
}

public enum class Missing(internal val core: CoreSearchFieldMissing) {
    FIRST(CoreSearchFieldMissing.FIRST),
    LAST(CoreSearchFieldMissing.LAST),
    ;
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

    internal val core : List<CoreSearchSort> = components.map { it.core }


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
    internal abstract val core: CoreSearchSort
    override fun toString(): String = core.toString()
}

internal class SearchSortFromString internal constructor(sort: String) : SearchSortComponent() {
    override val core = CoreSearchSortString(sort)
}

internal class SearchSortScore internal constructor(direction: Direction) : SearchSortComponent() {
    override val core = CoreSearchSortScore(direction == Direction.DESCENDING)
}

internal class SearchSortId internal constructor(direction: Direction) : SearchSortComponent() {
    override val core = CoreSearchSortId(direction == Direction.DESCENDING)
}

internal class SearchSortField internal constructor(
    field: String,
    type: FieldType,
    mode: Mode,
    missing: Missing,
    direction: Direction,
) : SearchSortComponent() {
    override val core = CoreSearchSortField(
        field,
        type.core,
        mode.core,
        missing.core,
        direction == Direction.DESCENDING
    )
}

internal class SearchSortGeoDistance internal constructor(
    private val field: String,
    private val location: GeoPoint,
    private val unit: GeoDistanceUnit,
    private val direction: Direction,
) : SearchSortComponent() {
    override val core = CoreSearchSortGeoDistance(
        location.core,
        field,
        unit.core,
        direction == Direction.DESCENDING,
    )
}
