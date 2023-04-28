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

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.search.CoreSearchKeyset
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference
import com.couchbase.client.core.json.Mapper

/**
 * Specifies the page to be returned by a search query.
 */
public sealed class SearchPage {

    internal class StartAt(val offset: Int) : SearchPage() {
        override fun toString(): String = "StartAt(offset=$offset)"
    }

    internal class SearchBefore(val keyset: SearchKeyset) : SearchPage() {
        override fun toString(): String = "SearchBefore(keyset=$keyset)"
    }

    internal class SearchAfter(val keyset: SearchKeyset) : SearchPage() {
        override fun toString(): String = "SearchAfter(keyset=$keyset)"
    }

    public companion object {

        /**
         * Offset pagination: return the page starting at row [offset].
         */
        public fun startAt(offset: Int): SearchPage = StartAt(offset);

        /**
         * Keyset pagination: return the page starting after [row].
         *
         * Requires the query's `sort` parameter to impose a total ordering
         * on the result set, typically by adding `byId()` as the final sort tier.
         */
        @SinceCouchbase("6.6.1")
        public fun searchAfter(row: SearchRow): SearchPage = searchAfter(row.keyset)


        /**
         * Keyset pagination: return the page starting after [keyset].
         *
         * Requires the query's `sort` parameter to impose a total ordering
         * on the result set, typically by adding `byId()` as the final sort tier.
         */
        @SinceCouchbase("6.6.1")
        public fun searchAfter(keyset: SearchKeyset): SearchPage = SearchAfter(keyset)

        /**
         * Keyset pagination: return the page ending before [row].
         *
         * Requires the query's `sort` parameter to impose a total ordering
         * on the result set, typically by adding `byId()` as the final sort tier.
         */
        @SinceCouchbase("6.6.1")
        public fun searchBefore(row: SearchRow): SearchPage = searchBefore(row.keyset)

        /**
         * Keyset pagination, returning the page ending before [keyset].
         *
         * Requires the query's `sort` parameter to impose a total ordering
         * on the result set, typically by adding `byId()` as the final sort tier.
         */
        @SinceCouchbase("6.6.1")
        public fun searchBefore(keyset: SearchKeyset): SearchPage = SearchBefore(keyset)
    }
}

/**
 * Identifies a row in the result set, for use with keyset pagination.
 */
public class SearchKeyset internal constructor(internal val components: List<String>) {
    public companion object {
        /**
         * Returns the deserialized form of [serialized].
         */
        public fun deserialize(serialized: String): SearchKeyset {
            return SearchKeyset(Mapper.decodeInto(serialized, object : TypeReference<List<String>>() {}))
        }
    }

    /**
     * Returns a string you can pass to [deserialize] later to get an equivalent SearchKeyset.
     */
    public fun serialize(): String = Mapper.encodeAsString(components)

    override fun toString(): String = serialize()

    internal val core: CoreSearchKeyset
        get() = CoreSearchKeyset(components)
}
