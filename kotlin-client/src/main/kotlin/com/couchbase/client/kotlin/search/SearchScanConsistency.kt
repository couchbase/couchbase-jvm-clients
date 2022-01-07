/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.search

import com.couchbase.client.kotlin.kv.MutationState
import com.couchbase.client.kotlin.search.SearchScanConsistency.Companion.consistentWith
import com.couchbase.client.kotlin.search.SearchScanConsistency.Companion.notBounded

/**
 * Create instances using the [consistentWith] or [notBounded]
 * factory methods.
 */
public sealed class SearchScanConsistency {

    internal open fun inject(indexName: String, searchJson: MutableMap<String, Any?>): Unit {
    }

    public companion object {
        /**
         * For when speed matters more than consistency. Executes the search
         * immediately, without waiting for prior K/V mutations to be indexed.
         */
        public fun notBounded(): SearchScanConsistency =
            NotBounded

        /**
         * Targeted consistency. Waits for specific K/V mutations to be indexed
         * before executing the search.
         *
         * Sometimes referred to as "At Plus".
         *
         * @param tokens the mutations to await before executing the search
         */
        public fun consistentWith(tokens: MutationState): SearchScanConsistency =
            if (tokens.isEmpty()) NotBounded else ConsistentWith(tokens)
    }

    private object NotBounded : SearchScanConsistency() {
        override fun toString(): String = "NotBounded"
    }

    private class ConsistentWith internal constructor(
        private val tokens: MutationState,
    ) : SearchScanConsistency() {
        override fun inject(indexName: String, searchJson: MutableMap<String, Any?>): Unit {
            searchJson["consistency"] = mapOf(
                "level" to "at_plus",
                "vectors" to mapOf(indexName to tokens.exportForSearch()),
            )
        }

        override fun toString(): String {
            return "ConsistentWith(tokens=$tokens)"
        }
    }
}

private fun Iterable<*>.isEmpty() = !iterator().hasNext()
