/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.view

import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.kotlin.view.ViewSelection.Companion.key
import com.couchbase.client.kotlin.view.ViewSelection.Companion.range

/**
 * Specify whether to query the view for specific keys, or a range.
 * Create new instances using one of the factory methods.
 * @see key
 * @see keys
 * @see range
 */
public sealed class ViewSelection {

    internal abstract fun inject(params: UrlQueryStringBuilder)
    internal abstract fun keys(): List<Any?>

    private class Keys(
        private val keys: List<Any?>
    ) : ViewSelection() {

        override fun inject(params: UrlQueryStringBuilder) {}

        override fun keys(): List<Any?> = keys

        override fun toString(): String {
            return "Keys(keys=$keys)"
        }
    }

    private class Range(
        private val descending: Boolean,
        private val startKey: Any?,
        private val startKeyDocId: String?,
        private val endKey: Any?,
        private val endKeyDocId: String?,
        private val inclusiveEnd: Boolean,
    ) : ViewSelection() {

        init {
            require(startKeyDocId == null || startKey != null) { "Must specify startKey if startKeyDocId is specified." }
            require(endKeyDocId == null || endKey != null) { "Must specify endKey if endKeyDocId is specified." }
        }

        override fun keys(): List<Any?> = emptyList()

        override fun inject(params: UrlQueryStringBuilder) {
            params.set("descending", descending)
            startKey?.let { params.set("startkey", Mapper.encodeAsString(it)) }
            startKeyDocId?.let { params.set("startkey_docid", it) }
            endKey?.let { params.set("endkey", Mapper.encodeAsString(it)) }
            endKeyDocId?.let { params.set("endkey_docid", it) }
            if (!inclusiveEnd) params.set("inclusive_end", inclusiveEnd)
        }

        override fun toString(): String {
            return "Range(descending=$descending, startKey=$startKey, startKeyDocId=$startKeyDocId, endKey=$endKey, endKeyDocId=$endKeyDocId, inclusiveEnd=$inclusiveEnd)"
        }
    }

    public companion object {
        /**
         * Select a specific key.
         */
        public fun key(key: Any?): ViewSelection = Keys(listOf(key))

        /**
         * Select multiple specific keys.
         */
        public fun keys(keys: List<Any?>): ViewSelection = Keys(keys)

        /**
         * Select a range of keys.
         */
        public fun range(
            order: ViewOrdering = ViewOrdering.ASCENDING,
            startKey: Any? = null,
            startKeyDocId: String? = null,
            endKey: Any? = null,
            endKeyDocId: String? = null,
            inclusiveEnd: Boolean = true,
        ): ViewSelection = Range(
            order == ViewOrdering.DESCENDING,
            startKey,
            startKeyDocId,
            endKey,
            endKeyDocId,
            inclusiveEnd,
        )
    }
}
