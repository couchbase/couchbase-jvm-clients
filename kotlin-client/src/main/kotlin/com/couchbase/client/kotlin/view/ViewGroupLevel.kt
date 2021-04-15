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

import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.kotlin.view.ViewGroupLevel.Companion.none
import com.couchbase.client.kotlin.view.ViewGroupLevel.Companion.exact
import com.couchbase.client.kotlin.view.ViewGroupLevel.Companion.of

/**
 * Specifies how to group the keys when a reduce function is used.
 * Create new instances using the factory methods.
 *
 * @see none
 * @see exact
 * @see of
 */
public sealed class ViewGroupLevel {
    internal abstract fun inject(params: UrlQueryStringBuilder)

    private object Exact : ViewGroupLevel() {
        override fun inject(params: UrlQueryStringBuilder): Unit {
            params.set("group", true)
        }

        override fun toString(): String = "All"
    }

    private object None : ViewGroupLevel() {
        override fun inject(params: UrlQueryStringBuilder): Unit {
            // group=false is the default.
            // Don't pass it explicitly, since it's an error to specify
            // the group setting without reduction, and we don't know
            // whether a reduce function is present.
        }

        override fun toString(): String = "None"
    }

    private class Level(
        val level: Int,
    ) : ViewGroupLevel() {
        override fun inject(params: UrlQueryStringBuilder) {
            params.set("group_level", level)
        }

        override fun toString(): String {
            return "Level(level=$level)"
        }
    }

    public companion object {
        /**
         * Corresponds to `group=false` in the REST API.
         */
        public fun none(): ViewGroupLevel = None

        /**
         * Corresponds to `group=true` in the REST API.
         */
        public fun exact(): ViewGroupLevel = Exact

        /**
         * Corresponds to `group_level=<level>` in the REST API.
         */
        public fun of(level: Int): ViewGroupLevel = if (level == 0) None else Level(level)
    }
}

