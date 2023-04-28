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

public class HighlightStyle internal constructor(
    public val name: String,
) {
    public companion object {
        public val HTML: HighlightStyle = HighlightStyle("html")
        public val ANSI: HighlightStyle = HighlightStyle("ansi")

        public fun of(name: String): HighlightStyle {
            if (name == HTML.name) return HTML
            if (name == ANSI.name) return ANSI
            return HighlightStyle(name)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as HighlightStyle

        if (name != other.name) return false

        return true
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }

    override fun toString(): String {
        return name
    }
}

public sealed class Highlight {

    public companion object {
        /**
         * No highlighting.
         */
        public fun none(): Highlight = None

        public fun html(fields: List<String>): Highlight = of(HighlightStyle.HTML, fields)
        public fun html(vararg fields: String): Highlight = html(listOf(*fields))

        public fun ansi(fields: List<String>): Highlight = of(HighlightStyle.ANSI, fields)
        public fun ansi(vararg fields: String): Highlight = ansi(listOf(*fields))

        /**
         * Escape hatch for specifying the server default highlight style (null)
         * or an arbitrary style not otherwise supported by this version of the SDK.
         */
        public fun of(
            style: HighlightStyle? = null,
            fields: List<String> = emptyList(),
        ): Highlight = Specific(style, fields)

        internal object None : Highlight() {
            override fun toString(): String = "DefaultHighlight"
        }

        internal class Specific internal constructor(
            internal val style: HighlightStyle? = null,
            internal val fields: List<String> = emptyList(),
        ) : Highlight() {
            override fun toString(): String =
                "Highlight(style=$style, fields=$fields)"
        }
    }

}
