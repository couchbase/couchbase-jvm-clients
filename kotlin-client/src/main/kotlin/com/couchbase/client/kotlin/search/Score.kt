package com.couchbase.client.kotlin.search

import com.couchbase.client.core.annotation.SinceCouchbase

public sealed class Score {
    internal abstract fun inject(params: MutableMap<String, Any?>)

    public companion object {
        /**
         * Calculate a score for each hit.
         */
        public fun default(): Score = Default

        /**
         * Assign all hits a score of zero, for improved performance
         * when scores are not required.
         */
        @SinceCouchbase("6.6.1")
        public fun none(): Score = None
    }

    internal object Default : Score() {
        override fun inject(params: MutableMap<String, Any?>) {
            // no-op
        }
    }

    internal object None : Score() {
        override fun inject(params: MutableMap<String, Any?>) {
            params["score"] = "none"
        }
    }
}
