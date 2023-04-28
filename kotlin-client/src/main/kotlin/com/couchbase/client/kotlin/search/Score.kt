package com.couchbase.client.kotlin.search

import com.couchbase.client.core.annotation.SinceCouchbase

public sealed class Score {
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

    internal object Default : Score()
    internal object None : Score()
}
