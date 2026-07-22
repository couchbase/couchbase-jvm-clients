package com.couchbase.client.kotlin.search

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.search.queries.CoreSearchScoring

public sealed class Score(internal val core: CoreSearchScoring?) {
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

        /**
         * Combine vector and non-vector query results using reciprocal rank fusion.
         *
         * Only applicable when using [SearchSpec.mixedMode].
         */
        @SinceCouchbase("8.5")
        public fun reciprocalRankFusion(
            windowSize: Int? = null,
            rankConstant: Int? = null,
        ): Score = Fusion(CoreSearchScoring.ReciprocalRankFusion(windowSize, rankConstant))

        /**
         * Combine vector and non-vector query results using relative score fusion.
         *
         * Only applicable when using [SearchSpec.mixedMode].
         */
        @SinceCouchbase("8.5")
        public fun relativeScoreFusion(
            windowSize: Int? = null,
        ): Score = Fusion(CoreSearchScoring.RelativeScoreFusion(windowSize))
    }

    internal object Default : Score(null)
    internal object None : Score(CoreSearchScoring.Disabled.INSTANCE)
    internal class Fusion(core: CoreSearchScoring) : Score(core)
}
