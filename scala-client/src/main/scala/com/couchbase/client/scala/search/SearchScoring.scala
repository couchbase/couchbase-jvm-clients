/*
 * Copyright (c) 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.search

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.search.queries.{CoreSearchScoring => CoreScoring}

/** Specifies how the server should assign scores to search hits.
  */
sealed trait SearchScoring {
  private[scala] def toCore: CoreScoring
}

object SearchScoring {

  /** Disable scoring. */
  case object Disabled extends SearchScoring {
    private[scala] def toCore: CoreScoring = CoreScoring.Disabled.INSTANCE
  }

  /** Combine vector and non-vector query results using reciprocal rank fusion.
   * This merges the result by rank, and is the recommended approach.
   *
   * Only applicable when the [[vector.SearchRequest]] includes both a [[queries.SearchQuery]] and a
   * [[vector.VectorSearch]].
   *
   * @param rankConstant sets the rank constant used in the
   *                     [[https://cormack.uwaterloo.ca/cormacksigir09-rrf.pdf RFF algorithm]].
   * @param windowSize how many results per list are used for score fusion.
   */
  @SinceCouchbase("8.5")
  case class ReciprocalRankFusion(
                                   windowSize: Option[Int] = None,
                                   rankConstant: Option[Int] = None
                                 ) extends SearchScoring {
    private[scala] def toCore: CoreScoring =
      new CoreScoring.ReciprocalRankFusion(
        windowSize.map(Integer.valueOf).orNull,
        rankConstant.map(Integer.valueOf).orNull
      )
  }

  /** Combine vector and non-vector query results using relative score fusion.
    * This merges the score by normalized score.
    *
    * Only applicable when the [[vector.SearchRequest]] includes both a [[queries.SearchQuery]] and a
    * [[vector.VectorSearch]].
    *
    * @param windowSize how many results per list are used for score fusion.
    */
  @SinceCouchbase("8.5")
  case class RelativeScoreFusion(windowSize: Option[Int] = None) extends SearchScoring {
    private[scala] def toCore: CoreScoring =
      new CoreScoring.RelativeScoreFusion(windowSize.map(Integer.valueOf).orNull)
  }
}
