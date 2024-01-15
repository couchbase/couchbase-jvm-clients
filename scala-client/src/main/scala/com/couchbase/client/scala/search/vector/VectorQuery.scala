/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.client.scala.search.vector

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.api.search.vector.CoreVectorQuery

/** Represents a vector query. */
@Volatile
case class VectorQuery private (
    private val vectorQuery: Array[Float],
    private val vectorField: String,
    private val numCandidates: Option[Int] = None,
    private val boost: Option[Double] = None
) {

  /** Can be used to control how much weight to give the results of this query vs other queries.
    *
    * See the [[https://docs.couchbase.com/server/current/fts/fts-query-string-syntax-boosting.html FTS documentation]] for details.
    *
    * @return a copy of this, for chaining.
    */
  def boost(boost: Double): VectorQuery =
    copy(boost = Some(boost))

  /** This is the number of results that will be returned from this vector query.
    *
    * @return a copy of this, for chaining.
    */
  def numCandidates(numCandidates: Int): VectorQuery =
    copy(numCandidates = Some(numCandidates))

  private[scala] def toCore: CoreVectorQuery =
    new CoreVectorQuery(
      vectorQuery,
      vectorField,
      numCandidates.map(Integer.valueOf).orNull,
      boost.map(java.lang.Double.valueOf).orNull
    )
}

@Volatile
object VectorQuery {
  def apply(vectorField: String, vectorQuery: Array[Float]): VectorQuery =
    new VectorQuery(vectorQuery, vectorField)
}
