/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.client.scala.search.queries

import com.couchbase.client.core.api.search.queries.CoreDisjunctionQuery

import scala.jdk.CollectionConverters._

/** A compound FTS query that performs a logical OR between all its sub-queries (disjunction).
  * It requires that a configurable minimum of the queries match (the default is 1).
  *
  * @since 1.0.0
  */
case class DisjunctionQuery(
    private[scala] val queries: Seq[SearchQuery] = Seq.empty,
    private[scala] val min: Option[Int] = None,
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends AbstractCompoundQuery {

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): DisjunctionQuery = {
    copy(boost = Some(boost))
  }

  /** The minimum number of sub-queries that must match.  The default is 1.
    */
  def min(min: Int): DisjunctionQuery = {
    copy(min = Some(min))
  }

  /** Adds more sub-queries to the disjunction.
    *
    * @return a copy of this, for chaining
    */
  def or(queries: SearchQuery*): DisjunctionQuery = {
    copy(queries = this.queries ++ queries.toSeq)
  }

  override private[scala] def toCore =
    new CoreDisjunctionQuery(
      queries.map(_.toCore).asJava,
      min.map(_.asInstanceOf[Integer]).orNull,
      boost.map(_.asInstanceOf[java.lang.Double]).orNull
    )
}
