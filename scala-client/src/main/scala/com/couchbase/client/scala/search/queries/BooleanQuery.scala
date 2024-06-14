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

import com.couchbase.client.core.api.search.queries.CoreBooleanQuery

/** A compound FTS query that allows various combinations of sub-queries.
  *
  * @since 1.0.0
  */
case class BooleanQuery private (
    private[scala] val must: Option[ConjunctionQuery] = None,
    private[scala] val should: Option[DisjunctionQuery] = None,
    private[scala] val mustNot: Option[DisjunctionQuery] = None,
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends SearchQuery {

  /** If a hit satisfies at least this many queries in the `should` section, its score will be boosted.  By default,
    * this is set to 1.
    *
    * @return a copy of this, for chaining
    */
  def shouldMin(minForShould: Int): BooleanQuery = {
    copy(should = Some((this.should match {
      case Some(existing) => existing
      case None           => DisjunctionQuery()
    }).min(minForShould)))
  }

  /** Results must satisfy all of these queries.
    *
    * @return a copy of this, for chaining
    */
  def must(mustQueries: SearchQuery*): BooleanQuery = {
    copy(must = Some((must match {
      case Some(existing) => existing
      case _              => ConjunctionQuery()
    }).and(mustQueries: _*)))
  }

  /** Results must not satisfy any of these queries.
    *
    * @return a copy of this, for chaining
    */
  def mustNot(mustNotQueries: SearchQuery*): BooleanQuery = {
    copy(mustNot = Some((mustNot match {
      case Some(existing) => existing
      case _              => DisjunctionQuery()
    }).or(mustNotQueries: _*)))
  }

  /** Results should satisfy all of these queries.
    *
    * @return a copy of this, for chaining
    */
  def should(shouldQueries: SearchQuery*): BooleanQuery = {
    copy(should = Some((should match {
      case Some(existing) => existing
      case _              => DisjunctionQuery()
    }).or(shouldQueries: _*)))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): BooleanQuery = {
    copy(boost = Some(boost))
  }

  override private[scala] def toCore =
    new CoreBooleanQuery(
      must.map(_.toCore).orNull,
      mustNot.map(_.toCore).orNull,
      should.map(_.toCore).orNull,
      boost.map(_.asInstanceOf[java.lang.Double]).orNull
    )
}
