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

import com.couchbase.client.core.api.search.queries.CoreNumericRangeQuery

/** An FTS query that matches documents on a range of values. At least one bound is required, and the
  * inclusiveness of each bound can be configured.
  *
  * @since 1.0.0
  */
case class NumericRangeQuery(
    private[scala] val min: Option[Double] = None,
    private[scala] val inclusiveMin: Option[Boolean] = None,
    private[scala] val max: Option[Double] = None,
    private[scala] val inclusiveMax: Option[Boolean] = None,
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends SearchQuery {

  /** Sets the lower boundary of the range, inclusive or not depending on the second parameter.
    */
  def min(min: Double, inclusive: Boolean): NumericRangeQuery = {
    copy(min = Some(min), inclusiveMin = Some(inclusive))
  }

  /** Sets the lower boundary of the range.
    * The lower boundary is considered inclusive by default on the server side.
    */
  def min(min: Double): NumericRangeQuery = {
    copy(min = Some(min), inclusiveMin = None)
  }

  /** Sets the upper boundary of the range, inclusive or not depending on the second parameter.
    */
  def max(max: Double, inclusive: Boolean): NumericRangeQuery = {
    copy(max = Some(max), inclusiveMax = Some(inclusive))
  }

  /** Sets the upper boundary of the range.
    * The upper boundary is considered exclusive by default on the server side.
    */
  def max(max: Double): NumericRangeQuery = {
    copy(max = Some(max), inclusiveMax = None)
  }

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): NumericRangeQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): NumericRangeQuery = {
    copy(boost = Some(boost))
  }

  override private[scala] def toCore =
    new CoreNumericRangeQuery(
      min.map(_.asInstanceOf[java.lang.Double]).orNull,
      max.map(_.asInstanceOf[java.lang.Double]).orNull,
      inclusiveMin.map(_.asInstanceOf[java.lang.Boolean]).orNull,
      inclusiveMax.map(_.asInstanceOf[java.lang.Boolean]).orNull,
      field.orNull,
      boost.map(_.asInstanceOf[java.lang.Double]).orNull
    )
}
