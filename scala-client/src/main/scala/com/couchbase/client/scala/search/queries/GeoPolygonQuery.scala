/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.{SinceCouchbase, Stability}
import com.couchbase.client.core.api.search.queries.CoreGeoPolygonQuery
import com.couchbase.client.scala.util.Coordinate

import scala.jdk.CollectionConverters._

/** An FTS query that finds all matches inside a given search polygon.
  */
@SinceCouchbase("6.5.1")
case class GeoPolygonQuery(
    private[scala] val coordinates: Seq[Coordinate],
    private[scala] val field: Option[String] = None,
    private[scala] val boost: Option[Double] = None
) extends SearchQuery {

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): GeoPolygonQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): GeoPolygonQuery = {
    copy(boost = Some(boost))
  }

  override private[scala] def toCore =
    new CoreGeoPolygonQuery(
      coordinates.map(_.toCore).asJava,
      field.orNull,
      boost.map(_.asInstanceOf[java.lang.Double]).orNull
    )
}
