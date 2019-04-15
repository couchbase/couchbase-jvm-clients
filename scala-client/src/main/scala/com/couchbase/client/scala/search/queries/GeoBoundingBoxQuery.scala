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

import com.couchbase.client.scala.json.{JsonArray, JsonObject}

/** An FTS query which finds all matches within a given box (identified by the upper left and lower right corner
  * coordinates).
  *
  * @param topLeftLon the longitude of the top-left point of the box
  * @param topLeftLat the latitude of the top-left point of the box
  * @param bottomRightLon the longitude of the bottom-right point of the box
  * @param bottomRightLat the latitude of the bottom-right point of the box
  *
  * @since 1.0.0
  */
case class GeoBoundingBoxQuery(private[scala] val topLeftLon: Double,
                               private[scala] val topLeftLat: Double,
                               private[scala] val bottomRightLon: Double,
                               private[scala] val bottomRightLat: Double,
                               private[scala] val field: Option[String] = None,
                               private[scala] val boost: Option[Double] = None) extends AbstractFtsQuery {

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): GeoBoundingBoxQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): GeoBoundingBoxQuery = {
    copy(boost = Some(boost))
  }

  override protected def injectParams(input: JsonObject): Unit = {
    input.put("top_left", JsonArray(topLeftLon, topLeftLat))
    input.put("bottom_right", JsonArray(bottomRightLon, bottomRightLat))
    boost.foreach(v => input.put("boost", v))
    field.foreach(v => input.put("field", v))
  }
}
