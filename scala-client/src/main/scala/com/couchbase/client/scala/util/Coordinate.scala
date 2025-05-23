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
package com.couchbase.client.scala.util

import com.couchbase.client.core.api.search.queries.CoreGeoCoordinates

/** A coordinate is a tuple of a latitude and a longitude. */
object Coordinate {

  /**
    * Creates a new [[Coordinate]] with a longitude and a latitude.
    *
    * @param lon the longitude of the coordinate.
    * @param lat the latitude of the coordinate.
    * @return a new Coordinate.
    */
  def ofLonLat(lon: Double, lat: Double) = new Coordinate(lon, lat)
}

case class Coordinate (
    /**
      * The longitude of this coordinate.
      */
    lon: Double,
    /**
      * The latitude of this coordinate.
      */
    lat: Double
) {
  private[scala] def toCore = CoreGeoCoordinates.lon(lon).lat(lat)
}
