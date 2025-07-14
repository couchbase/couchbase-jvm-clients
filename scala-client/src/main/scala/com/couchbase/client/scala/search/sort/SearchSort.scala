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
package com.couchbase.client.scala.search.sort

import com.couchbase.client.core.api.search.queries.CoreGeoCoordinates
import com.couchbase.client.core.api.search.sort._

/** Base class for all FTS sort options in querying.
  *
  * @since 1.0.0
  */
sealed trait SearchSort {

  /** The identifier for the sort type, used in the "by" field.
    */
  protected def identifier: String

  protected def descending: Option[Boolean]

  private[scala] def toCore: CoreSearchSort
}

/** Base class for all FTS sort options in querying.
  *
  * @since 1.0.0
  */
object SearchSort {

  /** Sort by the hit score.
    *
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is not sent and left to the server default.
    *
    * @since 1.0.0
    */
  case class ScoreSort(descending: Option[Boolean] = None) extends SearchSort {
    override protected def identifier = "score"

    override private[scala] def toCore =
      new CoreSearchSortScore(descending.map(_.asInstanceOf[java.lang.Boolean]).orNull)
  }

  /** Sort by the document ID.
    *
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is not sent and left to the server default.
    *
    * @since 1.0.0
    */
  case class IdSort(descending: Option[Boolean] = None) extends SearchSort {
    override protected def identifier = "id"

    override private[scala] def toCore =
      new CoreSearchSortId(descending.map(_.asInstanceOf[java.lang.Boolean]).orNull)
  }

  /** Sort by a field in the rows.
    *
    * @param field      the name of the field to sort on
    * @param typ        the type of the field.  If left at the default `None`, it will default on the server to `Auto`
    * @param mode       the sort mode.  If left at the default `None`, it will default on the server to `Default`
    * @param missing    the missing mode.  If left at the default `None`, it will default on the server to `Last`
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is not sent and left to the server default.
    */
  case class FieldSort(
      field: String,
      typ: Option[FieldSortType] = None,
      mode: Option[FieldSortMode] = None,
      missing: Option[FieldSortMissing] = None,
      descending: Option[Boolean] = None
  ) extends SearchSort {
    override protected def identifier = "field"

    override private[scala] def toCore =
      new CoreSearchSortField(
        field,
        typ.map {
          case FieldSortType.Auto   => CoreSearchFieldType.AUTO
          case FieldSortType.String => CoreSearchFieldType.STRING
          case FieldSortType.Number => CoreSearchFieldType.NUMBER
          case FieldSortType.Date   => CoreSearchFieldType.DATE
        }.orNull,
        mode.map {
          case FieldSortMode.Default => CoreSearchFieldMode.DEFAULT
          case FieldSortMode.Min     => CoreSearchFieldMode.MIN
          case FieldSortMode.Max     => CoreSearchFieldMode.MAX
        }.orNull,
        missing.map {
          case FieldSortMissing.Last  => CoreSearchFieldMissing.LAST
          case FieldSortMissing.First => CoreSearchFieldMissing.FIRST
        }.orNull,
        descending.map(_.asInstanceOf[java.lang.Boolean]).orNull
      )
  }

  /** Sort by a geo location distance.
    *
    * @param location   should be a two-value Seq containing the longitude and latitude (in that order) to centre the
    *                   search on
    * @param field      the name of the field to sort on
    * @param unit       the unit multipler to use for sorting.  Acceptable values are: "inch", "yards", "feet",
    *                   "kilometers", "nauticalmiles", "millimeters", "centimeters", "miles" and "meters".  Standard
    *                   abbreviations for those ("ft", "km", "mi") may also be used.  If left at default `None`, it
    *                   is not sent and left to the server default.
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is not sent and left to the server default.
    *
    * @since 1.0.0
    */
  case class GeoDistanceSort(
      location: Seq[Float],
      field: String,
      descending: Option[Boolean] = None,
      unit: Option[String] = None
  ) extends SearchSort {

    override protected def identifier = "geo_distance"

    def unit(unit: String): GeoDistanceSort = {
      copy(unit = Some(unit))
    }

    override private[scala] def toCore =
      new CoreSearchSortGeoDistance(
        CoreGeoCoordinates.lon(location.head).lat(location(1)),
        field,
        unit
          .map(v =>
            v.toLowerCase match {
              case "inch" | "in" | "inches"             => CoreSearchGeoDistanceUnits.INCH
              case "yards"                              => CoreSearchGeoDistanceUnits.YARDS
              case "feet" | "ft"                        => CoreSearchGeoDistanceUnits.FEET
              case "kilometers" | "km" | "kilometres"   => CoreSearchGeoDistanceUnits.KILOMETERS
              case "nauticalmiles"                      => CoreSearchGeoDistanceUnits.NAUTICAL_MILES
              case "millimeters" | "mm" | "millimetres" => CoreSearchGeoDistanceUnits.MILLIMETERS
              case "centimeters" | "cm" | "centimetres" => CoreSearchGeoDistanceUnits.CENTIMETERS
              case "miles" | "mi"                       => CoreSearchGeoDistanceUnits.MILES
              case "meters" | "m" | "metres"            => CoreSearchGeoDistanceUnits.METERS
            }
          )
          .orNull,
        descending.map(_.asInstanceOf[java.lang.Boolean]).orNull
      )
  }

}

/** The type of the field used for a [[com.couchbase.client.scala.search.sort.SearchSort.FieldSort]]. */
sealed trait FieldSortType

object FieldSortType {
  case object Auto   extends FieldSortType
  case object String extends FieldSortType
  case object Number extends FieldSortType
  case object Date   extends FieldSortType
}

/** The mode of a [[com.couchbase.client.scala.search.sort.SearchSort.FieldSort]]. */
sealed trait FieldSortMode

object FieldSortMode {
  case object Default extends FieldSortMode
  case object Min     extends FieldSortMode
  case object Max     extends FieldSortMode
}

/** The missing behaviour for a [[com.couchbase.client.scala.search.sort.SearchSort.FieldSort]]. */
sealed trait FieldSortMissing

object FieldSortMissing {
  case object First extends FieldSortMissing
  case object Last  extends FieldSortMissing
}
