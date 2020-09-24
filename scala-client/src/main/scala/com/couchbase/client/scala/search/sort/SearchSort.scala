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

import com.couchbase.client.scala.json.{JsonArray, JsonObject}

/** Base class for all FTS sort options in querying.
  *
  * @since 1.0.0
  */
sealed trait SearchSort {

  /** The identifier for the sort type, used in the "by" field.
    */
  protected def identifier: String

  protected def descending: Option[Boolean]

  private[scala] def injectParams(queryJson: JsonObject): Unit = {
    queryJson.put("by", identifier)
    descending.foreach(desc => queryJson.put("desc", desc))
  }
}

/**
  * Base class for all FTS sort options in querying.
  *
  * @since 1.0.0
  */
object SearchSort {

  /** Sort by the hit score.
    *
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is left to the server (which sorts descending by default)
    *
    * @since 1.0.0
    */
  case class ScoreSort(descending: Option[Boolean] = None) extends SearchSort {
    override protected def identifier = "score"
  }

  /** Sort by the document ID.
    *
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is left to the server (which sorts descending by default)
    *
    * @since 1.0.0
    */
  case class IdSort(descending: Option[Boolean] = None) extends SearchSort {
    override protected def identifier = "id"
  }

  /** Sort by a field in the rows.
    *
    * @param field      the name of the field to sort on
    * @param typ        the type of the field.  If left at the default `None`, it will default on the server to `Auto`
    * @param mode       the sort mode.  If left at the default `None`, it will default on the server to `Default`
    * @param missing    the missing mode.  If left at the default `None`, it will default on the server to `Last`
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is left to the server (which sorts descending by default)
    */
  case class FieldSort(
      field: String,
      typ: Option[FieldSortType] = None,
      mode: Option[FieldSortMode] = None,
      missing: Option[FieldSortMissing] = None,
      descending: Option[Boolean] = None
  ) extends SearchSort {
    override protected def identifier = "field"

    override def injectParams(queryJson: JsonObject): Unit = {
      super.injectParams(queryJson)
      queryJson.put("field", field)
      typ.foreach(v => queryJson.put("type", v.toString.toLowerCase))
      mode.foreach(v => queryJson.put("mode", v.toString.toLowerCase))
      missing.foreach(v => queryJson.put("missing", v.toString.toLowerCase))
    }
  }

  /** Sort by a geo location distance.
    *
    * @param location   should be a two-value Seq containing the longitude and latitude (in that order) to centre the
    *                   search on
    * @param field      the name of the field to sort on
    * @param unit       the unit multipler to use for sorting.  Acceptable values are: "inch", "yards", "feet",
    *                   "kilometers", "nauticalmiles", "millimeters", "centimeters", "miles" and "meters".  Standard
    *                   abbreviations for those ("ft", "km", "mi") may also be used.  If left at default `None`, it
    *                   defaults on the server side to "meters".
    * @param descending whether the search results should be sorted in descending order.  If None (the default) is
    *                   specified, it is left to the server (which sorts descending by default)
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

    override def injectParams(queryJson: JsonObject): Unit = {
      super.injectParams(queryJson)
      queryJson.put("location", JsonArray(location))
      queryJson.put("field", field)
      unit.foreach(v => queryJson.put("unit", v))
    }
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
