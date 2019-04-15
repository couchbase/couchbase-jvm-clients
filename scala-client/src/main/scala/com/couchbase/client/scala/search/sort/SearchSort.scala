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

/**
  * Base class for all FTS sort options in querying.
  *
  * @since 1.0.0
  */
object SearchSort {

  /** Sort by the document identifier.
    */
  def byId: SearchSortId = new SearchSortId

  /** Sort by the hit score.
    */
  def byScore: SearchSortScore = new SearchSortScore

  /** Sort by a field in the rows.
    *
    * @param field the field name.
    */
  def byField(field: String): SearchSortField = SearchSortField(field)

  /** Sort by geo location.
    *
    * @param locationLon longitude of the location.
    * @param locationLat latitude of the location.
    * @param field       the field name.
    */
  def byGeoDistance(locationLon: Double, locationLat: Double, field: String): SearchSortGeoDistance = {
    SearchSortGeoDistance(locationLon, locationLat, field)
  }
}

/** Base class for all FTS sort options in querying.
  *
  * @since 1.0.0
  */
sealed abstract class SearchSort protected() {

  /** The identifier for the sort type, used in the "by" field.
    */
  protected def identifier: String

  protected def descending: Boolean

  private[scala] def injectParams(queryJson: JsonObject): Unit = {
    queryJson.put("by", identifier)
    if (descending) queryJson.put("desc", true)
  }
}

/** Sort by the hit score.
  *
  * @since 1.0.0
  */
case class SearchSortScore private[scala](protected val descending: Boolean = false) extends SearchSort {
  override protected def identifier = "score"

  def descending(descending: Boolean): SearchSortScore = {
    copy(descending = descending)
  }
}

/**
  * Sort by the document ID.
  *
  * @since 1.0.0
  */
case class SearchSortId private[scala](protected val descending: Boolean = false) extends SearchSort {
  override protected def identifier = "id"

  def descending(descending: Boolean): SearchSortId = {
    copy(descending = descending)
  }
}

/**
  * Sort by a location and unit in the rows.
  *
  * @since 1.0.0
  */
case class SearchSortGeoDistance private[scala](private[scala] val locationLon: Double,
                                                private[scala] val locationLat: Double,
                                                private[scala] val field: String,
                                                protected val descending: Boolean = false,
                                                private[scala] val unit: Option[String] = None) extends SearchSort {

  override protected def identifier = "geo_distance"

  def descending(descending: Boolean): SearchSortGeoDistance = {
    copy(descending = descending)
  }

  def unit(unit: String): SearchSortGeoDistance = {
    copy(unit = Some(unit))
  }

  override def injectParams(queryJson: JsonObject): Unit = {
    super.injectParams(queryJson)
    queryJson.put("location", JsonArray(locationLon, locationLat))
    queryJson.put("field", field)
    unit.foreach(v => queryJson.put("unit", v))
  }
}


/**
  * Sort by a field in the rows.
  *
  * @since 1.0.0
  */
case class SearchSortField private[scala](private[scala] val field: String,
                                          protected val descending: Boolean = false,
                                          private[scala] val typ: Option[FieldType] = None,
                                          private[scala] val mode: Option[FieldMode] = None,
                                          private[scala] val missing: Option[FieldMissing] = None) extends SearchSort {

  def descending(descending: Boolean): SearchSortField = {
    copy(descending = descending)
  }

  def typ(typ: FieldType): SearchSortField = {
    copy(typ = Some(typ))
  }

  def mode(mode: FieldMode): SearchSortField = {
    copy(mode = Some(mode))
  }

  def missing(missing: FieldMissing): SearchSortField = {
    copy(missing = Some(missing))
  }

  override protected def identifier = "field"

  override def injectParams(queryJson: JsonObject): Unit = {
    super.injectParams(queryJson)
    queryJson.put("field", field)
    typ.foreach(v => queryJson.put("type", v.field))
    mode.foreach(v => queryJson.put("mode", v.field))
    missing.foreach(v => queryJson.put("missing", v.field))
  }
}

sealed abstract class FieldType(val field: String)

object FieldType {

  case object Auto extends FieldType("auto")

  case object String extends FieldType("string")

  case object Number extends FieldType("number")

  case object Date extends FieldType("date")

}

sealed abstract class FieldMode(val field: String)

object FieldMode {

  case object Default extends FieldMode("default")

  case object Min extends FieldMode("min")

  case object Max extends FieldMode("max")

}

sealed abstract class FieldMissing(val field: String)

object FieldMissing {

  case object First extends FieldMissing("first")

  case object Last extends FieldMissing("last")

}