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

package com.couchbase.client.scala.search.facet

import java.util.Date

import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.search.util.SearchUtils


/** Base class for all FTS facets in querying.
  *
  * @since 1.0.0
  */
object SearchFacet {
  /** Create a search facet that gives the number of occurrences of the most recurring terms in all rows.
    *
    * @param field the field to use for the facet
    * @param limit the maximum number of facets to return
    *
    * @return a constructed facet
    */
  def term(field: String, limit: Int): TermFacet = {
    new TermFacet(field, limit)
  }

  /** Create a search facet that categorizes rows into numerical ranges (or buckets) provided by the user.
    *
    * The application should supply at least one numerical range, using [[NumericRangeFacet.addRange]].
    *
    * @param field the field to use for the facet
    * @param limit the maximum number of facets to return
    *
    * @return a constructed facet
    */
  def numeric(field: String, limit: Int): NumericRangeFacet = {
    new NumericRangeFacet(field, limit, Map.empty)
  }

  /** Create a search facet that categorizes rows inside date ranges (or buckets) provided by the user.
    *
    * The application should supply at least one date range, using [[DateRangeFacet.addRange]].
    *
    * @param field the field to use for the facet
    * @param limit the maximum number of facets to return
    *
    * @return a constructed facet
    */
  def date(field: String, limit: Int): DateRangeFacet = {
    new DateRangeFacet(field, limit, Map.empty)
  }
}


/** Base class for all FTS facets in querying.
  *
  * @since 1.0.0
  */
abstract class SearchFacet protected() {
  protected val field: String
  protected val limit: Int

  def injectParams(queryJson: JsonObject): Unit = {
    queryJson.put("size", limit)
    queryJson.put("field", field)
  }
}

/** A facet that gives the number of occurrences of the most recurring terms in all rows.
  *
  * @since 1.0.0
  */
class TermFacet private[facet](override protected val field: String,
                               override protected val limit: Int) extends SearchFacet() {}


/** A facet that categorizes rows into numerical ranges (or buckets) provided by the user.
  *
  * @since 1.0.0
  */
case class NumericRangeFacet private[facet](override protected val field: String,
                                            override protected val limit: Int,
                                            private val numericRanges: Map[String, NumericRangeFacet.NumericRange])
  extends SearchFacet() {

  /** Add a new range for the facet.
    *
    * @param rangeName provide a name for the range to make it easier to find in the results
    * @param min       inclusive lower bound for the range.  It is optional but at least one of min and max must be
    *                  provided
    * @param max       exclusive upper bound for the range.  It is optional but at least one of min and max must be
    *                  provided
    *
    * @return a copy of this, for chaining
    */
  def addRange(rangeName: String, min: Option[Double], max: Option[Double]): NumericRangeFacet = {
    val newRange = numericRanges + (rangeName -> NumericRangeFacet.NumericRange(min, max))
    copy(numericRanges = newRange)
  }

  override def injectParams(queryJson: JsonObject): Unit = {
    super.injectParams(queryJson)
    val numericRange = JsonArray.create
    for ( nr <- numericRanges ) {
      val nrJson = JsonObject.create
      nrJson.put("name", nr._1)
      nr._2.min.foreach(v => nrJson.put("min", v))
      nr._2.max.foreach(v => nrJson.put("max", v))
      numericRange.add(nrJson)
    }
    queryJson.put("numeric_ranges", numericRange)
  }
}

object NumericRangeFacet {

  case class NumericRange(min: Option[Double], max: Option[Double])

}


/**
  * A facet that categorizes rows inside date ranges (or buckets) provided by the user.
  *
  * @since 1.0.0
  */
case class DateRangeFacet private[facet](override protected val field: String,
                                         override protected val limit: Int,
                                         private val dateRanges: Map[String, DateRangeFacet.DateRange])
  extends SearchFacet() {

  /** Add a new range for the facet.
    *
    * @param rangeName provide a name for the range to make it easier to find in the results
    * @param start     inclusive lower bound for the range.  Optional but at least one of start and end must be provided
    * @param end       exclusive upper bound for the range.  Optional but at least one of start and end must be provided
    *
    * @return a copy of this, for chaining
    */
  def addDateRange(rangeName: String, start: Option[Date], end: Option[Date]): DateRangeFacet = {
    addRange(rangeName, start.map(v => SearchUtils.toFtsUtcString(v)), end.map(v => SearchUtils.toFtsUtcString(v)))
  }

  /** Add a new range for the facet.
    *
    * @param rangeName provide a name for the range to make it easier to find in the results
    * @param start     inclusive lower bound for the range.  Optional but at least one of start and end must be provided
    * @param end       exclusive upper bound for the range.  Optional but at least one of start and end must be provided
    *
    * @return a copy of this, for chaining
    */
  def addRange(rangeName: String, start: Option[String], end: Option[String]): DateRangeFacet = {
    val newRange = dateRanges + (rangeName -> DateRangeFacet.DateRange(start, end))
    copy(dateRanges = newRange)
  }

  override def injectParams(queryJson: JsonObject): Unit = {
    super.injectParams(queryJson)
    val dateRange = JsonArray.create
    for ( dr <- dateRanges ) {
      val drJson = JsonObject.create
      drJson.put("name", dr._1)
      dr._2.start.foreach(v => drJson.put("start", v))
      dr._2.end.foreach(v => drJson.put("end", v))
      dateRange.add(drJson)
    }
    queryJson.put("date_ranges", dateRange)
  }
}

object DateRangeFacet {

  case class DateRange(start: Option[String], end: Option[String])

}
