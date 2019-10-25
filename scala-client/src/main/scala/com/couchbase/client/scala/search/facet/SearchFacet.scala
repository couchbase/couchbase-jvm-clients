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
sealed trait SearchFacet {
  protected val field: String
  protected val size: Option[Int]

  def injectParams(queryJson: JsonObject): Unit = {
    size.foreach(v => queryJson.put("size", v))
    queryJson.put("field", field)
  }
}

object SearchFacet {

  /** A search facet that gives the number of occurrences of the most recurring terms in all rows.
    *
    * @param field the field to use for the facet
    * @param size the maximum number of facets to return
    *
    * @return a constructed facet
    */
  case class TermFacet(field: String, size: Option[Int] = None) extends SearchFacet

  /** A search facet that categorizes rows into numerical ranges (or buckets) provided by the user.
    *
    * @param field         the field to use for the facet
    * @param size          the maximum number of facets to return
    * @param numericRanges the ranges.  At least one should be specified.
    *
    * @return a constructed facet
    */
  case class NumericRangeFacet(
      field: String,
      size: Option[Int] = None,
      numericRanges: Seq[NumericRange]
  ) extends SearchFacet {

    override def injectParams(queryJson: JsonObject): Unit = {
      super.injectParams(queryJson)
      val numericRange = JsonArray.create
      numericRanges.foreach(nr => {
        val nrJson = JsonObject.create
        nrJson.put("name", nr.name)
        nr.min.foreach(v => nrJson.put("min", v))
        nr.max.foreach(v => nrJson.put("max", v))
        numericRange.add(nrJson)
      })
      queryJson.put("numeric_ranges", numericRange)
    }
  }

  /** Defines a numeric range.
    *
    * `min` and `max` are both optional, but at least one should be provided.
    *
    * @param name the name of the range, to make it easier to find in the results
    * @param min  the lower bound (optional)
    * @param max  the upper bound (optional)
    */
  case class NumericRange(name: String, min: Option[Float], max: Option[Float])

  /** A facet that categorizes rows inside date ranges (or buckets) provided by the user.
    *
    * @param field         the field to use for the facet
    * @param size          the maximum number of facets to return
    * @param dateRanges    the ranges.  At least one should be specified.
    *
    * @since 1.0.0
    */
  case class DateRangeFacet(field: String, size: Option[Int] = None, dateRanges: Seq[DateRange])
      extends SearchFacet {

    override def injectParams(queryJson: JsonObject): Unit = {
      super.injectParams(queryJson)
      val dateRange = JsonArray.create
      dateRanges.foreach(dr => {
        val drJson = JsonObject.create
        drJson.put("name", dr.name)
        dr.start.foreach(v => drJson.put("start", v))
        dr.end.foreach(v => drJson.put("end", v))
        dateRange.add(drJson)
      })
      queryJson.put("date_ranges", dateRange)
    }
  }

  /** Defines a date range.
    *
    * `start` and `end` are both optional, but at least one should be provided.
    *
    * @param name   the name of the range, to make it easier to find in the results
    * @param start  the start of the range (optional)
    * @param end    the start of the range (optional)
    */
  case class DateRange(name: String, start: Option[String], end: Option[String])
}
