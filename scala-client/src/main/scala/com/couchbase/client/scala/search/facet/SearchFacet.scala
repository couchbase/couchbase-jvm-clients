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

import com.couchbase.client.core.api.search.facet._

import java.time.Instant
import scala.jdk.CollectionConverters._

/** Base class for all FTS facets in querying.
  *
  * @since 1.0.0
  */
sealed trait SearchFacet {
  protected val field: String
  protected val size: Option[Int]

  private[scala] def toCore: CoreSearchFacet
}

object SearchFacet {

  /** A search facet that gives the number of occurrences of the most recurring terms in all rows.
    *
    * @param field the field to use for the facet
    * @param size the maximum number of facets to return
    *
    * @return a constructed facet
    */
  case class TermFacet(field: String, size: Option[Int] = None) extends SearchFacet {
    override private[scala] def toCore =
      new CoreTermFacet(field, size.map(_.asInstanceOf[Integer]).orNull)
  }

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
      numericRanges: Seq[NumericRange],
      size: Option[Int] = None
  ) extends SearchFacet {
    override private[scala] def toCore =
      new CoreNumericRangeFacet(
        field,
        size.map(_.asInstanceOf[Integer]).orNull,
        numericRanges.map(_.toCore).asJava
      )
  }

  /** Defines a numeric range.
    *
    * `min` and `max` are both optional, but at least one should be provided.
    *
    * @param name the name of the range, to make it easier to find in the results
    * @param min  the lower bound (optional)
    * @param max  the upper bound (optional)
    */
  case class NumericRange(name: String, min: Option[Float], max: Option[Float]) {
    private[scala] val toCore = new CoreNumericRange(
      name,
      min.map(_.asInstanceOf[java.lang.Double]).orNull,
      max.map(_.asInstanceOf[java.lang.Double]).orNull
    )
  }

  /** A facet that categorizes rows inside date ranges (or buckets) provided by the user.
    *
    * @param field         the field to use for the facet
    * @param size          the maximum number of facets to return
    * @param dateRanges    the ranges.  At least one should be specified.
    *
    * @since 1.0.0
    */
  case class DateRangeFacet(field: String, dateRanges: Seq[DateRange], size: Option[Int] = None)
      extends SearchFacet {
    override private[scala] def toCore =
      new CoreDateRangeFacet(
        field,
        size.map(_.asInstanceOf[Integer]).orNull,
        dateRanges.map(_.toCore).asJava
      )
  }

  /** Defines a date range.
    *
    * `start` and `end` are both optional, but at least one must be provided.
    *
    * @param name   the name of the range, to make it easier to find in the results
    * @param start  the start of the range (optional)
    * @param end    the start of the range (optional)
    */
  case class DateRange(name: String, start: Option[String], end: Option[String]) {
    private[scala] def toCore = new CoreDateRange(name, start.orNull, end.orNull)
  }

  object DateRange {
    def create(name: String, start: Option[Instant], end: Option[Instant]): DateRange = {
      DateRange(name, start.map(_.toString), end.map(_.toString))
    }
  }
}
