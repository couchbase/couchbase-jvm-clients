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

package com.couchbase.client.scala.search.result
import com.couchbase.client.core.api.search.result._

import scala.jdk.CollectionConverters._

/**
  * Base interface for all facet results.
  *
  * @define name    the name of the [[com.couchbase.client.scala.search.facet.SearchFacet]] this result corresponds to.
  * @define field   the field the [[com.couchbase.client.scala.search.facet.SearchFacet]] was targeting.
  * @define total   the total number of valued facet results (it doesn't include missing).
  * @define missing the number of results that couldn't be faceted, missing the
  *                 adequate value. No matter how many more
  *                 buckets are added to the original facet, these
  *                 result won't ever be included in one.
  * @define other   the number of results that could have been faceted (because
  *                 they have a value for the facet's field) but
  *                 weren't, due to not having a bucket in which they belong.
  *                 Adding a bucket can result in these results being faceted.
  * @since 1.0.0
  */
sealed trait SearchFacetResult {
  protected val internal: CoreAbstractSearchFacetResult

  /** $name */
  def name: String = internal.name

  /** $field */
  def field: String = internal.field

  /** $total */
  def total: Long = internal.total

  /** $missing */
  def missing: Long = internal.missing

  /** $other */
  def other: Long = internal.other
}

object SearchFacetResult {

  /**
    * A range (or bucket) for a [[DateRangeSearchFacetResult]]. Counts the number of matches
    * that fall into the named range (which can overlap with other user-defined ranges).
    *
    * @since 1.0.0
    */
  case class DateRange(private val internal: CoreSearchDateRange) {
    def name: String = internal.name

    def start: String = internal.start.toString

    def end: String = internal.end.toString

    def count: Long = internal.count
  }

  /**
    * Represents the result for a [[com.couchbase.client.scala.search.facet.SearchFacet.DateRangeFacet]].
    *
    * @since 1.0.0
    */
  case class DateRangeSearchFacetResult (
      protected val internal: CoreDateRangeSearchFacetResult
  ) extends SearchFacetResult {

    /** The date range results. */
    def dateRanges: Seq[DateRange] = internal.dateRanges.asScala.toSeq.map(v => DateRange(v))
  }

  /**
    * Represents the result for a [[com.couchbase.client.scala.search.facet.SearchFacet.NumericRangeFacet]].
    *
    * @since 1.0.0
    */
  case class NumericRangeSearchFacetResult (
      protected val internal: CoreNumericRangeSearchFacetResult
  ) extends SearchFacetResult {

    /** The numeric range results. */
    def numericRanges: Seq[NumericRange] =
      internal.numericRanges.asScala.toSeq.map(v => NumericRange(v))
  }

  case class NumericRange(private val internal: CoreSearchNumericRange) {
    def name: String = internal.name

    def min: Double = internal.min

    def max: Double = internal.max

    def count: Long = internal.count
  }

  /**
    * Represents the result for a [[com.couchbase.client.scala.search.facet.SearchFacet.TermFacet]].
    *
    * @since 1.0.0
    */
  case class TermSearchFacetResult (protected val internal: CoreTermSearchFacetResult)
      extends SearchFacetResult {

    /** The term ranges results. */
    def terms: Seq[TermRange] = internal.terms.asScala.toSeq.map(v => TermRange(v))
  }

  /**
    * A range (or bucket) for a [[TermSearchFacetResult]].
    * Counts the number of occurrences of a given term.
    *
    * @since 1.0.0
    */
  case class TermRange(private val internal: CoreSearchTermRange) {
    def term: String = internal.name

    def count: Long = internal.count
  }
}
