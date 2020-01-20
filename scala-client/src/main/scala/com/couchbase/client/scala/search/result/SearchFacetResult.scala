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
import com.couchbase.client.scala.util.CouchbasePickler

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

  /** $name */
  def name: String

  /** $field */
  def field: String

  /** $total */
  def total: Long

  /** $missing */
  def missing: Long

  /** $other */
  def other: Long
}

object SearchFacetResult {

  /**
    * A range (or bucket) for a [[DateRangeSearchFacetResult]]. Counts the number of matches
    * that fall into the named range (which can overlap with other user-defined ranges).
    *
    * @since 1.0.0
    */
  case class DateRange(name: String, start: String, end: String, count: Long)

  private[scala] object DateRange {
    implicit val rw: CouchbasePickler.ReadWriter[DateRange] = CouchbasePickler.macroRW
  }

  /**
    * Represents the result for a [[com.couchbase.client.scala.search.facet.SearchFacet.DateRangeFacet]].
    *
    * @param name    the name of the [[com.couchbase.client.scala.search.facet.SearchFacet]] this result corresponds to.
    * @param field   the field the [[com.couchbase.client.scala.search.facet.SearchFacet]] was targeting.
    * @param total   the total number of valued facet results (it doesn't include missing).
    * @param missing the number of results that couldn't be faceted, missing the
    *                 adequate value. No matter how many more
    *                 buckets are added to the original facet, these
    *                 result won't ever be included in one.
    * @param other   the number of results that could have been faceted (because
    *                 they have a value for the facet's field) but
    *                 weren't, due to not having a bucket in which they belong.
    *                 Adding a bucket can result in these results being faceted.
    *
    * @since 1.0.0
    */
  case class DateRangeSearchFacetResult(
      name: String,
      field: String,
      total: Long,
      missing: Long,
      other: Long,
      dateRanges: Seq[DateRange]
  ) extends SearchFacetResult

  private[scala] object DateRangeSearchFacetResult {
    implicit val rw: CouchbasePickler.ReadWriter[DateRangeSearchFacetResult] =
      CouchbasePickler.macroRW
  }

  /**
    * Represents the result for a [[com.couchbase.client.scala.search.facet.SearchFacet.NumericRangeFacet]].
    *
    * @param name    the name of the [[com.couchbase.client.scala.search.facet.SearchFacet]] this result corresponds to.
    * @param field   the field the [[com.couchbase.client.scala.search.facet.SearchFacet]] was targeting.
    * @param total   the total number of valued facet results (it doesn't include missing).
    * @param missing the number of results that couldn't be faceted, missing the
    *                 adequate value. No matter how many more
    *                 buckets are added to the original facet, these
    *                 result won't ever be included in one.
    * @param other   the number of results that could have been faceted (because
    *                 they have a value for the facet's field) but
    *                 weren't, due to not having a bucket in which they belong.
    *                 Adding a bucket can result in these results being faceted.
    *
    * @since 1.0.0
    */
  case class NumericRangeSearchFacetResult(
      name: String,
      field: String,
      total: Long,
      missing: Long,
      other: Long,
      numericRanges: Seq[NumericRange]
  ) extends SearchFacetResult

  private[scala] object NumericRangeSearchFacetResult {
    implicit val rw: CouchbasePickler.ReadWriter[NumericRangeSearchFacetResult] =
      CouchbasePickler.macroRW
  }

  case class NumericRange(name: String, min: Double, max: Double, count: Long)

  private[scala] object NumericRange {
    implicit val rw: CouchbasePickler.ReadWriter[NumericRange] = CouchbasePickler.macroRW
  }

  /**
    * Represents the result for a [[com.couchbase.client.scala.search.facet.SearchFacet.TermFacet]].
    *
    * @param name    the name of the [[com.couchbase.client.scala.search.facet.SearchFacet]] this result corresponds to.
    * @param field   the field the [[com.couchbase.client.scala.search.facet.SearchFacet]] was targeting.
    * @param total   the total number of valued facet results (it doesn't include missing).
    * @param missing the number of results that couldn't be faceted, missing the
    *                 adequate value. No matter how many more
    *                 buckets are added to the original facet, these
    *                 result won't ever be included in one.
    * @param other   the number of results that could have been faceted (because
    *                 they have a value for the facet's field) but
    *                 weren't, due to not having a bucket in which they belong.
    *                 Adding a bucket can result in these results being faceted.
    *
    * @since 1.0.0
    */
  case class TermSearchFacetResult(
      name: String,
      field: String,
      total: Long,
      missing: Long,
      other: Long,
      terms: Seq[TermRange]
  ) extends SearchFacetResult

  private[scala] object TermSearchFacetResult {
    implicit val rw: CouchbasePickler.ReadWriter[TermSearchFacetResult] = CouchbasePickler.macroRW
  }

  /**
    * A range (or bucket) for a [[TermSearchFacetResult]].
    * Counts the number of occurrences of a given term.
    *
    * @since 1.0.0
    */
  case class TermRange(term: String, count: Long)

  private[scala] object TermRange {
    implicit val rw: CouchbasePickler.ReadWriter[TermRange] = CouchbasePickler.macroRW
  }
}
