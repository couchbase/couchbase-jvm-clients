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

import java.util.Date

import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.search.util.SearchUtils

/** An FTS query that matches documents on a range of dates. At least one bound is required, and the parser
  * to use for the date (in [[String]] form) can be customized (see [[#dateTimeParser(String)]]).
  *
  * @since 1.0.0
  */
case class DateRangeQuery(private[scala] val start: Option[String] = None,
                          private[scala] val inclusiveStart: Option[Boolean] = None,
                          private[scala] val end: Option[String] = None,
                          private[scala] val inclusiveEnd: Option[Boolean] = None,
                          private[scala] val dateTimeParser: Option[String] = None,
                          private[scala] val field: Option[String] = None,
                          private[scala] val boost: Option[Double] = None) extends AbstractFtsQuery {

  /** Sets the lower boundary of the range, inclusive or not depending on the second parameter.
    */
  def start(start: String, inclusive: Boolean): DateRangeQuery = {
    copy(start = Some(start), inclusiveStart = Some(inclusive))
  }

  /** Sets the lower boundary of the range.
    * The lower boundary is considered inclusive by default on the server side.
    */
  def start(start: String): DateRangeQuery = {
    copy(start = Some(start), inclusiveStart = None)
  }

  /** Sets the upper boundary of the range, inclusive or not depending on the second parameter.
    */
  def end(end: String, inclusive: Boolean): DateRangeQuery = {
    copy(end = Some(end), inclusiveEnd = Some(inclusive))
  }

  /** Sets the upper boundary of the range.
    * The upper boundary is considered exclusive by default on the server side.
    */
  def end(end: String): DateRangeQuery = {
    copy(end = Some(end), inclusiveEnd = None)
  }

  /** Sets the lower boundary of the range, inclusive or not depending on the second parameter.
    *
    * Works with a [[Date]] object, which is converted to RFC 3339 format using
    * [[SearchUtils#toFtsUtcString(Date)]], so you shouldn't use a non-default [[#dateTimeParser(String)]]
    * after that.
    */
  def start(start: Date, inclusive: Boolean): DateRangeQuery = {
    copy(start = Some(SearchUtils.toFtsUtcString(start)), inclusiveStart = Some(inclusive))
  }

  /** Sets the lower boundary of the range.
    * The lower boundary is considered inclusive by default on the server side.
    *
    * Works with a [[Date]] object, which is converted to RFC 3339 format using
    * [[SearchUtils#toFtsUtcString(Date)]], so you shouldn't use a non-default [[#dateTimeParser(String)]]
    * after that.
    */
  def start(start: Date): DateRangeQuery = {
    copy(start = Some(SearchUtils.toFtsUtcString(start)), inclusiveStart = None)
  }

  /** Sets the upper boundary of the range, inclusive or not depending on the second parameter.
    *
    * Works with a [[Date]] object, which is converted to RFC 3339 format using
    * [[SearchUtils#toFtsUtcString(Date)]], so you shouldn't use a non-default [[#dateTimeParser(String)]]
    * after that.
    */
  def end(end: Date, inclusive: Boolean): DateRangeQuery = {
    copy(end = Some(SearchUtils.toFtsUtcString(end)), inclusiveEnd = Some(inclusive))
  }

  /** Sets the upper boundary of the range.
    * The upper boundary is considered exclusive by default on the server side.
    *
    * Works with a [[Date]] object, which is converted to RFC 3339 format using
    * [[SearchUtils#toFtsUtcString(Date)]], so you shouldn't use a non-default [[#dateTimeParser(String)]]
    * after that.
    */
  def end(end: Date): DateRangeQuery = {
    copy(end = Some(SearchUtils.toFtsUtcString(end)), inclusiveEnd = None)
  }

  /** The name of the date/time parser to use to interpret `start` and `end`. Should not
    * be modified when passing in [[Date]].
    */
  def dateTimeParser(dateTimeParser: String): DateRangeQuery = {
    copy(dateTimeParser = Some(dateTimeParser))
  }

  /** If specified, only this field will be matched.
    *
    * @return a copy of this, for chaining
    */
  def field(field: String): DateRangeQuery = {
    copy(field = Some(field))
  }

  /** The boost parameter is used to increase the relative weight of a clause (with a boost greater than 1) or decrease
    * the relative weight (with a boost between 0 and 1)
    *
    * @param boost the boost parameter, which must be >= 0
    *
    * @return a copy of this, for chaining
    */
  def boost(boost: Double): DateRangeQuery = {
    copy(boost = Some(boost))
  }

  override protected def injectParams(input: JsonObject): Unit = {
    start.foreach(s => {
      input.put("start", s)
      inclusiveStart.foreach(is => input.put("inclusive_start", is))
    })
    end.foreach(e => {
      input.put("end", e)
      inclusiveEnd.foreach(ie => input.put("inclusive_end", ie))
    })
    dateTimeParser.foreach(v => input.put("datetime_parser", v))
    boost.foreach(v => input.put("boost", v))
    field.foreach(v => input.put("field", v))
  }
}
