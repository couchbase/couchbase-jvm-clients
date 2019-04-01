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

package com.couchbase.client.scala.view

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.json.{JsonArray, JsonArraySafe, JsonObject}
import com.couchbase.client.scala.query.QueryOptions

import scala.concurrent.duration.Duration



/** Customize the execution of a spatial view.
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class SpatialViewOptions(
                               private[scala] val development: Option[Boolean] = None,
                               private[scala] val limit: Option[Int] = None,
                               private[scala] val skip: Option[Int] = None,
                               private[scala] val stale: Option[Stale] = None,
                               private[scala] val onError: Option[OnError] = None,
                               private[scala] val debug: Option[Boolean] = None,
                               private[scala] val startRange: Option[String] = None,
                               private[scala] val endRange: Option[String] = None,
                               private[scala] val retryStrategy: Option[RetryStrategy] = None,
                               private[scala] val timeout: Option[Duration] = None
                             ) {

  def development(value: Boolean): SpatialViewOptions = {
    copy(development = Some(value))
  }

  /**
    * Limit the number of the returned documents to the specified number.
    *
    * @return this for further chaining
    */
  def limit(value: Int): SpatialViewOptions = {
    copy(limit = Some(value))
  }

  /** Skip this number of records before starting to return the results.
    *
    * @return this for further chaining
    */
  def skip(value: Int): SpatialViewOptions = {
    copy(skip = Some(value))
  }

  /** Allow the results from a stale view to be used.
    *
    * The default is Stale.UpdateAfter.
    *
    * @return this for further chaining
    */
  def stale(value: Stale): SpatialViewOptions = {
    copy(stale = Some(value))
  }

  /** Sets the response in the event of an error.
    *
    * @return this for further chaining
    */
  def onError(value: OnError): SpatialViewOptions = {
    copy(onError = Some(value))
  }

  /** Enable debugging on view queries.
    *
    * @return this for further chaining
    */
  def debug(value: Boolean): SpatialViewOptions = {
    copy(debug = Some(value))
  }

  def startRange(value: Any): SpatialViewOptions = {
    value match {
      case v: String => copy(startRange = Some(v))
      case JsonArray | JsonArraySafe => copy(startRange = Some(value.toString))
      case _ =>
        // This will trigger a validation failure later
        copy(startRange = null)
    }
  }

  def endRange(value: Any): SpatialViewOptions = {
    value match {
      case v: String => copy(endRange = Some(v))
      case JsonArray | JsonArraySafe => copy(endRange = Some(value.toString))
      case _ =>
        // This will trigger a validation failure later
        copy(endRange = null)
    }
  }

  def range(startRange: Any, endRange: Any): SpatialViewOptions = {
    val next: SpatialViewOptions = this.startRange(startRange)
    next.endRange(endRange)
  }

  def timeout(timeout: Duration): SpatialViewOptions = {
    copy(timeout = Option(timeout))
  }

  /** Sets what retry strategy to use if the operation fails.  See [[RetryStrategy]] for details.
    *
    * @param strategy the retry strategy to use
    *
    * @return a copy of this with the change applied, for chaining.
    */
  def retryStrategy(strategy: RetryStrategy): SpatialViewOptions = {
    copy(retryStrategy = Some(strategy))
  }


  private[scala] def durationToN1qlFormat(duration: Duration) = {
    if (duration.toSeconds > 0) duration.toSeconds + "s"
    else duration.toNanos + "ns"
  }

  private[scala] def encode() = {
    val sb = new StringBuilder()

    limit.foreach(v => {
      sb.append("limit=")
      sb.append(v.toString)
      sb.append('&')
    })

    skip.foreach(v => {
      sb.append("skip=")
      sb.append(v.toString)
      sb.append('&')
    })

    stale.foreach(v => {
      sb.append("stale=")
      sb.append(v.encode)
      sb.append('&')
    })

    onError.foreach(v => {
      sb.append("on_error=")
      sb.append(v.encode)
      sb.append('&')
    })

    debug.foreach(v => {
      sb.append("debug=")
      sb.append(v.toString)
      sb.append('&')
    })

    startRange.foreach(v => {
      sb.append("start_range=")
      sb.append(v.toString)
      sb.append('&')
    })

    endRange.foreach(v => {
      sb.append("end_range=")
      sb.append(v.toString)
      sb.append('&')
    })

    sb.toString.stripSuffix("&")
  }
}




