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

package com.couchbase.client.scala.analytics

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.analytics.{AnalyticsChunkRow, AnalyticsResponse}
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.util.FunctionalUtil
import reactor.core.scala.publisher.{Flux, Mono}

import scala.util.{Failure, Success, Try}

/** The results of an Analytics query.
  *
  * @param rows            all rows returned from the analytics service
  * @param clientContextId if a clientContextId was provided in [[AnalyticsOptions]] it will be returned here.  This
  *                        allows the application to tie requests and responses together.
  * @param signature       details of the signature of the analytics query, if any were present
  * @param metrics         metrics related to the analytics request, if they are available
  * @param warnings        any warnings returned from the analytics service
  * @param status          the raw status string returned from the analytics service
  * @param profile         if a profile was requested in [[AnalyticsOptions]] it will be returned here
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class AnalyticsResult(private[scala] val rows: Seq[AnalyticsChunkRow],
                           private[scala] val requestId: String,
                           clientContextId: Option[String],
                           signature: Option[AnalyticsSignature],
                           metrics: Option[AnalyticsMetrics],
                           warnings: Option[AnalyticsWarnings],
                           status: String,
                           profile: Option[AnalyticsProfile]) {
  /** Return all rows, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def rowsAs[T](implicit ev: Conversions.Decodable[T]): Try[Seq[T]] = {
    val out = rows.map(row => {
      ev.decode(row.data(), Conversions.JsonFlags)
    })

    FunctionalUtil.traverse(out)
  }
}

/** The results of an Analytics query, as returned by the reactive API.
  *
  * @param rows            a Flux of any returned rows.  If the Analytics service returns an error while returning the
  *                        rows, it will
  *                        be raised on this Flux
  * @param clientContextId if a clientContextId was provided in [[AnalyticsOptions]] it will be returned here.  This
  *                        allows the application to tie requests and responses together.
  * @param signature       details of the signature of the Analytics query, if any were present
  * @param meta            a Mono containing additional information related to the Analytics query, that is received from the
  *                        Analytics service after any rows and errors
  */
case class ReactiveAnalyticsResult(private[scala] val rows: Flux[AnalyticsChunkRow],
                                   meta: Mono[AnalyticsMeta]) {
  /** Return all rows, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def rowsAs[T](implicit ev: Conversions.Decodable[T]): Flux[T] = {
    rows.map(row => ev.decode(row.data(), Conversions.JsonFlags) match {
      case Success(v) => v
      case Failure(err) => throw err
    })
  }
}

/** Returns the profile information of a Analytics request, in JSON form. */
case class AnalyticsProfile(private val _content: Array[Byte]) {

  /** Return the content as an `Array[Byte]` */
  def contentAsBytes: Array[Byte] = _content

  /** Return the content, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }

  override def toString: String = contentAs[JsonObject].toString
}

/** An error returned by the Analytics service. */
case class AnalyticsError(private val content: Array[Byte]) extends CouchbaseException {
  private lazy val str = new String(content, CharsetUtil.UTF_8)
  private lazy val json = JsonObject.fromJson(str)

  /** A human-readable error code. */
  def msg: String = {
    json.safe.str("msg") match {
      case Success(msg) => msg
      case Failure(_) => s"unknown error ($str)"
    }
  }

  /** The raw error code returned by the Analytics service. */
  def code: Try[Int] = {
    json.safe.num("code")
  }


  override def toString: String = msg
}

/** Returns the signature information of a Analytics request, in JSON form. */
case class AnalyticsSignature(private val _content: Array[Byte]) {
  /** Return the content as an `Array[Byte]` */
  def contentAsBytes: Array[Byte] = {
    _content
  }

  /** Return the content, converted into the application's preferred representation.
    *
    * The content is a JSON object, so a suitable default representation would be [[JsonObject]].
    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }

  override def toString: String = contentAs[JsonObject].get.toString
}

/** Returns any warnings of a Analytics request, in JSON form. */
case class AnalyticsWarnings(private val _content: Array[Byte]) {
  /** Return the content as an `Array[Byte]` */
  def contentAsBytes: Array[Byte] = {
    _content
  }

  /** Return the content, converted into the application's preferred representation.
    *
    * The content is a JSON array, so a suitable default representation would be
    * [[com.couchbase.client.scala.json.JsonArray]].
    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }

  override def toString: String = contentAs[JsonObject].get.toString
}

/** Metrics of a given Analytics request.
  *
  * @param elapsedTime   the total time taken for the request, that is the time from when the
  *                      request was received until the results were returned, in a human-readable
  *                      format (eg. 123.45ms for a little over 123 milliseconds).
  * @param executionTime the time taken for the execution of the request, that is the time from
  *                      when analytics execution started until the results were returned, in a human-readable
  *                      format (eg. 123.45ms for a little over 123 milliseconds).
  * @param resultCount   the total number of results selected by the engine before restriction
  *                      through LIMIT clause.
  * @param resultSize    the total number of returned rows.
  * @param processedObjects   the total number of processed objects.
  * @param mutationCount the number of mutations that were made during the request.
  * @param sortCount     the total number of results selected by the engine before restriction
  *                      through LIMIT clause.
  * @param errorCount    the number of errors that occurred during the request.
  * @param warningCount  the number of warnings that occurred during the request.
  */
case class AnalyticsMetrics(elapsedTime: String,
                            executionTime: String,
                            resultCount: Int,
                            resultSize: Int,
                            processedObjects: Int,
                            mutationCount: Int,
                            sortCount: Int,
                            errorCount: Int,
                            warningCount: Int)

private[scala] object AnalyticsMetrics {
  def fromBytes(in: Array[Byte]): AnalyticsMetrics = {
    JsonObjectSafe.fromJson(new String(in, CharsetUtil.UTF_8)) match {
      case Success(jo) =>
        AnalyticsMetrics(
          jo.str("elapsedTime").getOrElse(""),
          jo.str("executionTime").getOrElse(""),
          jo.num("resultCount").getOrElse(0),
          jo.num("resultSize").getOrElse(0),
          jo.num("processedObjects").getOrElse(0),
          jo.num("mutationCount").getOrElse(0),
          jo.num("sortCount").getOrElse(0),
          jo.num("errorCount").getOrElse(0),
          jo.num("warningCount").getOrElse(0)
        )

      case Failure(err) =>
        AnalyticsMetrics("", "", 0, 0, 0, 0, 0, 0, 0)
    }

  }
}

/** Additional information returned by the Analytics service after any rows and errors.
  *
  * @param metrics         metrics related to the Analytics request, if they were not disabled in [[AnalyticsOptions]]
  * @param warnings        any warnings returned from the Analytics service
  * @param status          the raw status string returned from the Analytics service
  * @param profile         if a profile was requested in [[AnalyticsOptions]] it will be returned here
  */
case class AnalyticsMeta(private[scala] val requestId: String,
                         clientContextId: Option[String],
                         signature: Option[AnalyticsSignature],
                         metrics: Option[AnalyticsMetrics],
                         warnings: Option[AnalyticsWarnings],
                         status: String,
                         profile: Option[AnalyticsProfile])
