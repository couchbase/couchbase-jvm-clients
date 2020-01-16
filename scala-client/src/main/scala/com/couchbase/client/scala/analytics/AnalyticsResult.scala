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
import com.couchbase.client.core.error.ErrorCodeAndMessage
import com.couchbase.client.core.msg.analytics.AnalyticsChunkRow
import com.couchbase.client.core.util.Golang
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.json.JsonObjectSafe
import com.couchbase.client.scala.util.{DurationConversions, RowTraversalUtil}
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** The results of an Analytics query.
  *
  * @param rows            all rows returned from the analytics service
  * @param metaData            any additional information related to the Analytics query
  *
  * @author Graham Pople
  * @since 1.0.0
  */
case class AnalyticsResult(
    private[scala] val rows: Seq[AnalyticsChunkRow],
    metaData: AnalyticsMetaData
) {

  /** All returned rows.  All rows are buffered from the analytics service first.
    *
    * @tparam T The rows can be converted into the user's desired type.  This can be any type for which an
    *                        implicit `JsonDeserializer[T]` can be found, and can include
    *                        `com.couchbase.client.scala.json.JsonObject`, a case class, String,
    *                        or one of a number of supported third-party JSON libraries.
    *
    * @return either `Success` if all rows could be decoded successfully, or a Failure containing the first error
    */
  def rowsAs[T](implicit deserializer: JsonDeserializer[T]): Try[collection.Seq[T]] = {
    val all = rows.iterator.map(row => deserializer.deserialize(row.data()))
    RowTraversalUtil.traverse(all)
  }
}

/** The results of an Analytics query, as returned by the reactive API.
  *
  * @param rows            a Flux of any returned rows.  If the Analytics service returns an error while returning the
  *                        rows, it will
  *                        be raised on this Flux
  * @param meta            any additional information related to the Analytics query
  */
case class ReactiveAnalyticsResult(
    private[scala] val rows: SFlux[AnalyticsChunkRow],
    meta: SMono[AnalyticsMetaData]
) {

  /** Return all rows, converted into the application's preferred representation.
    *
    * @tparam T this can be of any type for which an implicit
    *   *                        `com.couchbase.client.scala.codec.Conversions.JsonSerializer` can be found: a list
    *   *                        of types that are supported 'out of the box' is available at
    *   *                        [[https://docs.couchbase.com/scala-sdk/1.0/howtos/json.html these JSON docs]]
    */
  def rowsAs[T](implicit deserializer: JsonDeserializer[T]): SFlux[T] = {
    rows.map(
      row =>
        deserializer.deserialize(row.data()) match {
          case Success(v)   => v
          case Failure(err) => throw err
        }
    )
  }
}

case class AnalyticsWarning(private val inner: ErrorCodeAndMessage) {
  def code: Int = inner.code

  def message: String = inner.message
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
  * @param errorCount    the number of errors that occurred during the request.
  * @param warningCount  the number of warnings that occurred during the request.
  */
case class AnalyticsMetrics(
    elapsedTime: Duration,
    executionTime: Duration,
    resultCount: Long,
    resultSize: Long,
    processedObjects: Long,
    errorCount: Long,
    warningCount: Long
)

private[scala] object AnalyticsMetrics {
  def fromBytes(in: Array[Byte]): AnalyticsMetrics = {
    JsonObjectSafe.fromJson(new String(in, CharsetUtil.UTF_8)) match {
      case Success(jo) =>
        AnalyticsMetrics(
          jo.str("elapsedTime")
            .map(time => {
              DurationConversions.javaDurationToScala(Golang.parseDuration(time))
            })
            .getOrElse(Duration.Zero),
          jo.str("executionTime")
            .map(time => {
              DurationConversions.javaDurationToScala(Golang.parseDuration(time))
            })
            .getOrElse(Duration.Zero),
          jo.numLong("resultCount").getOrElse(0L),
          jo.numLong("resultSize").getOrElse(0L),
          jo.numLong("sortCount").getOrElse(0L),
          jo.numLong("errorCount").getOrElse(0L),
          jo.numLong("warningCount").getOrElse(0L)
        )

      case Failure(err) =>
        AnalyticsMetrics(Duration.Zero, Duration.Zero, 0, 0, 0, 0, 0)
    }

  }
}

/** Additional information returned by the Analytics service after any rows and errors.
  *
  * @param metrics         metrics related to the Analytics request, if they are available
  * @param warnings        any warnings returned from the Analytics service
  * @param status          the raw status string returned from the Analytics service
  */
case class AnalyticsMetaData(
    requestId: String,
    clientContextId: String,
    private val signatureContent: Option[Array[Byte]],
    metrics: AnalyticsMetrics,
    warnings: collection.Seq[AnalyticsWarning],
    status: AnalyticsStatus
) {

  /** Return any signature content, converted into the application's preferred representation.
    *
    * @tparam T The rows can be converted into the user's desired type.  This can be any type for which an
    *                        implicit `JsonDeserializer[T]` can be found, and can include
    *                        `com.couchbase.client.scala.json.JsonObject`, a case class, String,
    *                        or one of a number of supported third-party JSON libraries.
    */
  def signatureAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    signatureContent match {
      case Some(content) => deserializer.deserialize(content)
      case _             => Failure(new IllegalArgumentException("No signature is available"))
    }
  }
}
