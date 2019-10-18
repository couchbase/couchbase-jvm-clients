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

package com.couchbase.client.scala.query

import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.query.{QueryChunkRow, QueryResponse}
import com.couchbase.client.core.util.Golang
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.util.{DurationConversions, FunctionalUtil, RowTraversalUtil}
import com.couchbase.client.scala.util.RowTraversalUtil
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** The results of a N1QL query.
  *
  * @param rows            all rows returned from the query
  * @param metaData            any additional information related to the query
  *
  * @define SupportedTypes The rows can be converted into the user's desired type.  This can be any type for which an
  *                        implicit `Decodable[T]` can be found, and can include [[JsonObject]], a case class, String,
  *                        or one of a number of supported third-party JSON libraries.
  * @author Graham Pople
  * @since 1.0.0
  */
case class QueryResult(private[scala] val rows: Seq[QueryChunkRow],
                       metaData: QueryMetaData) {
  /** Returns an [[Iterator]] of any returned rows.  All rows are buffered from the query service first.
    *
    * $SupportedTypes
    *
    * The return type is of `Iterator[Try[T]]` in case any row cannot be decoded.  See rowsAs` for a more
    * convenient interface that does not require handling individual row decode errors.
    **/
  def rowsAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[Seq[T]] = {
    RowTraversalUtil.traverse(rows.iterator.map(row => {
      ev.decode(row.data(), Conversions.JsonFlags)
    }))
  }
}

/** The results of a N1QL query, as returned by the reactive API.
  *
  * @param metaData            any additional information related to the query
  */
case class ReactiveQueryResult(private[scala] val rows: SFlux[QueryChunkRow],
                               metaData: SMono[QueryMetaData]) {
  /** A Flux of any returned rows, streamed directly from the query service.  If the query service returns an error
    * while returning the rows, it will be raised on this.
    *
    * $SupportedTypes
    */
  def rowsAs[T]
  (implicit ev: Conversions.Decodable[T]): SFlux[T] = {
    rows.map(row => {
      // The .get will raise an exception as .onError on the flux
      ev.decode(row.data(), Conversions.JsonFlags).get
    })
  }
}

/** If an error is returned by the query service as it is processing rows, this will be raised. */
case class QueryError(private val content: Array[Byte]) extends CouchbaseException {
  private lazy val str = new String(content, CharsetUtil.UTF_8)
  private lazy val json = JsonObjectSafe.fromJson(str)

  /** A human-readable error code. */
  def msg: String = {
    json match {
      case Success(j) =>
        j.str("msg") match {
          case Success(msg) => msg
          case Failure(_) => s"unknown error ($str)"
        }
      case Failure(err) => s"unknown error ($str)"
    }
  }

  /** The raw error code returned by the query service. */
  def code: Try[Int] = {
    json.flatMap(_.num("code"))
  }


  override def toString: String = msg
}

/** A warning returned from the query service. */
case class QueryWarning(code: Int, message: String)


/** Metrics of a given query request.
  *
  * @param elapsedTime   the total time taken for the request, that is the time from when the
  *                      request was received until the results were returned.
  * @param executionTime the time taken for the execution of the request, that is the time from
  *                      when query execution started until the results were returned.
  * @param resultCount   the total number of results selected by the engine before restriction
  *                      through LIMIT clause.
  * @param resultSize    the total number of returned rows.
  * @param mutationCount the number of mutations that were made during the request.
  * @param sortCount     the total number of results selected by the engine before restriction
  *                      through LIMIT clause.
  * @param errorCount    the number of errors that occurred during the request.
  * @param warningCount  the number of warnings that occurred during the request.
  */
case class QueryMetrics(elapsedTime: Duration,
                        executionTime: Duration,
                        resultCount: Long,
                        resultSize: Long,
                        mutationCount: Long,
                        sortCount: Long,
                        errorCount: Long,
                        warningCount: Long)

private[scala] object QueryMetrics {
  import com.couchbase.client.scala.util.DurationConversions._

  def fromBytes(in: Array[Byte]): Option[QueryMetrics] = {
    JsonObjectSafe.fromJson(new String(in, CharsetUtil.UTF_8)) match {
      case Success(jo) =>

        Some(QueryMetrics(
          jo.str("elapsedTime").map(time => {
            DurationConversions.javaDurationToScala(Golang.parseDuration(time))
          }).getOrElse(Duration.Zero),
          jo.str("executionTime").map(time => {
            DurationConversions.javaDurationToScala(Golang.parseDuration(time))
          }).getOrElse(Duration.Zero),
          jo.numLong("resultCount").getOrElse(0l),
          jo.numLong("resultSize").getOrElse(0l),
          jo.numLong("mutationCount").getOrElse(0l),
          jo.numLong("sortCount").getOrElse(0l),
          jo.numLong("errorCount").getOrElse(0l),
          jo.numLong("warningCount").getOrElse(0l)
        ))

      case Failure(err) =>
        None
    }

  }
}

/** Additional information returned by the query service aside from any rows and errors.
  *
  * @param clientContextId the client context id passed into [[QueryOptions]]
  * @param metrics         metrics related to the query request, if they were enabled in [[QueryOptions]]
  * @param warnings        any warnings returned from the query service
  * @param status          the status returned from the query service
  */
case class QueryMetaData(private[scala] val requestId: String,
                         clientContextId: String,
                         private val _signatureContent: Option[Array[Byte]],
                         metrics: Option[QueryMetrics],
                         warnings: Seq[QueryWarning],
                         status: QueryStatus,
                         private val _profileContent: Option[Array[Byte]]) {

  /** Return the profile content, converted into the application's preferred representation.
    *
    * Note a profile must first be requested with [[QueryOptions.profile]].
    *
    * @tparam T $SupportedTypes
    */
  def profileAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    _profileContent match {
      case Some(content) => ev.decode(content, Conversions.JsonFlags)
      case _ => Failure(new IllegalArgumentException("No profile is available"))
    }
  }

  /** Return any signature content, converted into the application's preferred representation.
    *
    * @tparam T $SupportedTypes
    */
  def signatureAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    _signatureContent match {
      case Some(content) => ev.decode(content, Conversions.JsonFlags)
      case _ => Failure(new IllegalArgumentException("No signature is available"))
    }
  }
}


sealed trait QueryStatus

object QueryStatus {
  case object Running extends QueryStatus
  case object Success extends QueryStatus
  case object Errors extends QueryStatus
  case object Completed extends QueryStatus
  case object Stopped extends QueryStatus
  case object Timeout extends QueryStatus
  case object Closed extends QueryStatus
  case object Fatal extends QueryStatus
  case object Aborted extends QueryStatus
  case object Unknown extends QueryStatus
}