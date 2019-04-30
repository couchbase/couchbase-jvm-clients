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
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.util.{FunctionalUtil, RowTraversalUtil}
import reactor.core.scala.publisher.{Flux, Mono}

import scala.util.{Failure, Success, Try}

/** The results of a N1QL query.
  *
  * @param rows            all rows returned from the query
  * @param meta            any additional information related to the query
  *
  * @define SupportedTypes The rows can be converted into the user's desired type.  This can be any type for which an
  *                        implicit `Decodable[T]` can be found, and can include [[JsonObject]], a case class, String,
  *                        or one of a number of supported third-party JSON libraries.
  * @author Graham Pople
  * @since 1.0.0
  */
case class QueryResult(private[scala] val rows: Seq[QueryChunkRow],
                       meta: QueryMeta) {
  /** Returns an [[Iterator]] of any returned rows.  All rows are buffered from the query service first.
    *
    * $SupportedTypes
    *
    * The return type is of `Iterator[Try[T]]` in case any row cannot be decoded.  See allRowsAs` for a more
    * convenient interface that does not require handling individual row decode errors.
    **/
  def rowsAs[T]
  (implicit ev: Conversions.Decodable[T]): Iterator[Try[T]] = {
    rows.iterator.map(row => ev.decode(row.data(), Conversions.JsonFlags))
  }

  /** All returned rows.  All rows are buffered from the query service first.
    *
    * $SupportedTypes
    *
    * @return either `Success` if all rows could be decoded successfully, or a Failure containing the first error
    */
  def allRowsAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[Seq[T]] = {
    RowTraversalUtil.traverse(rowsAs[T])
  }
}

/** The results of a N1QL query, as returned by the reactive API.
  *
  * @param meta            any additional information related to the query
  */
case class ReactiveQueryResult(private val rows: Flux[QueryChunkRow],
                               meta: Mono[QueryMeta]) {
  /** A Flux of any returned rows, streamed directly from the query service.  If the query service returns an error
    * while returning the rows, it will be raised on this.
    *
    * $SupportedTypes
    */
  def rowsAs[T]
  (implicit ev: Conversions.Decodable[T]): Flux[T] = {
    rows.map(row => {
      // The .get will raise an exception as .onError on the flux
      ev.decode(row.data(), Conversions.JsonFlags).get
    })
  }
}

/** Returns the profile information of a query request, in JSON form. */
case class QueryProfile(private val _content: Array[Byte]) {

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

/** Returns the signature information of a query request, in JSON form. */
case class QuerySignature(private val _content: Array[Byte]) {
  /** Return the content as an `Array[Byte]` */
  def contentAsBytes: Array[Byte] = {
    _content
  }

  /** Return the content, converted into the application's preferred representation.
    *
    * The content is a JSON object, so a suitable representation would be [[JsonObject]].
    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }

  override def toString: String = contentAs[JsonObject].get.toString
}

/** Returns any warnings of a query request, in JSON form. */
case class QueryWarnings(private val _content: Array[Byte]) {
  /** Return the content as an `Array[Byte]` */
  def contentAsBytes: Array[Byte] = {
    _content
  }

  /** Return the content, converted into the application's preferred representation.
    *
    * The content is a JSON array, so a suitable representation would be
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

/** Metrics of a given query request.
  *
  * @param elapsedTime   the total time taken for the request, that is the time from when the
  *                      request was received until the results were returned, in a human-readable
  *                      format (eg. 123.45ms for a little over 123 milliseconds).
  * @param executionTime the time taken for the execution of the request, that is the time from
  *                      when query execution started until the results were returned, in a human-readable
  *                      format (eg. 123.45ms for a little over 123 milliseconds).
  * @param resultCount   the total number of results selected by the engine before restriction
  *                      through LIMIT clause.
  * @param resultSize    the total number of returned rows.
  * @param mutationCount the number of mutations that were made during the request.
  * @param sortCount     the total number of results selected by the engine before restriction
  *                      through LIMIT clause.
  * @param errorCount    the number of errors that occurred during the request.
  * @param warningCount  the number of warnings that occurred during the request.
  */
case class QueryMetrics(elapsedTime: String,
                        executionTime: String,
                        resultCount: Int,
                        resultSize: Int,
                        mutationCount: Int,
                        sortCount: Int,
                        errorCount: Int,
                        warningCount: Int)

private[scala] object QueryMetrics {
  def fromBytes(in: Array[Byte]): QueryMetrics = {
    JsonObjectSafe.fromJson(new String(in, CharsetUtil.UTF_8)) match {
      case Success(jo) =>
        QueryMetrics(
          jo.str("elapsedTime").getOrElse(""),
          jo.str("executionTime").getOrElse(""),
          jo.num("resultCount").getOrElse(0),
          jo.num("resultSize").getOrElse(0),
          jo.num("mutationCount").getOrElse(0),
          jo.num("sortCount").getOrElse(0),
          jo.num("errorCount").getOrElse(0),
          jo.num("warningCount").getOrElse(0)
        )

      case Failure(err) =>
        QueryMetrics("", "", 0, 0, 0, 0, 0, 0)
    }

  }
}

/** Additional information returned by the query service aside from any rows and errors.
  *
  * @param metrics         metrics related to the query request, if they were not disabled in [[QueryOptions]]
  * @param warnings        any warnings returned from the query service
  * @param status          the raw status string returned from the query service
  * @param profile         if a profile was requested in [[QueryOptions]] it will be returned here
  */
case class QueryMeta(private[scala] val requestId: String,
                     clientContextId: Option[String],
                     signature: Option[QuerySignature],
                     metrics: Option[QueryMetrics],
                     warnings: Option[QueryWarnings],
                     status: String,
                     profile: Option[QueryProfile])


