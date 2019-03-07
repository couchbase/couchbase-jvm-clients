/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.msg.query.QueryResponse
import com.couchbase.client.scala.codec.Conversions
import com.couchbase.client.scala.json.{JsonObject, JsonObjectSafe}
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil
import reactor.core.scala.publisher.{Flux, Mono}

import scala.util.{Failure, Success, Try}

abstract class QueryException extends CouchbaseException

case class QueryErrorException() extends QueryException

case class QueryServiceException(errors: Seq[QueryError]) extends QueryException

case class QueryResult(rows: Seq[QueryRow],
                       requestId: String,
                       clientContextId: Option[String],
                       other: QueryOther)

case class QueryRow(_content: Array[Byte]) {

  def contentAsBytes: Array[Byte] = _content

  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }

  override def toString: String = contentAs[JsonObject].toString
}

case class QueryError(private val content: Array[Byte]) {
  private lazy val str = new String(content, CharsetUtil.UTF_8)
  private lazy val json = JsonObject.fromJson(str)

  def msg: String = {
    json.safe.str("msg") match {
      case Success(msg) => msg
      case Failure(_) => s"unknown error ($str)"
    }
  }

  def code: Try[Int] = {
    json.safe.num("code")
  }


  override def toString: String = msg
}

case class QueryMetrics(elapsedTime: String,
                        executionTime: String,
                        resultCount: Int,
                        resultSize: Int,
                        mutationCount: Int,
                        sortCount: Int,
                        errorCount: Int,
                        warningCount: Int)

object QueryMetrics {
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

case class QueryOther(metrics: QueryMetrics,
                      warnings: Seq[QueryError])


case class ReactiveQueryResult(
                              // TODO add
//                                requestId: String,
//                               clientContextId: String,
                               rows: Flux[QueryRow],
                              errors: Flux[QueryError]
//                               rows: reactor.core.publisher.Flux[QueryRow],
                              // TODO add
//                               other: Mono[QueryOther]
                              )


