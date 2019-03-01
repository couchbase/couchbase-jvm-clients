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
import com.couchbase.client.scala.json.JsonObject
import io.netty.util.CharsetUtil

import scala.util.{Failure, Success, Try}

abstract class QueryException extends CouchbaseException

case class QueryErrorException() extends QueryException
case class QueryServiceException(errors: Seq[QueryError]) extends QueryException

//class QueryException() extends CouchbaseException {
//  def first: QueryError
//}

case class QueryResult(
  rows: Seq[QueryRow],
//  errors: Seq[QueryError],
//  status: String,
//  requestId: String,
//  clientContextId: String
  // TODO other params
                      ) {
}


case class QueryRow(_content: Array[Byte]) {

  def contentAsBytes: Array[Byte] = _content

  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }
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

  override def toString: String = msg
}
