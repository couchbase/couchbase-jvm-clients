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

import com.couchbase.client.core.msg.query.QueryResponse
import com.couchbase.client.scala.codec.Conversions

import scala.util.Try

case class QueryResult(
  rows: Seq[QueryRow],
  _errors: Seq[Array[Byte]],
//  status: String,
//  requestId: String,
//  clientContextId: String
  // TODO other params
                      ) {
//  def errors: Seq[String]
}


case class QueryRow(_content: Array[Byte]) {

  def contentAsBytes: Array[Byte] = _content

  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonFlags)
  }
}
