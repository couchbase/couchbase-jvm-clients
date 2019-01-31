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
import com.couchbase.client.scala.document._

import scala.util.Try

// TODO MVP
class N1qlResult[T] {
  def rows(): Iterable[T] = null
  def allRows(): List[T] = null
  def status(): String = null
  def requestId(): String = null
  def clientContextId(): String = null
  // TODO other params
}

case class QueryRow(_content: Array[Byte]) {

  def contentAsBytes: Array[Byte] = _content

  def contentAs[T]
  (implicit ev: Conversions.Decodable[T]): Try[T] = {
    ev.decode(_content, Conversions.JsonDecodeParams)
  }

//  def value: JsonObject = ???
//
//  def contentAs[T]: T = ???
//
//  def selectDynamic(name: String): GetSelecter = ???
//
//  def contentAsObject: JsonObject = contentAs[JsonObject]
//
//  def contentAsObject(path: String): JsonObject = contentAs[JsonObject](path)
//
//  def contentAsArray: JsonArray = contentAs[JsonArray]
//
//  def contentAsArray(path: String): JsonArray = contentAs[JsonArray](path)
//
//  def content(idx: Int): GetSelecter = ???
//
//  def content(path: String): GetSelecter = ???
//
//  def contentAs[T]: T = ???
//
//  def contentAs[T](path: String): T = ???
//
//  def contentAs[T](path: String, decoder: Array[Byte] => T): T = ???
//
//  def selectDynamic(name: String): GetSelecter = GetSelecter(this, PathElements(List(PathObjectOrField(name))))
//  def applyDynamic(name: String)(index: Int): GetSelecter = GetSelecter(this, PathElements(List(PathArray(name, index))))
}

class QueryResult(_result: QueryConsumer) {
  def rows: Iterator[QueryRow] = {
    // TODO error handling
    _result.rows
      .view // make it lazy so we don't do the entire map in one go
      .map(row => QueryRow(row.data()))
      .iterator
  }
}