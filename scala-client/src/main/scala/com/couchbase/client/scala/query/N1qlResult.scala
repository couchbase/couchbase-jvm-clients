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

import com.couchbase.client.scala.document.JsonObject

class N1qlResult[T] {
  def rows(): Iterable[T] = null
  def allRows(): List[T] = null
  def status(): String = null
  def requestId(): String = null
  def clientContextId(): String = null
  // TODO other params
}

case class N1lQueryRow(bytes: List[Byte]) {
  def value(): JsonObject = null
}

class N1qlQueryResult extends N1qlResult[N1lQueryRow]
