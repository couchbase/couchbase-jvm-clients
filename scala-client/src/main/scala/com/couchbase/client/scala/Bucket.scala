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

package com.couchbase.client.scala

import com.couchbase.client.scala.api.QueryOptions
//import com.couchbase.client.scala.query.{N1qlQueryResult, N1qlResult}
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext

class Bucket(val async: AsyncBucket)
            (implicit ec: ExecutionContext) {
  def collection(scopeName: String, collection: String) = {
    scope(Defaults.DefaultScope).flatMap(_.collection(collection))
  }

  def defaultCollection() = {
    scope(Defaults.DefaultScope).flatMap(_.defaultCollection())
  }

  def scope(name: String) = {
    AsyncUtils.block(async.scope(name))
      .map(asyncScope => new Scope(asyncScope, async.name))
  }

  // TODO BLOCKED
//  def query(statement: String, query: QueryOptions = QueryOptions()): N1qlQueryResult = ???
//
//  def queryAs[T](statement: String, query: QueryOptions = QueryOptions()): N1qlResult[T] = ???
}
