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

import java.util

import com.couchbase.client.core.Core
import com.couchbase.client.core.env.CoreEnvironment
import com.couchbase.client.core.io.NetworkAddress
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.query.{N1qlQueryResult, N1qlResult}

// TODO (michael: made the compiler happy, but obviously this needs fixing ;-))
class Cluster(env: CouchbaseEnvironment, node: String) {
  private val coreEnv = CoreEnvironment.create("ADMIN-TODO", "PASSWORD-TODO")
  private val networkSet = new util.HashSet[NetworkAddress]()
  networkSet.add(NetworkAddress.create(node))
  val core = Core.create(coreEnv)

  def openBucket(name: String) = new Bucket(this, name)

  def query(statement: String, query: QueryOptions = QueryOptions()): N1qlQueryResult = {
    null
  }

  def queryAs[T](statement: String, query: QueryOptions = QueryOptions()): N1qlResult[T] = {
    null
  }

}

object Cluster {
  def connect(node: String, username: String, password: String) = {
    new Cluster(CouchbaseEnvironment.default(), node)
  }
}
