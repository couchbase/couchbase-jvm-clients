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
import com.couchbase.client.scala.env.ClusterEnvironment
//import com.couchbase.client.scala.query.{N1qlQueryResult, N1qlResult}
import java.util.concurrent.Executors

import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext
import scala.util.Try

class Cluster(env: => ClusterEnvironment)
             (implicit ec: ExecutionContext) {

  private val asyncCluster = new AsyncCluster(env)
  // TODO MVP? reactive cluster
//  private val coreEnv = CoreEnvironment.create("ADMIN-TODO", "PASSWORD-TODO")
//  private val networkSet = new util.HashSet[NetworkAddress]()
//  networkSet.add(NetworkAddress.create(node))

//  val core = Core.create(coreEnv)

  def bucket(name: String) = {
    AsyncUtils.block(asyncCluster.bucket(name))
      .map(new Bucket(_))
  }

  // TODO MVP
//  def query(statement: String, query: QueryOptions = QueryOptions()): N1qlQueryResult = {
//    null
//  }
//
//  def queryAs[T](statement: String, query: QueryOptions = QueryOptions()): N1qlResult[T] = {
//    null
//  }

}

/// TODO     db.entries.select(_.car, _.id, _.velocity).where(_.car eqs carId).fetch()

object Cluster {
  private val threadPool = Executors.newFixedThreadPool(10) // TODO 10?
  private implicit val ec = ExecutionContext.fromExecutor(threadPool)

  // TODO MVP decision may be made to defer errors until the operation level, e.g. opening resources would always
  // succeed
  def connect(connectionString: String, username: String, password: String) = {
    Try(new Cluster(ClusterEnvironment.create(connectionString, username, password)))
  }
}
