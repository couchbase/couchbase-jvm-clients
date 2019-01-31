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

import com.couchbase.client.scala.env.ClusterEnvironment
import java.util.concurrent.{Executors, TimeUnit}

import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.query.QueryResult
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class Cluster(env: => ClusterEnvironment)
             (implicit ec: ExecutionContext) {

  private val async = new AsyncCluster(env)
  // TODO MVP? reactive cluster

  def bucket(name: String) = {
    AsyncUtils.block(async.bucket(name))
      .map(new Bucket(_))
  }

  // TODO share this code
  implicit def scalaFiniteDurationToJava(in: scala.concurrent.duration.FiniteDuration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def scalaDurationToJava(in: scala.concurrent.duration.Duration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def javaDurationToScala(in: java.time.Duration): scala.concurrent.duration.FiniteDuration = {
    FiniteDuration.apply(in.toNanos, TimeUnit.NANOSECONDS)
  }

  // TODO MVP
  def query(statement: String, options: QueryOptions = QueryOptions()): Try[QueryResult] = {
    val timeout: java.time.Duration = options.timeout match {
      case Some(v) => v
      case _ => env.queryTimeout()
    }

    AsyncUtils.block(async.query(statement, options), timeout)
  }

  // TODO see how other libs do case class encoding/decoding
}

object Cluster {
  private val threadPool = Executors.newFixedThreadPool(10) // TODO 10?
  private implicit val ec = ExecutionContext.fromExecutor(threadPool)

  // TODO MVP decision may be made to defer errors until the operation level, e.g. opening resources would always
  // succeed
  def connect(connectionString: String, username: String, password: String) = {
    Try(new Cluster(ClusterEnvironment.create(connectionString, username, password)))
  }

  // TODO MVP support credentials
}
