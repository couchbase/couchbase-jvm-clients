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

import com.couchbase.client.core.env.Credentials
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.query.QueryResult
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration}
import scala.util.Try

// TODO: v important, check what happens when converting CompletableFuture to Future - want to happen on core's threadpool
class Cluster(env: => ClusterEnvironment)
             (implicit ec: ExecutionContext) {

  val async = new AsyncCluster(env)
  // TODO
  //  val reactive = new ReactiveCluster(async)

  def bucket(name: String) = {
    AsyncUtils.block(async.bucket(name))
      .map(new Bucket(_))
  }

  implicit def scalaDurationToJava(in: scala.concurrent.duration.Duration): java.time.Duration = {
    java.time.Duration.ofNanos(in.toNanos)
  }

  implicit def javaDurationToScala(in: java.time.Duration): scala.concurrent.duration.Duration = {
    Duration.apply(in.toNanos, TimeUnit.NANOSECONDS)
  }

  // TODO MVP
  def query(statement: String, options: QueryOptions = QueryOptions()): Try[QueryResult] = {
    val timeout: java.time.Duration = options.timeout match {
      case Some(v) => v
      case _ => env.timeoutConfig.queryTimeout()
    }

    AsyncUtils.block(async.query(statement, options), timeout)
  }

  def shutdown(): Unit = {
    AsyncUtils.block(async.shutdown(), Duration.Inf)
  }
}

object Cluster {
  private val threadPool = Executors.newFixedThreadPool(10) // TODO 10? should we use core's instead?
  private implicit val ec = ExecutionContext.fromExecutor(threadPool)

  def connect(connectionString: String, username: String, password: String): Try[Cluster] = {
    Try(new Cluster(ClusterEnvironment.create(connectionString, username, password)))
  }

  def connect(connectionString: String, credentials: Credentials): Try[Cluster] = {
    Try(new Cluster(ClusterEnvironment.create(connectionString, credentials)))
  }

  def connect(environment: ClusterEnvironment): Try[Cluster] = {
    Try(new Cluster(environment))
  }
}
