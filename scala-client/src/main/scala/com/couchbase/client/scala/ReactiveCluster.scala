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

import java.util.concurrent.TimeUnit

import com.couchbase.client.core.env.Credentials
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.QueryResult
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import reactor.core.scala.publisher.{Flux, Mono}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class ReactiveCluster(val async: AsyncCluster)
                     (implicit ec: ExecutionContext) {
  private val env = async.env

  import DurationConversions._

  def query(statement: String, options: QueryOptions = QueryOptions()): Flux[QueryResult] = ???

  def bucket(name: String): Mono[ReactiveBucket] = {
    Mono.fromFuture(async.bucket(name)).map(v => new ReactiveBucket(v))
  }

  def shutdown(): Mono[Unit] = {
    Mono.fromFuture(async.shutdown())
  }
}

object ReactiveCluster {
  private implicit val ec = Cluster.ec

  def connect(connectionString: String, username: String, password: String): Mono[ReactiveCluster] = {
    Mono.fromFuture(AsyncCluster.connect(connectionString, username, password)
      .map(cluster => new ReactiveCluster(cluster)))
  }

  def connect(connectionString: String, credentials: Credentials): Mono[ReactiveCluster] = {
    Mono.fromFuture(AsyncCluster.connect(connectionString, credentials)
      .map(cluster => new ReactiveCluster(cluster)))
  }

  def connect(environment: ClusterEnvironment): Mono[ReactiveCluster] = {
    Mono.fromFuture(AsyncCluster.connect(environment)
      .map(cluster => new ReactiveCluster(cluster)))
  }
}