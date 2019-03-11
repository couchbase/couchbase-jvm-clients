/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.env.Credentials
import com.couchbase.client.core.error.QueryServiceException
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import reactor.core.scala.publisher.{Flux, Mono}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._
import reactor.core.publisher.{Flux => JavaFlux, Mono => JavaMono}
import reactor.core.scala.publisher.{Flux => ScalaFlux, Mono => ScalaMono}
import scala.compat.java8.OptionConverters._

class ReactiveCluster(val async: AsyncCluster)
                     (implicit ec: ExecutionContext) {
  private val env = async.env

  def query(statement: String, options: QueryOptions = QueryOptions()): ScalaMono[ReactiveQueryResult] = {
    async.queryHandler.request(statement, options, async.core, async.env) match {
      case Success(request) =>

        async.core.send(request)

        val ret: JavaMono[ReactiveQueryResult] = FutureConversions.javaCFToJavaMono(request, request.response(), false)
          .map(response => {
            val rows: ScalaFlux[QueryRow] = FutureConversions.javaFluxToScalaFlux(response.rows())
              .map[QueryRow](bytes => {
              QueryRow(bytes)
            }).onErrorResume(err => {
              err match {
                case e: QueryServiceException => ScalaMono.error(QueryError(e.content))
                case _ => ScalaMono.error(err)
              }
            })

            val additional: ScalaMono[QueryAdditional] = FutureConversions.javaMonoToScalaMono(response.additional())
                .map(addl => {
                  QueryAdditional(QueryMetrics.fromBytes(addl.metrics),
                    addl.warnings.asScala.map(QueryError),
                    addl.status,
                    addl.profile.asScala.map(v => QueryProfile(v))
                  )
                })

            ReactiveQueryResult(
              rows,
              response.requestId(),
              response.clientContextId().asScala,
              QuerySignature(response.signature().asScala),
              additional
              )
          })

        FutureConversions.javaMonoToScalaMono(ret)

      case Failure(err) =>
        ScalaMono.error(err)
    }
  }

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