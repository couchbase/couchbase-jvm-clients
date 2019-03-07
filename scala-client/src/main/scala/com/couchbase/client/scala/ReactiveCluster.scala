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
import java.util.concurrent.atomic.AtomicReference

import com.couchbase.client.core.env.Credentials
import com.couchbase.client.scala.api.QueryOptions
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import reactor.core.scala.publisher.{Flux, Mono}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

import reactor.core.publisher.{Mono => JavaMono, Flux => JavaFlux}
import reactor.core.scala.publisher.{Mono => ScalaMono, Flux => ScalaFlux}

class ReactiveCluster(val async: AsyncCluster)
                     (implicit ec: ExecutionContext) {
  private val env = async.env

  import DurationConversions._

  def query(statement: String, options: QueryOptions = QueryOptions()): JavaMono[ReactiveQueryResult] = {
    async.queryHandler.request(statement, options, async.core, async.env) match {
      case Success(request) =>

        async.core.send(request)

        val ret: JavaMono[ReactiveQueryResult] = FutureConversions.javaCFToJavaMono(request, request.response(), false)
          .map(response => {

            //            val requestIdKeeper = new AtomicReference[String]()
            //            val clientContextIdKeeper = new AtomicReference[String]()

            //            val requestIdStore: JavaMono[String] = response.clientContextId().doOnNext(v => {
            //              clientContextIdKeeper.set(v)
            //            }).doOnSubscribe(_ => {
            //              println("sub")
            //            })
            //            val clientContextIdStore: JavaMono[String] = response.requestId().doOnNext(v => {
            //              requestIdKeeper.set(v)
            //            })

            //            val x: JavaMono[ReactiveQueryResult] =
            //              requestIdStore
            //              .`then`(clientContextIdStore)
            //              .map[ReactiveQueryResult](_ => {

            val justRows: JavaFlux[QueryRow] = response.rows()
              .map[QueryRow](bytes => {
              QueryRow(bytes)
            })
//              .doOnSubscribe(_ => println("rows subscribed"))
//              .doOnNext(v => println(s"rows next ${v.contentAs[String]}"))
//              .doOnComplete(() => println("rows completed"))
//              .doOnTerminate(() => println("rows terminate"))

//            val justErrs: JavaFlux[QueryError] = response.errors().map(bytes => QueryError(bytes))

//            FutureConversions.javaFluxToScalaFlux(justRows).zipWith[Array[Byte], QueryRow](errs, (a, b) => {
//              a
//            })

//            justRows.zipWith(response.errors(), (a, b) => {
//
//            })

            // At the end, see if there are any errors, and raise if so
//            val rowsWithErrors: JavaFlux[QueryRow] = justRows.flatMap(_ => {
//              response.errors()
//
//                .doOnSubscribe(_ => println("err subscribed"))
//                .doOnNext(_ => println("err next"))
//                .doOnComplete(() => println("err completed"))
//                .doOnTerminate(() => println("err terminate"))
//
//                .collectList()
//                .flatMap[QueryRow](errs => {
//                if (!errs.isEmpty) {
//                  JavaMono.error(QueryServiceException(errs.asScala.map(QueryError)))
//                }
//                else {
//                  JavaMono.empty[QueryRow]()
//                }
//              })
//            })

//            val rowsOut = rowsWithErrors.flux()
//              .doOnComplete(() => println("out done2"))
//              .doOnSubscribe(_ => println("out sub"))
//              .doOnNext(_ => println("out next"))
//              .doOnTerminate(() => println("out tet"))

            ReactiveQueryResult(
              //                requestIdKeeper.get(),
              //                clientContextIdKeeper.get(),
//              FutureConversions.javaFluxToScalaFlux(rowsWithErrors),
              FutureConversions.javaFluxToScalaFlux(justRows),
              null
//              FutureConversions.javaFluxToScalaFlux(justErrs)
            )
            //            })
            //                .doOnSubscribe(_ => {
            //                  println("sub")
            //                })
            //
            //            x
          })

        ret
//        FutureConversions.javaMonoToScalaMono(ret)


      case Failure(err) =>
        JavaMono.error(err)
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