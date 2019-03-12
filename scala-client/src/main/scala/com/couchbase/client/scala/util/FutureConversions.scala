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
package com.couchbase.client.scala.util


import java.util.concurrent.CompletableFuture

import com.couchbase.client.core.msg.{CancellationReason, Request}
import com.couchbase.client.scala.query.QueryResult
import reactor.core.publisher.SignalType

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import reactor.core.publisher.{Flux => JavaFlux, Mono => JavaMono}
import reactor.core.scala.publisher.{Flux => ScalaFlux, Mono => ScalaMono}
import scala.compat.java8.FutureConverters._

/** Convert between Java and Scala async and reactive APIs.
  *
  * Note: Scala Monos are different classes to Java Monos
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] object FutureConversions {
  def javaCFToScalaFuture(future: CompletableFuture[QueryResult]): Future[QueryResult] = {
    future.toScala
  }

  def javaMonoToScalaFuture[T](in: JavaMono[T]): Future[T] = {
    FutureConverters.toScala(in.toFuture)
  }

  def javaMonoToScalaMono[T](in: JavaMono[T]): ScalaMono[T] = {
    ScalaMono.from(in)
  }

  def javaFluxToScalaFlux[T](in: JavaFlux[T]): ScalaFlux[T] = {
    ScalaFlux.from(in)
  }

  def javaCFToScalaMono[T](request: Request[_],
                           response: CompletableFuture[T],
                           propagateCancellation: Boolean): ScalaMono[T] = {
    val javaMono = JavaMono.fromFuture(response)
    val scalaMono = ScalaMono.from(javaMono)

    if (propagateCancellation) {
      scalaMono.doFinally(st => {
        if (st == SignalType.CANCEL) {
          request.cancel(CancellationReason.STOPPED_LISTENING)
        }
      })
    }
    else scalaMono
  }

  def javaCFToJavaMono[T](request: Request[_],
                           response: CompletableFuture[T],
                           propagateCancellation: Boolean): JavaMono[T] = {
    val javaMono = JavaMono.fromFuture(response)

    if (propagateCancellation) {
      javaMono.doFinally(st => {
        if (st == SignalType.CANCEL) {
          request.cancel(CancellationReason.STOPPED_LISTENING)
        }
      })
    }
    else javaMono
  }
}
