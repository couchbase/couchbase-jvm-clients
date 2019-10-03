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


import java.util.concurrent.{CompletableFuture, CompletionException}
import java.util.function.Consumer

import com.couchbase.client.core.msg.{CancellationReason, Request, Response}
import reactor.core.publisher.{SignalType, Flux => JavaFlux, Mono => JavaMono}
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.compat.java8.FutureConverters
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future}

/** Convert between Java and Scala async and reactive APIs.
  *
  * Note: Scala Monos are different classes to Java Monos
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[scala] object FutureConversions {
  def javaCFToScalaFuture[T](future: CompletableFuture[T]): Future[T] = {
    future.toScala
  }

  def javaMonoToScalaFuture[T](in: JavaMono[T]): Future[T] = {
    FutureConverters.toScala(in.toFuture)
  }

  def javaMonoToScalaMono[T](in: JavaMono[T]): SMono[T] = {
    SMono(in)
  }

  def javaFluxToScalaFlux[T](in: JavaFlux[T]): SFlux[T] = {
    SFlux(in)
  }

  def javaCFToScalaMono[T](request: Request[_],
                           response: CompletableFuture[T],
                           propagateCancellation: Boolean): SMono[T] = {
    val javaMono = JavaMono.fromFuture(response)
    val scalaMono = SMono(javaMono)

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
      javaMono.doFinally(new Consumer[SignalType] {
        override def accept(t: SignalType): Unit = {
          if (t == SignalType.CANCEL) {
            request.cancel(CancellationReason.STOPPED_LISTENING)
          }
        }
      })
    }
    else javaMono
  }

  /**
    * Wraps a {@link Request} and returns it in a {@link Mono}.
    *
    * @param request               the request to wrap.
    * @param response              the full response to wrap, might not be the same as in the request.
    * @param propagateCancellation if a cancelled/unsubscribed mono should also cancel the
    *                              request.
    *
    * @return the mono that wraps the request.
    */
  def wrap[T](request: Request[_ <: Response],
              response: CompletableFuture[T],
              propagateCancellation: Boolean)
             (implicit ec: ExecutionContext): SMono[T] = {
    val future = javaCFToScalaFuture(response)
    var mono = SMono.fromFuture(future)
    if (propagateCancellation) {
      mono = mono.doFinally((st: SignalType) => {
        if (st == SignalType.CANCEL) request.cancel(CancellationReason.STOPPED_LISTENING)
      })
    }
    mono.onErrorResume((err: Throwable) => {
      if (err.isInstanceOf[CompletionException]) SMono.raiseError(err.getCause)
      else SMono.raiseError(err)
    })
  }


}
