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

import com.couchbase.client.core.msg.{CancellationReason, Request, Response}
import reactor.core.publisher.{SignalType, Flux => JavaFlux, Mono => JavaMono}
import reactor.core.scala.publisher.{SFlux, SMono}

import java.util.concurrent.{CompletableFuture, CompletionException, CompletionStage}
import java.util.function.Consumer
import scala.compat.java8.FutureConverters
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success}

/** Convert between Java and Scala async and reactive APIs.
  *
  * Note: Scala Monos are different classes to Java Monos
  *
  * @author Graham Pople
  * @since 1.0.0
  */
private[client] object FutureConversions {
  def javaCFToScalaFuture[T](future: CompletableFuture[T]): Future[T] = {
    future.toScala
  }

    def scalaFutureToJavaCF[T](future: Future[T]): CompletionStage[T] = {
        FutureConverters.toJava(future)
    }

    /** Performs logic similar to Java's AsyncUtil, mapping raw ExecutionExceptions into more useful ones.
    */
  def javaCFToScalaFutureMappingExceptions[T](
      future: CompletableFuture[T]
  )(implicit ec: ExecutionContext): Future[T] = {
    future.toScala
      .transform {
        case Success(result) =>
          Success(result)
        case Failure(err: InterruptedException) =>
          Failure(new RuntimeException(err))
        case Failure(err: CompletionException) =>
          err.getCause match {
            case cause: RuntimeException =>
              // Rethrow the cause but first adjust the stack trace to point HERE instead of
              // the thread where the exception was actually thrown, otherwise the stack trace
              // does not include the context of the blocking call.
              // Preserve the original async stack trace as a suppressed exception.
              val suppressed = new Exception(
                "The above exception was originally thrown by another thread at the following location."
              )
              suppressed.setStackTrace(cause.getStackTrace)
              cause.fillInStackTrace
              cause.addSuppressed(suppressed)
              Failure(cause)
            case cause: TimeoutException => Failure(new RuntimeException(cause))
            case x                       => Failure(x)
          }
        case Failure(err) => Failure(err)
      }
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

  def javaCFToScalaMono[T](response: CompletableFuture[T]): SMono[T] = {
    SMono(JavaMono.fromFuture(response))
  }

  def javaCFToScalaMono[T](
      request: Request[_],
      response: CompletableFuture[T],
      propagateCancellation: Boolean
  ): SMono[T] = {
    wrap(request, response, propagateCancellation)
  }

  def javaCFToJavaMono[T](
      request: Request[_],
      response: CompletableFuture[T],
      propagateCancellation: Boolean
  ): JavaMono[T] = {
    val javaMono = JavaMono.fromFuture(response)

    if (propagateCancellation) {
      javaMono.doFinally(new Consumer[SignalType] {
        override def accept(t: SignalType): Unit = {
          if (t == SignalType.CANCEL) {
            request.cancel(CancellationReason.STOPPED_LISTENING)
          }
        }
      })
    } else javaMono
  }

  /**
    * Wraps a `Request` and returns it in a `SMono`.
    *
    * @param request               the request to wrap.
    * @param response              the full response to wrap, might not be the same as in the request.
    * @param propagateCancellation if a cancelled/unsubscribed mono should also cancel the
    *                              request.
    *
    * @return the mono that wraps the request.
    */
  def wrap[T](
      request: Request[_ <: Response],
      response: CompletableFuture[T],
      propagateCancellation: Boolean
  ): SMono[T] = {
    val javaMono = JavaMono.fromFuture(response)
    val mono = {
      if (propagateCancellation) {
        SMono(javaMono).doFinally((st: SignalType) => {
          if (st == SignalType.CANCEL) request.cancel(CancellationReason.STOPPED_LISTENING)
        })
      } else SMono(javaMono)
    }

    mono.onErrorResume((err: Throwable) => {
      if (err.isInstanceOf[CompletionException]) SMono.error(err.getCause)
      else SMono.error(err)
    })
  }

    def scalaFutureToJavaMono[T](in: Future[T]): JavaMono[T] = {
        JavaMono.fromCompletionStage(scalaFutureToJavaCF(in))
    }
}
