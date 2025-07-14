/*
 * Copyright (c) 2025 Couchbase, Inc.
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

import java.util.concurrent.{
  CompletableFuture,
  CompletionException,
  CompletionStage,
  ExecutionException
}
import java.util.function.Consumer
import com.couchbase.client.core.msg.{CancellationReason, Request, Response}
import reactor.core.publisher.{SignalType, Flux as JavaFlux, Mono as JavaMono}

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.jdk.FutureConverters.*
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
    future.asScala
  }

  def scalaFutureToJavaCF[T](future: Future[T]): CompletionStage[T] = {
    future.asJava
  }

  /** Performs logic similar to Java's AsyncUtil, mapping raw ExecutionExceptions into more useful ones.
    */
  def javaCFToScalaFutureMappingExceptions[T](
      future: CompletableFuture[T]
  )(implicit ec: ExecutionContext): Future[T] = {
    future.asScala
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
    in.toFuture.asScala
  }

  def scalaFutureToJavaMono[T](in: Future[T]): JavaMono[T] = {
    JavaMono.fromCompletionStage(scalaFutureToJavaCF(in))
  }
}
