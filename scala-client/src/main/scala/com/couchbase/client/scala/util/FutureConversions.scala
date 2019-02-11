package com.couchbase.client.scala.util


import java.util.concurrent.CompletableFuture

import com.couchbase.client.core.msg.{CancellationReason, Request}
import reactor.core.publisher.SignalType

import scala.compat.java8.FutureConverters
import scala.concurrent.Future

import reactor.core.publisher.{Mono => JavaMono}
import reactor.core.scala.publisher.{Mono => ScalaMono}

/**
  * Note: Scala Monos are different classes to Java Monos
  */
object FutureConversions {
  def javaMonoToScalaFuture[T](in: JavaMono[T]): Future[T] = {
    FutureConverters.toScala(in.toFuture)
  }

  def javaMonoToScalaMono[T](in: JavaMono[T]): ScalaMono[T] = {
    ScalaMono.from(in)
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
}
