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

/**
  * Note: Scala Monos are different classes to Java Monos
  */
object FutureConversions {
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
