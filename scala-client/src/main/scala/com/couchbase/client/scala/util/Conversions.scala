package com.couchbase.client.scala.util

import reactor.core.publisher.Mono

import scala.compat.java8.FutureConverters
import scala.concurrent.Future

object Conversions {
  def monoToFuture[T](in: Mono[T]): Future[T] = {
    FutureConverters.toScala(in.toFuture)
  }
}
