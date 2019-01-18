package com.couchbase.client.scala.util

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Try}
import scala.concurrent.duration._

object AsyncUtils {
  // TODO what should this be?
  private val DefaultTimeout = 10.seconds

  def block[A](in: Future[A], timeout: FiniteDuration = DefaultTimeout): Try[A] = {
    Try(Await.result(in, timeout))
  }
}
