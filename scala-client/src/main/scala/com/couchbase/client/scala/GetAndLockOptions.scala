package com.couchbase.client.scala

import scala.concurrent.duration.FiniteDuration

case class GetAndLockOptions(timeout: FiniteDuration = null) {
  def timeout(timeout: FiniteDuration) = copy(timeout = timeout)
}

//case class GetAndLockOptions() {
//  private var timeout: FiniteDuration = null
//
//  def timeout(timeout: FiniteDuration): GetAndLockOptions = {
//    this.timeout = timeout
//    this
//  }
//
//  def build(): GetAndLockOptionsBuilt = GetAndLockOptionsBuilt(timeout)
//}

object GetAndLockOptions {
  def apply() = new GetAndLockOptions()
}