package com.couchbase.client.scala

import scala.concurrent.duration.FiniteDuration

case class GetOptions(timeout: FiniteDuration = null) {
  def timeout(timeout: FiniteDuration) = copy(timeout = timeout)
}
//
//case class GetOptions() {
//  private var timeout: FiniteDuration = null
//
//  def timeout(timeout: FiniteDuration): GetOptions = {
//    this.timeout = timeout
//    this
//  }
//
//  def build(): GetOptionsBuilt = GetOptionsBuilt(timeout)
//}

object GetOptions {
  def apply() = new GetOptions()
}