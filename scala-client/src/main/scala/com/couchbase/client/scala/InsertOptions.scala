package com.couchbase.client.scala

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

case class InsertOptions(timeout: FiniteDuration = null,
                              expiration: FiniteDuration = 0.seconds,
                              replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
                              persistTo: PersistTo.Value = PersistTo.NONE) {
    def timeout(timeout: FiniteDuration): InsertOptions = copy(timeout = timeout)
    def expiration(expiration: FiniteDuration): InsertOptions = copy(expiration = expiration)
    def replicateTo(replicateTo: ReplicateTo.Value): InsertOptions = copy(replicateTo = replicateTo)
    def persistTo(persistTo: PersistTo.Value): InsertOptions = copy(persistTo = persistTo)
}

//case class InsertOptionsBuilt(timeout: FiniteDuration,
//                              expiration: FiniteDuration,
//                              replicateTo: ReplicateTo.Value,
//                              persistTo: PersistTo.Value)
//
//class InsertOptions() {
//  private var timeout: FiniteDuration = null
//  private var expiration: FiniteDuration = 0.seconds
//  private var replicateTo: ReplicateTo.Value = ReplicateTo.NONE
//  private var persistTo: PersistTo.Value = PersistTo.NONE
//
//  def timeout(timeout: FiniteDuration): InsertOptions = {
//    this.timeout = timeout
//    this
//  }
//
//  def expiration(expiration: FiniteDuration): InsertOptions = {
//    this.expiration = expiration
//    this
//  }
//
//  def replicateTo(replicateTo: ReplicateTo.Value): InsertOptions = {
//    this.replicateTo = replicateTo
//    this
//  }
//
//  def persistTo(persistTo: PersistTo.Value): InsertOptions = {
//    this.persistTo = persistTo
//    this
//  }
//
//  def build(): InsertOptionsBuilt = InsertOptionsBuilt(timeout,
//    expiration,
//    replicateTo,
//    persistTo)
//}
//

object InsertOptions {
  def apply(): InsertOptions = new InsertOptions()
}

