package com.couchbase.client.scala

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

case class ReplaceOptionsBuilt(timeout: FiniteDuration,
                              expiration: FiniteDuration,
                              replicateTo: ReplicateTo.Value,
                              persistTo: PersistTo.Value)

class ReplaceOptions() {
  private var timeout: FiniteDuration = null
  private var expiration: FiniteDuration = 0.seconds
  private var replicateTo: ReplicateTo.Value = ReplicateTo.NONE
  private var persistTo: PersistTo.Value = PersistTo.NONE

  def timeout(timeout: FiniteDuration): ReplaceOptions = {
    this.timeout = timeout
    this
  }

  def expiration(expiration: FiniteDuration): ReplaceOptions = {
    this.expiration = expiration
    this
  }

  def replicateTo(replicateTo: ReplicateTo.Value): ReplaceOptions = {
    this.replicateTo = replicateTo
    this
  }

  def persistTo(persistTo: PersistTo.Value): ReplaceOptions = {
    this.persistTo = persistTo
    this
  }

  def build(): ReplaceOptionsBuilt = ReplaceOptionsBuilt(timeout,
    expiration,
    replicateTo,
    persistTo)
}

object ReplaceOptions {
  def apply(): ReplaceOptions = new ReplaceOptions()
}




