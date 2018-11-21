package com.couchbase.client.scala

import scala.concurrent.duration.{FiniteDuration, _}

case class RemoveOptions(timeout: FiniteDuration = null,
                               replicateTo: ReplicateTo.Value = ReplicateTo.NONE,
                               persistTo: PersistTo.Value = PersistTo.NONE) {
  def timeout(timeout: FiniteDuration): RemoveOptions = copy(timeout = timeout)
  def replicateTo(replicateTo: ReplicateTo.Value): RemoveOptions = copy(replicateTo = replicateTo)
  def persistTo(persistTo: PersistTo.Value): RemoveOptions = copy(persistTo = persistTo)
}

object RemoveOptions {
  def apply(): RemoveOptions = new RemoveOptions()
}

case class RemoveResult(cas: Long, mutationToken: Option[MutationToken])