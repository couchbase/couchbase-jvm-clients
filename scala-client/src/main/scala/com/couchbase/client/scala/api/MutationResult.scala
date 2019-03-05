package com.couchbase.client.scala.api

import com.couchbase.client.core.msg.kv.MutationToken

private[scala] trait HasDurabilityTokens {
  val cas: Long
  val mutationToken: Option[MutationToken]
}

/** Contains the result of a successful mutation.
  *
  * @param cas           each Couchbase document has a CAS value, which is increased (not necessarily monotonically)
  *                      on each
  *                      successful mutation.  This is the updated post-mutation value.
  * @param mutationToken if the [[com.couchbase.client.scala.env.ClusterEnvironment]]'s `ioConfig()
  *                      .mutationTokensEnabled()` field is true (which is recommended), this will contain a
  *                      `MutationToken` providing additional context on the mutation.
  * @author Graham Pople
  * @since 1.0.0
  */
case class MutationResult(cas: Long, mutationToken: Option[MutationToken]) extends HasDurabilityTokens

case class CounterResult(cas: Long,
                         mutationToken: Option[MutationToken],
                         content: Long) extends HasDurabilityTokens
