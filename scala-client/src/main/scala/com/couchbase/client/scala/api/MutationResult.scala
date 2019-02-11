package com.couchbase.client.scala.api

import com.couchbase.client.core.msg.kv.MutationToken

trait HasDurabilityTokens {
  val cas: Long
  val mutationToken: Option[MutationToken]
}

case class MutationResult(cas: Long, mutationToken: Option[MutationToken]) extends HasDurabilityTokens
