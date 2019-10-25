package com.couchbase.client.scala.kv

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.scala.api.MutationResult

/** Represents the tokens from one or more mutations. */
case class MutationState(tokens: Seq[MutationToken]) {

  /** Add the token from a [[MutationResult]], if it's present, to this.
    *
    * @return a copy of this, for chaining
    */
  def add(mutationResult: MutationResult): MutationState = {
    mutationResult.mutationToken match {
      case Some(mt) => copy(tokens :+ mt)
      case _        => this
    }
  }

  /** Add the tokens from another MutationState to this.
    *
    * @return a copy of this, for chaining
    */
  def add(mutationState: MutationState): MutationState = {
    copy(tokens ++ mutationState.tokens)
  }
}

object MutationState {

  /** Create a new MutationState representing all the tokens from the provided [[MutationResult]]s. */
  def from(result: MutationResult*): MutationState = {
    MutationState(result.map(_.mutationToken).filter(_.isDefined).map(_.get))
  }
}
