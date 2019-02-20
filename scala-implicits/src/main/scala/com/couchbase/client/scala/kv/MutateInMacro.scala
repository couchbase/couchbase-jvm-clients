package com.couchbase.client.scala.kv

sealed trait MutateInMacro {
  private[scala] def value: String
}

object MutateInMacro {

  case object MutationCAS extends MutateInMacro {
    private[scala] val value = "${Mutation.CAS}"
  }

}

