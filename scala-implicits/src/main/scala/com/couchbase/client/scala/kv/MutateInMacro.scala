package com.couchbase.client.scala.kv

sealed trait MutateInMacro {
  private[scala] def value: String
}

object MutateInMacro {

  case object MutationCAS extends MutateInMacro {
    private[scala] val value = """"${Mutation.CAS}""""
  }

  case object MutationSeqNo extends MutateInMacro {
    private[scala] val value = """"${Mutation.seqno}""""
  }

  case object MutationCrc32c extends MutateInMacro {
    private[scala] val value = """"${Mutation.value_crc32c}""""
  }
}

