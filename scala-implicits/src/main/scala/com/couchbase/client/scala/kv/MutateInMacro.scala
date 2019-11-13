package com.couchbase.client.scala.kv

sealed trait MutateInMacro {
  private[scala] def value: String
}

object MutateInMacro {

  case object CAS extends MutateInMacro {
    private[scala] val value = """"${Mutation.CAS}""""
  }

  case object SeqNo extends MutateInMacro {
    private[scala] val value = """"${Mutation.seqno}""""
  }

  case object ValueCrc32c extends MutateInMacro {
    private[scala] val value = """"${Mutation.value_crc32c}""""
  }
}

