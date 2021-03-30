package com.couchbase.client.kotlin.kv

import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi

@VolatileCouchbaseApi
public open class MutateInMacro internal constructor(internal val value: String) {
    public object Cas : MutateInMacro("\${Mutation.CAS}")
    public object SeqNo : MutateInMacro("\${Mutation.seqno}")
    public object ValueCrc32c : MutateInMacro("\${Mutation.value_crc32c}")

    override fun toString(): String {
        return "MutateInMacro(value='$value')"
    }
}
