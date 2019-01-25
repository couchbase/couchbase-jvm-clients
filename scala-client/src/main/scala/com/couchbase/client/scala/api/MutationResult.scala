package com.couchbase.client.scala.api

import com.couchbase.client.core.msg.kv.MutationToken

//case class MutationToken(sequenceNumber: Int,
//                         vbucketId: Long,
//                         vbucketUUID: Long)
case class MutationResult(cas: Long, mutationToken: Option[MutationToken])
