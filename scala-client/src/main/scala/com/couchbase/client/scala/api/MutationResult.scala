package com.couchbase.client.scala.api

case class MutationToken(sequenceNumber: Int,
                         vbucketId: Long,
                         vbucketUUID: Long)
case class MutationResult(cas: Long, mutationToken: Option[MutationToken])
