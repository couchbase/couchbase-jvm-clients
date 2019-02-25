package com.couchbase.client.scala.kv

import com.couchbase.client.scala.json.JsonObject

import scala.concurrent.duration.Duration

sealed trait Replica
object Replica {
  case object Master extends Replica
  case object ReplicaOne extends Replica
  case object ReplicaTwo extends Replica
  case object ReplicaThree extends Replica
}

class GetFromReplicaResult(id: String,
                           // It's Right only in the case where projections were requested
                           _content: Either[Array[Byte], JsonObject],
                           flags: Int,
                           cas: Long,
                           expiration: Option[Duration],
                           val replica: Replica) extends GetResult(id, _content, flags, cas, expiration)
