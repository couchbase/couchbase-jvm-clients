package com.couchbase.client.scala.manager.user

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.scala.util.CouchbasePickler


sealed trait AuthDomain {
  def alias: String
}

object AuthDomain {

  @Volatile
  case object Local extends AuthDomain {
    def alias: String = "local"
  }

  @Volatile
  case object External extends AuthDomain {
    def alias: String = "external"
  }

  implicit val rw: CouchbasePickler.ReadWriter[AuthDomain] = CouchbasePickler.readwriter[String].bimap[AuthDomain](
    x => x.alias,
    str => {
      str match {
        case "local" => Local
        case "external" => External
      }
    }
  )
}

