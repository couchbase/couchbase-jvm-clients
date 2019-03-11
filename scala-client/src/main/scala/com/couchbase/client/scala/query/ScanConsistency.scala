package com.couchbase.client.scala.query

import scala.concurrent.duration.Duration

sealed trait ScanConsistency {
  private[scala] def encoded: String
}

object ScanConsistency {

  case object NotBounded extends ScanConsistency {
    private[scala] def encoded = "not_bounded"
  }

  //case class AtPlus(consistentWith: List[MutationToken], scanWait: Option[Duration] = None) extends ScanConsistency

  case class RequestPlus(scanWait: Option[Duration] = None) extends ScanConsistency {
    private[scala] def encoded = "request_plus"
  }
}