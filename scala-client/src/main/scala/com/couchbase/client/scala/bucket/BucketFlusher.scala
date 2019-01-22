package com.couchbase.client.scala.bucket

import com.couchbase.client.core.Core

// TODO this makes more sense in core
object BucketFlusher {
  private val NumFlushMarkers = 1024
  private val FlushMarkerIds: Seq[String] = Range(0, NumFlushMarkers).map("__flush_marker_" + _)

  def flush(core: Core, bucket: String, username: String, password: String) = ???

  private def createMarkerDocuments(core: Core, bucket: String) = {
      FlushMarkerIds.foreach(fmId => {

      })
  }
}
