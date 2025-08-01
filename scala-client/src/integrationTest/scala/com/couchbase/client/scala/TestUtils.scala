package com.couchbase.client.scala

import com.couchbase.client.core.CoreProtostellar

import java.util.UUID
import com.couchbase.client.core.diagnostics.PingState
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.diagnostics.PingOptions
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.test.Util

import scala.util.{Failure, Success}

object TestUtils {
  def docId(idx: Int = 0): String = {
    // This used to use Thread.getStackTrace to generate a good test-dependent name, but it doesn't work with ScalaTest
    // due to anonymous names
    UUID.randomUUID().toString
  }

  /** Wait until ping result for the service returns success */
  def waitForService(bucket: Bucket, serviceType: ServiceType): Unit = {
    // Now a no-op as bucket.ping removed in Scala 3
  }

  /** Wait for indexer to be aware of a (possibly newly created) bucket or collection */
  def waitForIndexerToHaveKeyspace(cluster: Cluster, keyspaceName: String): Unit = {
    println(s"Waiting for indexer to be aware of bucket ${keyspaceName}")

    var ready = false
    var guard = 100

    while (!ready && guard != 0) {
      guard -= 1
      val statement =
        s"""SELECT COUNT(*) > 0 as present FROM system:keyspaces where name = "${keyspaceName}";"""

      cluster.query(statement) match {
        case Success(result) =>
          println(s"Got result: ${result}")
          if (result.rows.size == 1 && result.rowsAs[JsonObject].get.head.bool("present")) {
            ready = true
          }
        case Failure(err) =>
          println(s"Failure getting indexer status: $err")
      }

      if (!ready) {
        Thread.sleep(50)
      }
    }
  }

  def waitForNsServerToBeReady(cluster: Cluster): Unit = {
    // Now a no-op as user management removed in Scala 3
  }
}
