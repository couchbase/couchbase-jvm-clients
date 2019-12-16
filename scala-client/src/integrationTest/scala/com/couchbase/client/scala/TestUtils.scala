package com.couchbase.client.scala

import java.util.UUID

import com.couchbase.client.core.service.ServiceType

import scala.util.{Failure, Success}

object TestUtils {
  def docId(idx: Int = 0): String = {
    // This used to use Thread.getStackTrace to generate a good test-dependent name, but it doesn't work with ScalaTest
    // due to anonymous names
    UUID.randomUUID().toString
  }

  def waitForService(bucket: Bucket, serviceType: ServiceType): Unit = {
    var done  = false
    var guard = 100

    /*while (!done && guard != 0) {
      guard -= 1
      bucket.ping(Seq(serviceType)) match {
        case Success(result) =>
          println(s"Ping: ${result}")
          if (!result.services().isEmpty && result.services().get(0).state() == PingState.OK) {
            done = true
          }
        case Failure(err) => println("Ping failed: " + err)
      }

      if (!done) {
        Thread.sleep(100)
      }
    }*/

  }
}
