package com.couchbase.client.scala

import java.util.UUID

object TestUtils {
  def docId(idx: Int = 0): String = {
    // This used to use Thread.getStackTrace to generate a good test-dependent name, but it doesn't work with ScalaTest
    // due to anonymous names
    UUID.randomUUID().toString
  }
}
