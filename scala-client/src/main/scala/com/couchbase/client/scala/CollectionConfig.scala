package com.couchbase.client.scala
import scala.concurrent.duration._

class CollectionConfig {
  def keyValueTimeout(): FiniteDuration = 1000.milliseconds
}

object CollectionConfig {
  def default() = new CollectionConfig()
}
