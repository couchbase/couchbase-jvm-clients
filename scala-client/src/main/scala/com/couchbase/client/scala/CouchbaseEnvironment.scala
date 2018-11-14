package com.couchbase.client.scala
import scala.concurrent.duration._

class CouchbaseEnvironment {
  def keyValueTimeout(): FiniteDuration = 1000.milliseconds
}

object CouchbaseDefaultEnvironment {
  def create() = new CouchbaseEnvironment()
}
