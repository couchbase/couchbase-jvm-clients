package com.couchbase.client.scala

object ReplicateTo extends Enumeration {
  val NONE, ONE, TWO, THREE, ALL, MAJORITY = Value
}

object PersistTo extends Enumeration {
  val NONE, ONE, TWO, THREE, ALL, MAJORITY = Value
}
