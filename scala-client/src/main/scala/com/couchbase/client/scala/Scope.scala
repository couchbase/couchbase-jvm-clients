package com.couchbase.client.scala

class Scope(val cluster: Cluster) {
  def openCollection(name: String): Collection = {
    new Collection(name, this)
  }
}

