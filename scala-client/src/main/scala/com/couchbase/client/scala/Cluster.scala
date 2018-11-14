package com.couchbase.client.scala

class Bucket(val cluster: Cluster) {
  def openScope(name: String) = new Scope(cluster)
}

class Cluster(val env: CouchbaseEnvironment,
              val name: String) {
  def authenticate(username: String, password: String) = null
  def openBucket(name: String) = new Bucket(this)
}

object CouchbaseCluster {
  def create(env: CouchbaseEnvironment, name: String = "localhost"): Cluster = {
    new Cluster(env, name)
  }
}