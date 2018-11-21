package com.couchbase.client.scala

class Bucket(cluster: Cluster,
             name: String) {
  def openScope(name: String) = new Scope(cluster.core, cluster, this, name)
}
