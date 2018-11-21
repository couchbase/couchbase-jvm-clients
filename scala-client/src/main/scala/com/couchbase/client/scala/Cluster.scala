package com.couchbase.client.scala

import java.util

import com.couchbase.client.core.Core
import com.couchbase.client.core.env.CoreEnvironment
import com.couchbase.client.core.io.NetworkAddress
import com.couchbase.client.scala.query.{N1qlQueryResult, N1qlResult}

// TODO
class Cluster(env: CouchbaseEnvironment, node: String) {
  private val coreEnv = CoreEnvironment.create()
  private val networkSet = new util.HashSet[NetworkAddress]()
  networkSet.add(NetworkAddress.create(node))
  val core = Core.create(coreEnv, networkSet)

  def openBucket(name: String) = new Bucket(this, name)

  def query(statement: String, query: QueryOptions = QueryOptions()): N1qlQueryResult = {
    null
  }

  def queryAs[T](statement: String, query: QueryOptions = QueryOptions()): N1qlResult[T] = {
    null
  }
}

object CouchbaseCluster {
  def create(node: String) = {
    new Cluster(CouchbaseEnvironment.default(), node)
  }
}
