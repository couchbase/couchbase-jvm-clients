package com.couchbase.client.scala

import com.couchbase.client.core.Core
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.util.Conversions

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

class AsyncCluster(environment: => ClusterEnvironment)
                  (implicit ec: ExecutionContext) {
  private val core = Core.create(environment)

  // TODO MVP other credentials
  // TODO MVP query
  // TODO MVP shutdown

  def bucket(name: String): Future[AsyncBucket] = {
    Conversions.monoToFuture(core.openBucket(name))
      .map(v => new AsyncBucket(name, core, environment))
  }
}

object AsyncCluster {
//  def connect(username: String, password: String) = {
//    AsyncCluster(() => ClusterEnvironment.create(usernmae, password))
//  }
}