package com.couchbase.client.scala

import com.couchbase.client.core.Core
import com.couchbase.client.scala.env.ClusterEnvironment

import scala.concurrent.{ExecutionContext, Future}

class AsyncBucket(val name: String,
                  private[scala] val core: Core,
                  private[scala] val environment: ClusterEnvironment)
                 (implicit ec: ExecutionContext) {
  def scope(scope: String): Future[AsyncScope] = {
    Future {
      new AsyncScope(scope, name, core, environment)
    }
  }

  def defaultCollection(): Future[AsyncCollection] = {
    scope(Defaults.DefaultScope).flatMap(_.defaultCollection())
  }

  def collection(collection: String): Future[AsyncCollection] = {
    scope(Defaults.DefaultScope).flatMap(_.collection(collection))
  }
}



