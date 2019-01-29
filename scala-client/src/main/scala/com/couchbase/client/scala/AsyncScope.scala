package com.couchbase.client.scala

import java.time.Duration

import com.couchbase.client.core.Core
import com.couchbase.client.core.msg.kv.GetCollectionIdRequest
import com.couchbase.client.scala.env.ClusterEnvironment

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, Future}

class AsyncScope(val scopeName: String,
                 bucketName: String,
                 core: Core,
                 environment: ClusterEnvironment)
                (implicit ec: ExecutionContext) {
  def name = scopeName

  def defaultCollection(): Future[AsyncCollection] = collection(Defaults.DefaultCollection)

  def collection(name: String): Future[AsyncCollection] = {
    if (name == Defaults.DefaultCollection && scopeName == Defaults.DefaultScope) {
      Future {
        new AsyncCollection(name, Defaults.DefaultCollectionId, bucketName, core, environment)
      }
    }
    else {
      val request = new GetCollectionIdRequest(Duration.ofSeconds(1),
        core.context(), bucketName, environment.retryStrategy(), scopeName, name)
      core.send(request)
      FutureConverters.toScala(request.response())
        .map(res => {
          if (res.status().success()) {
            new AsyncCollection(name, res.collectionId().get(), bucketName, core, environment)
          } else {
            // TODO fix
            throw new IllegalStateException("Do not raise me.. propagate into collection.. " + "collection error")
          }
        })

    }
  }
}
