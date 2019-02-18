package com.couchbase.client.scala

import reactor.core.scala.publisher.Mono

import scala.concurrent.ExecutionContext

class ReactiveScope(async: AsyncScope, bucketName: String)
                   (implicit ec: ExecutionContext){
  def defaultCollection(): Mono[ReactiveCollection] = {
    collection(Defaults.DefaultCollection)
  }

  def collection(name: String): Mono[ReactiveCollection] = {
    Mono.fromFuture(async.collection(name)).map(v => new ReactiveCollection(v))
  }
}
