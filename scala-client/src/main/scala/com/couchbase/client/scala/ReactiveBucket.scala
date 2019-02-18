package com.couchbase.client.scala

import reactor.core.scala.publisher.Mono

import scala.concurrent.ExecutionContext

class ReactiveBucket(val async: AsyncBucket)
                    (implicit ec: ExecutionContext) {
  def scope(name: String): Mono[ReactiveScope] = {
    Mono.fromFuture(async.scope(name)).map(v => new ReactiveScope(v, async.name))
  }

  def defaultCollection(): Mono[ReactiveCollection] = {
    scope(Defaults.DefaultScope).flatMap(v => v.defaultCollection())
  }

  def collection(name: String): Mono[ReactiveCollection] = {
    scope(Defaults.DefaultScope).flatMap(v => v.collection(name))
  }
}
