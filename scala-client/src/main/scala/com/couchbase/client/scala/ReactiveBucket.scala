/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala

import reactor.core.scala.publisher.Mono

import scala.concurrent.ExecutionContext

/** Represents a Couchbase bucket resource.
  *
  * This is the reactive version of the [[Bucket]] API.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param name the name of this bucket
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  */
class ReactiveBucket(val async: AsyncBucket)
                    (implicit ec: ExecutionContext) {
  /** Opens and returns a Couchbase scope resource.
    *
    * @param name the name of the scope
    */
  def scope(name: String): Mono[ReactiveScope] = {
    Mono.fromFuture(async.scope(name)).map(v => new ReactiveScope(v, async.name))
  }

  /** Opens and returns the default Couchbase scope. */
  def defaultScope: Mono[ReactiveScope] = {
    scope(DefaultResources.DefaultScope)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: Mono[ReactiveCollection] = {
    scope(DefaultResources.DefaultScope).flatMap(v => v.defaultCollection)
  }

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collection the name of the collection
    * @return a created collection resource
    */
  def collection(name: String): Mono[ReactiveCollection] = {
    scope(DefaultResources.DefaultScope).flatMap(v => v.collection(name))
  }
}
