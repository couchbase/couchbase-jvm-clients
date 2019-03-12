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

import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext

/** Represents a Couchbase bucket resource.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param async provides an asynchronous version of this interface
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  */
class Bucket private(val async: AsyncBucket)
            (implicit ec: ExecutionContext) {
  /** Returns the name of this bucket. */
  def name: String = async.name

  /** Provides a reactive version of this API. */
  lazy val reactive: ReactiveBucket = new ReactiveBucket(async)

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collection the name of the collection
    * @return a created collection resource
    */
  def collection(collection: String): Collection = {
    scope(DefaultResources.DefaultScope).collection(collection)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: Collection = {
    scope(DefaultResources.DefaultScope).defaultCollection
  }

  /** Opens and returns a Couchbase scope resource.
    *
    * @param name the name of the scope
    */
  def scope(name: String): Scope = {
    AsyncUtils.block(async.scope(name))
      .map(asyncScope => new Scope(asyncScope, async.name))
      .get
  }

  /** Opens and returns the default Couchbase scope. */
  def defaultScope: Scope = {
    scope(DefaultResources.DefaultScope)
  }
}
