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

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.scala.util.AsyncUtils
import com.couchbase.client.scala.view.{ViewOptions, ViewResult}

import scala.concurrent.ExecutionContext
import scala.util.Try

/** Represents a Couchbase bucket resource.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param async provides an asynchronous version of this interface
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  * @author Graham Pople
  * @since 1.0.0
  */
class Bucket private[scala] (val async: AsyncBucket) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  /** Returns the name of this bucket. */
  def name: String = async.name

  /** Provides a reactive version of this API. */
  lazy val reactive: ReactiveBucket = new ReactiveBucket(async)

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collection the name of the collection
    * @return a created collection resource
    */
  @Stability.Volatile
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
  @Stability.Volatile
  def scope(name: String): Scope = {
    AsyncUtils.block(async.scope(name))
      .map(asyncScope => new Scope(asyncScope, async.name))
      .get
  }

  /** Opens and returns the default Couchbase scope. */
  @Stability.Volatile
  def defaultScope: Scope = {
    scope(DefaultResources.DefaultScope)
  }

  /** Performs a view query against the cluster.
    *
    * This is synchronous (blocking).  See [[Bucket.reactive]] for a reactive streaming version of this API, and
    * [[Bucket.async]] for an async version.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param options   any view query options - see [[ViewOptions]] for documentation
    *
    * @return a `Try` containing a `Success(ViewResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def viewQuery(designDoc: String,
                viewName: String,
                options: ViewOptions = ViewOptions()): Try[ViewResult] = {
  AsyncUtils.block(async.viewQuery(designDoc, viewName, options))
  }

}
