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

import java.util.UUID

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.retry.{FailFastRetryStrategy, RetryStrategy}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.manager.collection.{AsyncCollectionManager, CollectionManager}
import com.couchbase.client.scala.manager.view.{AsyncViewIndexManager}
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import com.couchbase.client.scala.util.FutureConversions
import com.couchbase.client.scala.view.{ViewOptions, ViewResult}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** Represents a Couchbase bucket resource.
  *
  * This is the asynchronous version of the [[Bucket]] API.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param name the name of this bucket
  * @param ec   an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *             opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */
class AsyncBucket private[scala] (
    val name: String,
    private[scala] val core: Core,
    private[scala] val environment: ClusterEnvironment
) {
  private[scala] implicit val ec: ExecutionContext = environment.ec
  private[scala] val kvTimeout                     = javaDurationToScala(environment.timeoutConfig.kvTimeout())
  val reactive                                     = new ReactiveBucket(this)

  @Stability.Volatile
  lazy val collections = new AsyncCollectionManager(reactive.collections)

  @Stability.Volatile
  lazy val viewIndexes = new AsyncViewIndexManager(reactive.viewIndexes)

  /** Opens and returns a Couchbase scope resource.
    *
    * @param scopeName the name of the scope
    */
  @Stability.Volatile
  def scope(scopeName: String): AsyncScope = {
    new AsyncScope(scopeName, name, core, environment)
  }

  /** Opens and returns the default Couchbase scope. */
  @Stability.Volatile
  def defaultScope: AsyncScope = {
    scope(DefaultResources.DefaultScope)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: AsyncCollection = {
    scope(DefaultResources.DefaultScope).defaultCollection
  }

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collectionName the name of the collection
    *
    * @return a created collection resource
    */
  @Stability.Volatile
  def collection(collectionName: String): AsyncCollection = {
    scope(DefaultResources.DefaultScope).collection(collectionName)
  }

  /** Performs a view query against the cluster.
    *
    * This is asynchronous.  See [[Bucket.reactive]] for a reactive streaming version of this API, and
    * [[Bucket]] for a blocking version.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param options   any view query options - see [[ViewOptions]] for documentation
    *
    * @return a `Future` containing a `Success(ViewResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def viewQuery(
      designDoc: String,
      viewName: String,
      options: ViewOptions = ViewOptions()
  ): Future[ViewResult] = {
    reactive
      .viewQuery(designDoc, viewName, options)
      .flatMap(response => {
        response.rows
          .collectSeq()
          .flatMap(rows => {
            response.meta.map(meta => ViewResult(meta, rows))
          })
      })
      .toFuture
  }

}
