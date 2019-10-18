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

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.diag.PingResult
import com.couchbase.client.core.retry.{FailFastRetryStrategy, RetryStrategy}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.manager.view.ViewIndexManager
import com.couchbase.client.scala.util.AsyncUtils
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import com.couchbase.client.scala.view.{ViewOptions, ViewResult}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
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
  private[scala] val kvTimeout = javaDurationToScala(async.environment.timeoutConfig.kvTimeout())

  /** Returns the name of this bucket. */
  def name: String = async.name

  /** Provides a reactive version of this API. */
  lazy val reactive: ReactiveBucket = new ReactiveBucket(async)

  @Stability.Volatile
  lazy val viewIndexes = new ViewIndexManager(reactive.viewIndexes)

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collectionName the name of the collection
    * @return a created collection resource
    */
  @Stability.Volatile
  def collection(collectionName: String): Collection = {
    scope(DefaultResources.DefaultScope).collection(collectionName)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: Collection = {
    scope(DefaultResources.DefaultScope).defaultCollection
  }

  /** Opens and returns a Couchbase scope resource.
    *
    * @param scopeName the name of the scope
    */
  @Stability.Volatile
  def scope(scopeName: String): Scope = {
    AsyncUtils.block(async.scope(scopeName))
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

  /** Performs a diagnostic active "ping" call against one or more services.
    *
    * Note that since each service has different timeouts, you need to provide a timeout that suits
    * your needs (how long each individual service ping should take max before it times out).
    *
    * @param services        which services to ping.  Default (an empty Seq) means to fetch all of them.
    * @param reportId        this will be returned in the [[PingResult]].  If not specified it defaults to a UUID.
    * @param timeout         when the operation will timeout.  This will default to `timeoutConfig().kvTimeout()`
    *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
    * @param retryStrategy   provides some control over how the SDK handles failures.  Will default to
    *                        FailFastRetryStrategy.
    *
    * @return a ping report once created.
    */
  @Stability.Volatile
  def ping(services: Seq[ServiceType] = Seq(),
           reportId: String = UUID.randomUUID.toString,
           timeout: Duration = kvTimeout,
           retryStrategy: RetryStrategy = FailFastRetryStrategy.INSTANCE): Try[PingResult] = {
    AsyncUtils.block(async.ping(services, reportId, timeout, retryStrategy))
  }
}
