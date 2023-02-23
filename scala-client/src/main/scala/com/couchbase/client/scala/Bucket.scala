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
import com.couchbase.client.core.diagnostics.{
  ClusterState,
  HealthPinger,
  PingResult
}
import com.couchbase.client.core.retry.{FailFastRetryStrategy, RetryStrategy}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.diagnostics.{PingOptions, WaitUntilReadyOptions}
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.manager.collection.CollectionManager
import com.couchbase.client.scala.manager.view.ViewIndexManager
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions, TimeoutUtil}
import com.couchbase.client.scala.util.DurationConversions.{
  javaDurationToScala,
  scalaDurationToJava
}
import com.couchbase.client.scala.view.{ViewOptions, ViewResult}

import scala.concurrent.{ExecutionContext, Future}
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

  /** Returns the name of this bucket. */
  def name: String = async.name

  /** Provides a reactive version of this API. */
  lazy val reactive: ReactiveBucket = new ReactiveBucket(async)

  lazy val collections = new CollectionManager(async.collections)

  lazy val viewIndexes = new ViewIndexManager(reactive.viewIndexes)

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collectionName the name of the collection
    * @return a created collection resource
    */
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
  def scope(scopeName: String): Scope = {
    new Scope(async.scope(scopeName), async.name)
  }

  /** Opens and returns the default Couchbase scope. */
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
    * @param options   any view query options - see [[com.couchbase.client.scala.view.ViewOptions]] for documentation
    *
    * @return a `Try` containing a `Success(ViewResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def viewQuery(
      designDoc: String,
      viewName: String,
      options: ViewOptions
  ): Try[ViewResult] = {
    AsyncUtils.block(async.viewQuery(designDoc, viewName, options))
  }

  /** Performs a view query against the cluster.
    *
    * This is synchronous (blocking).  See [[Bucket.reactive]] for a reactive streaming version of this API, and
    * [[Bucket.async]] for an async version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.view.ViewOptions]] instead, which supports all available options.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param timeout   how long the operation is allowed to take
    *
    * @return a `Try` containing a `Success(ViewResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def viewQuery(
      designDoc: String,
      viewName: String,
      timeout: Duration = async.environment.timeoutConfig.viewTimeout()
  ): Try[ViewResult] = {
    AsyncUtils.block(async.viewQuery(designDoc, viewName, timeout))
  }

  /** Performs application-level ping requests with custom options against services in the Couchbase cluster.
    *
    * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
    * not wish to perform I/O, consider using `.diagnostics()` instead.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.diagnostics.PingOptions]] instead, which supports all available options.
    *
    * @param timeout the timeout to use for the operation
    *
    * @return the `PingResult` once complete.
    */
  def ping(timeout: Option[Duration] = None): Try[PingResult] = {
    AsyncUtils.block(async.ping(timeout))
  }

  /** Performs application-level ping requests with custom options against services in the Couchbase cluster.
    *
    * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
    * not wish to perform I/O, consider using `.diagnostics()` instead.
    *
    * @param options options to customize the ping
    *
    * @return the `PingResult` once complete.
    */
  def ping(options: PingOptions): Try[PingResult] = {
    AsyncUtils.block(async.ping(options))
  }

  /** Waits until the desired `ClusterState` is reached.
    *
    * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
    * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
    * and usable before moving on.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.diagnostics.WaitUntilReadyOptions]] instead, which supports all available options.
    *
    * @param timeout the maximum time to wait until readiness.
    */
  def waitUntilReady(timeout: Duration): Try[Unit] = {
    AsyncUtils.block(async.waitUntilReady(timeout))
  }

  /** Waits until the desired `ClusterState` is reached.
    *
    * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
    * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
    * and usable before moving on.
    *
    * @param timeout the maximum time to wait until readiness.
    * @param options options to customize the wait
    */
  def waitUntilReady(timeout: Duration, options: WaitUntilReadyOptions): Try[Unit] = {
    AsyncUtils.block(async.waitUntilReady(timeout, options))
  }
}
