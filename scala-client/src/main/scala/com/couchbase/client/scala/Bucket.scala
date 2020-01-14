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
  PingResult,
  WaitUntilReadyHelper
}
import com.couchbase.client.core.retry.{FailFastRetryStrategy, RetryStrategy}
import com.couchbase.client.core.service.ServiceType
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

  @Stability.Volatile
  lazy val collections = new CollectionManager(reactive.collections)

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
    new Scope(async.scope(scopeName), async.name)
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
  def viewQuery(
      designDoc: String,
      viewName: String,
      options: ViewOptions = ViewOptions()
  ): Try[ViewResult] = {
    AsyncUtils.block(async.viewQuery(designDoc, viewName, options))
  }

  /**
    * Performs application-level ping requests with custom options against services in the Couchbase cluster.
    *
    * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
    * not wish to perform I/O, consider using the [[.diagnostics]] instead. You can also combine
    * the functionality of both APIs as needed, which is [[.waitUntilReady} is doing in its
    * implementation as well.
    *
    * @param reportId a custom report ID to be returned in the `PingResult`.  If none is provided, a unique one is
    *                 automatically generated.
    * @param serviceTypes the set of services to ping.  If empty, all possible services will be pinged.
    * @param timeout the timeout to use for the operation
    *
    * @return the `PingResult` once complete.
    */
  def ping(
      serviceTypes: Set[ServiceType] = Set(),
      reportId: Option[String] = None,
      timeout: Option[Duration] = None,
      retryStrategy: RetryStrategy = async.environment.retryStrategy
  ): Try[PingResult] = {
    AsyncUtils.block(async.ping(serviceTypes, reportId, timeout, retryStrategy))
  }

  /**
    * Waits until the desired `ClusterState` is reached.
    *
    * This method will wait until either the cluster state is "online", or the timeout is reached. Since the SDK is
    * bootstrapping lazily, this method allows to eagerly check during bootstrap if all of the services are online
    * and usable before moving on.
    *
    * @param timeout the maximum time to wait until readiness.
    * @param desiredState the cluster state to wait for, usually ONLINE.
    * @param serviceTypes the set of service types to check, if empty all services found in the cluster config will be
    *                     checked.
    */
  def waitUntilReady(
      timeout: Duration,
      desiredState: ClusterState = ClusterState.ONLINE,
      serviceTypes: Set[ServiceType] = Set()
  ): Try[Unit] = {
    AsyncUtils.block(async.waitUntilReady(timeout, desiredState, serviceTypes))
  }
}
