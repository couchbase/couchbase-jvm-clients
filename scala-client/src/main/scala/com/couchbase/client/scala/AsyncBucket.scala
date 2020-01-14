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

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.diagnostics.{
  ClusterState,
  HealthPinger,
  PingResult,
  WaitUntilReadyHelper
}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.manager.collection.AsyncCollectionManager
import com.couchbase.client.scala.manager.view.AsyncViewIndexManager
import com.couchbase.client.scala.util.DurationConversions.scalaDurationToJava
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import com.couchbase.client.scala.view.{ViewOptions, ViewResult}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import com.couchbase.client.scala.util.DurationConversions._

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
            response.metaData.map(meta => ViewResult(meta, rows))
          })
      })
      .toFuture
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
      retryStrategy: RetryStrategy = environment.retryStrategy
  ): Future[PingResult] = {

    import scala.collection.JavaConverters._

    val future = HealthPinger
      .ping(
        core,
        timeout.map(scalaDurationToJava).asJava,
        retryStrategy,
        if (serviceTypes.isEmpty) null else serviceTypes.asJava,
        reportId.asJava,
        false
      )
      .toFuture

    FutureConversions.javaCFToScalaFuture(future)
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
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFuture(
        WaitUntilReadyHelper.waitUntilReady(
          core,
          if (serviceTypes.isEmpty) null else serviceTypes.asJava,
          timeout,
          desiredState,
          false
        )
      )
      .map(_ => Unit)
  }
}
