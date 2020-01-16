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
import com.couchbase.client.core.diagnostics.{HealthPinger, PingResult, WaitUntilReadyHelper}
import com.couchbase.client.scala.diagnostics.{PingOptions, WaitUntilReadyOptions}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.manager.collection.AsyncCollectionManager
import com.couchbase.client.scala.manager.view.AsyncViewIndexManager
import com.couchbase.client.scala.util.DurationConversions.{scalaDurationToJava, _}
import com.couchbase.client.scala.util.FutureConversions
import com.couchbase.client.scala.view.{ViewOptions, ViewResult}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** Represents a Couchbase bucket resource.
  *
  * This is the asynchronous version of the [[Bucket]] API.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param name the name of this bucket
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
    * @param options   any view query options - see [[com.couchbase.client.scala.view.ViewOptions]] for documentation
    *
    * @return a `Future` containing a `Success(ViewResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def viewQuery(
      designDoc: String,
      viewName: String,
      options: ViewOptions
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

  /** Performs a view query against the cluster.
    *
    * This is asynchronous.  See [[Bucket.reactive]] for a reactive streaming version of this API, and
    * [[Bucket]] for a blocking version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.view.ViewOptions]] instead, which supports all available options.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param timeout   how long the operation is allowed to take
    *
    * @return a `Future` containing a `Success(ViewResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def viewQuery(
      designDoc: String,
      viewName: String,
      timeout: Duration = environment.timeoutConfig.viewTimeout()
  ): Future[ViewResult] = {
    viewQuery(designDoc, viewName, ViewOptions(timeout = Some(timeout)))
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
    * @return the PingResult once complete.
    */
  def ping(timeout: Option[Duration] = None): Future[PingResult] = {
    var opts = PingOptions()
    timeout.foreach(v => opts = opts.timeout(v))
    ping(opts)
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
  def ping(options: PingOptions): Future[PingResult] = {

    val future = HealthPinger
      .ping(
        core,
        options.timeout.map(scalaDurationToJava).asJava,
        options.retryStrategy.getOrElse(environment.retryStrategy),
        if (options.serviceTypes.isEmpty) null else options.serviceTypes.asJava,
        options.reportId.asJava,
        false
      )
      .toFuture

    FutureConversions.javaCFToScalaFuture(future)
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
  def waitUntilReady(timeout: Duration): Future[Unit] = {
    waitUntilReady(timeout, WaitUntilReadyOptions())
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
  def waitUntilReady(timeout: Duration, options: WaitUntilReadyOptions): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFuture(
        WaitUntilReadyHelper.waitUntilReady(
          core,
          if (options.serviceTypes.isEmpty) null else options.serviceTypes.asJava,
          timeout,
          options.desiredState,
          false
        )
      )
      .map(_ => ())
  }
}
