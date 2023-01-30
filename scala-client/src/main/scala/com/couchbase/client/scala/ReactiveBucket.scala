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

import com.couchbase.client.core.diagnostics.PingResult
import com.couchbase.client.core.msg.view.ViewRequest
import com.couchbase.client.scala.diagnostics.{PingOptions, WaitUntilReadyOptions}
import com.couchbase.client.scala.manager.collection.ReactiveCollectionManager
import com.couchbase.client.scala.manager.view.ReactiveViewIndexManager
import com.couchbase.client.scala.query.handlers.ViewHandler
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import com.couchbase.client.scala.util.FutureConversions
import com.couchbase.client.scala.view._
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/** Represents a Couchbase bucket resource.
  *
  * This is the reactive version of the [[Bucket]] API.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param async the async bucket reference
  * @param ec   an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *             opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */
class ReactiveBucket private[scala] (val async: AsyncBucket) {
  private[scala] implicit val ec: ExecutionContext = async.ec
  private[scala] val hp                            = HandlerBasicParams(async.core, async.environment)
  private[scala] val viewHandler                   = new ViewHandler(hp)

  lazy val collections = new ReactiveCollectionManager(async.collections)

  lazy val viewIndexes = new ReactiveViewIndexManager(async.core, async.name)

  /** Opens and returns a Couchbase scope resource.
    *
    * @param scopeName the name of the scope
    */
  def scope(scopeName: String): ReactiveScope = {
    new ReactiveScope(async.scope(scopeName), async.name)
  }

  /** Opens and returns the default Couchbase scope. */
  def defaultScope: ReactiveScope = {
    scope(DefaultResources.DefaultScope)
  }

  /** Returns the Couchbase default collection resource. */
  def defaultCollection: ReactiveCollection = {
    scope(DefaultResources.DefaultScope).defaultCollection
  }

  /** Opens a Couchbase collection resource on the default scope.
    *
    * @param collectionName the name of the collection
    *
    * @return a created collection resource
    */
  def collection(collectionName: String): ReactiveCollection = {
    scope(DefaultResources.DefaultScope).collection(collectionName)
  }

  /** Performs a view query against the cluster.
    *
    * This is a reactive streaming version of this API.  See [[Bucket]] for a synchronous blocking version, and
    * [[Bucket.async]] for an async version.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param options   any view query options - see [[view.ViewOptions]] for documentation
    *
    * @return a `Mono` containing a [[view.ViewResult]] (which includes any returned rows)
    */
  def viewQuery(
      designDoc: String,
      viewName: String,
      options: ViewOptions
  ): SMono[ReactiveViewResult] = {
    val req =
      viewHandler.request(designDoc, viewName, options, async.core, async.environment, async.name)
    viewQuery(req)
  }

  /** Performs a view query against the cluster.
    *
    * This is a reactive streaming version of this API.  See [[Bucket]] for a synchronous blocking version, and
    * [[Bucket.async]] for an async version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.view.ViewOptions]] instead, which supports all available options.
    *
    * @param designDoc the view design document to use
    * @param viewName  the view to use
    * @param timeout   how long the operation is allowed to take
    *
    * @return a `Mono` containing a [[view.ViewResult]] (which includes any returned rows)
    */
  def viewQuery(
      designDoc: String,
      viewName: String,
      timeout: Duration = async.environment.timeoutConfig.viewTimeout()
  ): SMono[ReactiveViewResult] = {
    viewQuery(designDoc, viewName, ViewOptions(timeout = Some(timeout)))
  }

  private def viewQuery(req: Try[ViewRequest]): SMono[ReactiveViewResult] = {
    req match {
      case Failure(err) =>
        SMono.error(err)

      case Success(request) =>
        SMono.defer(() => {
          async.core.send(request)

          FutureConversions
            .javaCFToScalaMono(request, request.response(), false)
            .map(response => {

              val rows: SFlux[ViewRow] = FutureConversions
                .javaFluxToScalaFlux(response.rows())
                .map[ViewRow](bytes => ViewRow(bytes.data()))

              val meta = ViewMetaData(
                response.header().debug().asScala,
                response.header().totalRows()
              )

              ReactiveViewResult(SMono.just(meta), rows)
            })
            .doOnNext(_ => request.context.logicallyComplete)
            .doOnError(err => request.context().logicallyComplete(err))
        })
    }
  }

  /** Performs application-level ping requests with custom options against services in the Couchbase cluster.
    *
    * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
    * not wish to perform I/O, consider using the [[.diagnostics]] instead.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.diagnostics.PingOptions]] instead, which supports all available options.
    *
    * @param timeout the timeout to use for the operation
    *
    * @return the `PingResult` once complete.
    */
  def ping(timeout: Option[Duration] = None): SMono[PingResult] = {
    SMono.defer(() => SMono.fromFuture(async.ping(timeout)))
  }

  /** Performs application-level ping requests with custom options against services in the Couchbase cluster.
    *
    * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
    * not wish to perform I/O, consider using the [[.diagnostics]] instead.
    *
    * @param options options to customize the ping
    *
    * @return the `PingResult` once complete.
    */
  def ping(options: PingOptions): SMono[PingResult] = {
    SMono.defer(() => SMono.fromFuture(async.ping(options)))
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
  def waitUntilReady(timeout: Duration): SMono[Unit] = {
    SMono.defer(() => SMono.fromFuture(async.waitUntilReady(timeout)))
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
  def waitUntilReady(timeout: Duration, options: WaitUntilReadyOptions): SMono[Unit] = {
    SMono.defer(() => SMono.fromFuture(async.waitUntilReady(timeout, options)))
  }
}
