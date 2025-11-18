/*
 * Copyright (c) 2025 Couchbase, Inc.
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
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.diagnostics.{HealthPinger, PingResult}
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.scala.diagnostics.{PingOptions, WaitUntilReadyOptions}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.manager.collection.AsyncCollectionManager
import com.couchbase.client.scala.manager.view.AsyncViewIndexManager
import com.couchbase.client.scala.view.{ViewOptions, ViewResult}
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions

import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption

/** Represents a Couchbase bucket resource.
  *
  * This is the asynchronous version of the [[Bucket]] API.
  *
  * Applications should not create these manually, but instead use the functions in [[Cluster]].
  *
  * @param name the name of this bucket
  */
class AsyncBucket private[scala] (
    val name: String,
    private[scala] val couchbaseOps: CoreCouchbaseOps,
    private[scala] val environment: ClusterEnvironment
) extends AsyncBucketBase {
  val reactive = new ReactiveBucket(this)

  @deprecated(
    "Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version. Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the Index Service (GSI) and the Query Service (SQL++).",
    since = "3.10.1"
  )
  lazy val viewIndexes = new AsyncViewIndexManager(reactive.viewIndexes)

  lazy val collections = new AsyncCollectionManager(this)

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
  @deprecated(
    "Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version. Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the Index Service (GSI) and the Query Service (SQL++).",
    since = "3.10.1"
  )
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
  @deprecated(
    "Views are deprecated in Couchbase Server 7.0+, and will be removed from a future server version. Views are not compatible with the Magma storage engine. Instead of views, use indexes and queries using the Index Service (GSI) and the Query Service (SQL++).",
    since = "3.10.1"
  )
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
    couchbaseOps match {
      case core: Core =>
        val future = HealthPinger
          .ping(
            core,
            options.timeout.map(scalaDurationToJava).toJava,
            options.retryStrategy.getOrElse(environment.retryStrategy),
            if (options.serviceTypes.isEmpty) null else options.serviceTypes.asJava,
            options.reportId.toJava,
            Optional.of(name)
          )
          .toFuture

        FutureConversions.javaCFToScalaFuture(future)

      case _ => Future.failed(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }
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
  def waitUntilReady(
      timeout: Duration,
      options: WaitUntilReadyOptions = WaitUntilReadyOptions.Default
  ): Future[Unit] = {
    couchbaseOps match {
      case core: Core =>
        FutureConversions
          .javaCFToScalaFuture(
            core.waitUntilReady(
              if (options.serviceTypes.isEmpty) null else options.serviceTypes.asJava,
              timeout,
              options.desiredState,
              name
            )
          )
          .map(_ => ())

      case _ => Future.failed(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }
  }
}
