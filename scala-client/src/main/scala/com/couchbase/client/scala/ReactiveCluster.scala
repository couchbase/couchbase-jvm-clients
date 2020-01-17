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
import java.util.stream.Collectors

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.diagnostics.{ClusterState, DiagnosticsResult, PingResult}
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.error.ErrorCodeAndMessage
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.scala.AsyncCluster.{extractClusterEnvironment, seedNodesFromConnectionString}
import com.couchbase.client.scala.analytics._
import com.couchbase.client.scala.env.SeedNode
import com.couchbase.client.scala.manager.analytics.{AnalyticsIndexManager, ReactiveAnalyticsIndexManager}
import com.couchbase.client.scala.manager.bucket.ReactiveBucketManager
import com.couchbase.client.scala.manager.query.ReactiveQueryIndexManager
import com.couchbase.client.scala.manager.search.ReactiveSearchIndexManager
import com.couchbase.client.scala.manager.user.ReactiveUserManager
import com.couchbase.client.scala.query.handlers.SearchHandler
import com.couchbase.client.scala.query.{ReactiveQueryResult, _}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.{ReactiveSearchResult, SearchMetaData, SearchRow}
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import reactor.core.scala.publisher.SMono

import scala.collection.GenMap
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import com.couchbase.client.scala.util.DurationConversions._

/** Represents a connection to a Couchbase cluster.
  *
  * This is the reactive version of the [[Cluster]] API.
  *
  * These can be created through the functions in the companion object, or through [[Cluster.reactive]].
  *
  * @param async an asynchronous version of this API
  * @param ec    an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *              opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */
class ReactiveCluster(val async: AsyncCluster) {
  private[scala] implicit val ec: ExecutionContext = async.env.ec
  private val env                                  = async.env

  /** The ReactiveUserManager provides programmatic access to and creation of users and groups. */
  @Stability.Volatile
  lazy val users = new ReactiveUserManager(async.core)

  /** The ReactiveBucketManager provides access to creating and getting buckets. */
  @Stability.Volatile
  lazy val buckets = new ReactiveBucketManager(async.core)

  /** The ReactiveQueryIndexManager provides access to creating and managing query indexes. */
  @Stability.Volatile
  lazy val queryIndexes = new ReactiveQueryIndexManager(async.queryIndexes, this)

  @Stability.Volatile
  lazy val searchIndexes = new ReactiveSearchIndexManager(async.searchIndexes)

  @Stability.Volatile
  lazy val analyticsIndexes = new ReactiveAnalyticsIndexManager(this)

  /** Performs a N1QL query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[QueryOptions]] for documentation
    *
    * @return a `Mono` containing a [[ReactiveQueryResult]] which includes a Flux giving streaming access to any
    *         returned rows
    **/
  def query(
      statement: String,
      options: QueryOptions
  ): SMono[ReactiveQueryResult] = {
    async.queryHandler.queryReactive(statement, options, env)
  }

  /** Performs a N1QL query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[QueryOptions]] instead, which supports all available options.
    *
    * @param statement the N1QL statement to execute
    * @param parameters provides named or positional parameters for queries parameterised that way.
    * @param timeout sets a maximum timeout for processing.
    * @param adhoc if true (the default), adhoc mode is enabled: queries are just run.  If false, adhoc mode is disabled
    *              and transparent prepared statement mode is enabled: queries are first prepared so they can be executed
    *              more efficiently in the future.
    *
    * @return a `Mono` containing a [[ReactiveQueryResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def query(
      statement: String,
      parameters: QueryParameters = QueryParameters.None,
      timeout: Duration = env.timeoutConfig.queryTimeout(),
      adhoc: Boolean = true
  ): SMono[ReactiveQueryResult] = {
    val opts = QueryOptions()
      .adhoc(adhoc)
      .timeout(timeout)
      .parameters(parameters)
    query(statement, opts)
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[analytics.AnalyticsOptions]] for documentation
    *
    * @return a `Mono` containing a [[analytics.ReactiveAnalyticsResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def analyticsQuery(
      statement: String,
      options: AnalyticsOptions
  ): SMono[ReactiveAnalyticsResult] = {
    async.analyticsHandler.request(statement, options, async.core, async.env) match {
      case Success(request) =>
        SMono.defer(() => {
          async.core.send(request)

          FutureConversions
            .javaCFToScalaMono(request, request.response(), false)
            .map(response => {
              val meta: SMono[AnalyticsMetaData] = FutureConversions
                .javaMonoToScalaMono(response.trailer())
                .map(trailer => {
                  val warnings: Seq[AnalyticsWarning] = trailer.warnings.asScala
                    .map(
                      warnings =>
                        ErrorCodeAndMessage
                          .fromJsonArray(warnings)
                          .asScala
                          .map(codeAndMessage => AnalyticsWarning(codeAndMessage))
                    )
                    .getOrElse(Seq.empty)

                  AnalyticsMetaData(
                    response.header().requestId(),
                    response.header().clientContextId().orElse(""),
                    response.header().signature.asScala,
                    AnalyticsMetrics.fromBytes(trailer.metrics()),
                    warnings,
                    AnalyticsStatus.from(trailer.status)
                  )
                })
                .doOnTerminate(() => request.context().logicallyComplete())

              val rows = FutureConversions.javaFluxToScalaFlux(response.rows())

              ReactiveAnalyticsResult(
                rows,
                meta
              )
            })
        })

      case Failure(err) => SMono.raiseError(err)
    }
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the Analytics query to execute
    * @param parameters provides named or positional parameters for queries parameterised that way.
    * @param timeout sets a maximum timeout for processing.
    *
    * @return a `Mono` containing a [[analytics.ReactiveAnalyticsResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def analyticsQuery(
      statement: String,
      parameters: AnalyticsParameters = AnalyticsParameters.None,
      timeout: Duration = env.timeoutConfig.queryTimeout()
  ): SMono[ReactiveAnalyticsResult] = {
    val opts = AnalyticsOptions()
      .timeout(timeout)
      .parameters(parameters)
    analyticsQuery(statement, opts)
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param options           the FTS query to execute.  See [[SearchOptions]] for how to construct
    *
    * @return a `Mono` containing a [[search.result.ReactiveSearchResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      options: SearchOptions
  ): SMono[ReactiveSearchResult] = {
    async.searchHandler.request(indexName, query, options, async.core, async.env) match {
      case Success(request) =>
        SMono.defer(() => {
          async.core.send(request)

          FutureConversions
            .javaCFToScalaMono(request, request.response(), false)
            .map(response => {
              val meta: SMono[SearchMetaData] = FutureConversions
                .javaMonoToScalaMono(response.trailer())
                .map(trailer => SearchHandler.parseSearchMeta(response, trailer))

              val facets = FutureConversions
                .javaMonoToScalaMono(response.trailer())
                .map(trailer => SearchHandler.parseSearchFacets(trailer))

              val rows = FutureConversions
                .javaFluxToScalaFlux(response.rows())
                .map(row => SearchRow.fromResponse(row))

              ReactiveSearchResult(
                rows,
                facets,
                meta
              )
            })
            .doOnTerminate(() => request.context().logicallyComplete())
        })

      case Failure(err) =>
        SMono.raiseError(err)
    }
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[SearchOptions]] instead, which supports all available options.
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param timeout   how long the operation is allowed to take
    *
    * @return a `Mono` containing a [[search.result.ReactiveSearchResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      timeout: Duration = async.env.timeoutConfig.searchTimeout()
  ): SMono[ReactiveSearchResult] = {
    searchQuery(indexName, query, SearchOptions(timeout = Some(timeout)))
  }

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param bucketName the name of the bucket to open
    */
  def bucket(bucketName: String): ReactiveBucket = {
    new ReactiveBucket(async.bucket(bucketName))
  }

  /** Shutdown all cluster resources.search
    *
    * This should be called before application exit.
    */
  def disconnect(): SMono[Unit] = {
    FutureConversions
      .javaMonoToScalaMono(async.core.shutdown())
      .`then`(SMono.defer(() => {
        if (env.owned) {
          env.shutdownReactive()
        } else {
          SMono.empty[Unit]
        }
      }))
  }

  /** Returns a [[DiagnosticsResult]], reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param reportId        this will be returned in the [[DiagnosticsResult]].  If not specified it defaults to a UUID.
    *
    * @return a { @link DiagnosticsResult}
    */
  def diagnostics(reportId: String = UUID.randomUUID.toString): SMono[DiagnosticsResult] = {
    SMono.fromFuture(async.diagnostics(reportId))
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
      retryStrategy: RetryStrategy = env.retryStrategy
  ): SMono[PingResult] = {
    SMono.fromFuture(async.ping(serviceTypes, reportId, timeout, retryStrategy))
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
  ): SMono[Unit] = {
    SMono.fromFuture(async.waitUntilReady(timeout, desiredState, serviceTypes))
  }
}

/** Functions to allow creating a `ReactiveCluster`, which represents a connection to a Couchbase cluster.
  *
  * @define DeferredErrors Note that during opening of resources, all errors will be deferred until the first
  *                        attempted operation.
  */
object ReactiveCluster {

  /** Connect to a Couchbase cluster with a username and a password as credentials.
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param username         the name of a user with appropriate permissions on the cluster.
    * @param password         the password of a user with appropriate permissions on the cluster.
    *
    * @return a Try[ReactiveCluster] representing a connection to the cluster
    */
  def connect(
      connectionString: String,
      username: String,
      password: String
  ): Try[ReactiveCluster] = {
    connect(connectionString, ClusterOptions(PasswordAuthenticator.create(username, password)))
  }

  /** Connect to a Couchbase cluster with custom `com.couchbase.client.core.env.Credentials`.
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param options custom options used when connecting to the cluster.
    *
    * @return a Try[ReactiveCluster] representing a connection to the cluster
    */
  def connect(connectionString: String, options: ClusterOptions): Try[ReactiveCluster] = {
    extractClusterEnvironment(connectionString, options)
      .map(ce => {
        implicit val ec: ExecutionContextExecutor = ce.ec
        val seedNodes = seedNodesFromConnectionString(connectionString, ce)
        val cluster = new ReactiveCluster(new AsyncCluster(ce, options.authenticator, seedNodes))
        cluster.async.performGlobalConnect()
        cluster
      })
  }

  /** Connect to a Couchbase cluster with a custom Set of [[SeedNode]].
    *
    * $DeferredErrors
    *
    * @param seedNodes known nodes from the Couchbase cluster to use for bootstrapping.
    * @param options custom options used when connecting to the cluster.
    *
    * @return a Try[[ReactiveCluster]] representing a connection to the cluster
    */
  def connect(seedNodes: Set[SeedNode], options: ClusterOptions): Try[ReactiveCluster] = {
    AsyncCluster
      .extractClusterEnvironment(options)
      .map(ce => {
        val cluster = new ReactiveCluster(new AsyncCluster(ce, options.authenticator, seedNodes))
        cluster.async.performGlobalConnect()
        cluster
      })
  }

}
