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
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.diagnostics.{
  DiagnosticsResult,
  EndpointDiagnostics,
  HealthPinger,
  PingResult
}
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionStringUtil.{
  asConnectionString,
  checkConnectionString
}
import com.couchbase.client.scala.analytics.{AnalyticsOptions, AnalyticsParameters, AnalyticsResult}
import com.couchbase.client.scala.diagnostics.{
  DiagnosticsOptions,
  PingOptions,
  WaitUntilReadyOptions
}
import com.couchbase.client.scala.env.{ClusterEnvironment, PasswordAuthenticator, SeedNode}
import com.couchbase.client.scala.handlers.AnalyticsHandler
import com.couchbase.client.scala.manager.analytics.{
  AsyncAnalyticsIndexManager,
  ReactiveAnalyticsIndexManager
}
import com.couchbase.client.scala.manager.bucket.{AsyncBucketManager, ReactiveBucketManager}
import com.couchbase.client.scala.manager.eventing.AsyncEventingFunctionManager
import com.couchbase.client.scala.manager.query.AsyncQueryIndexManager
import com.couchbase.client.scala.manager.search.AsyncSearchIndexManager
import com.couchbase.client.scala.manager.user.AsyncUserManager
import com.couchbase.client.scala.query.{QueryOptions, QueryParameters, QueryResult}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.util.CoreCommonConverters.convert

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions

import java.util.{Optional, UUID}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption

class AsyncCluster(
    _env: => ClusterEnvironment,
    private[scala] val authenticator: Authenticator,
    private[scala] val connectionString: ConnectionString
) extends AsyncClusterBase {
  private[scala] lazy val environment = _env

  private[scala] lazy val reactiveAnalyticsIndexManager = new ReactiveAnalyticsIndexManager(
    new ReactiveCluster(this),
    analyticsIndexes
  )

  lazy val analyticsIndexes: AsyncAnalyticsIndexManager =
    new AsyncAnalyticsIndexManager(reactiveAnalyticsIndexManager, couchbaseOps)

  private[scala] lazy val reactiveBucketManager = new ReactiveBucketManager(couchbaseOps)

  /** The AsyncBucketManager provides access to creating and getting buckets. */
  lazy val buckets = new AsyncBucketManager(couchbaseOps)

  /** The AsyncUserManager provides programmatic access to and creation of users and groups. */
  lazy val users = new AsyncUserManager(couchbaseOps)

  lazy val queryIndexes  = new AsyncQueryIndexManager(this)
  lazy val searchIndexes = new AsyncSearchIndexManager(couchbaseOps)

  @Stability.Uncommitted
  lazy val eventingFunctions = new AsyncEventingFunctionManager(env, couchbaseOps)

  /** Performs a N1QL query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `Future` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(statement: String, options: QueryOptions): Future[QueryResult] = {
    convert(queryOps.queryAsync(statement, options.toCore, null, null, null))
      .map(result => convert(result))
  }

  /** Performs a N1QL query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.query.QueryOptions]] instead, which supports all available options.
    *
    * @param statement the N1QL statement to execute
    * @param parameters provides named or positional parameters for queries parameterised that way.
    * @param timeout sets a maximum timeout for processing.
    * @param adhoc if true (the default), adhoc mode is enabled: queries are just run.  If false, adhoc mode is disabled
    *              and transparent prepared statement mode is enabled: queries are first prepared so they can be executed
    *              more efficiently in the future.
    *
    * @return a `Future` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(
      statement: String,
      parameters: QueryParameters = QueryParameters.None,
      timeout: Duration = env.timeoutConfig.queryTimeout(),
      adhoc: Boolean = true
  ): Future[QueryResult] = {
    convert(
      queryOps.queryAsync(
        statement,
        QueryOptions()
          .adhoc(adhoc)
          .timeout(timeout)
          .parameters(parameters)
          .toCore,
        null,
        null,
        null
      )
    ).map(result => convert(result))
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[com.couchbase.client.scala.analytics.AnalyticsOptions]] for documentation
    *
    * @return a `Future` containing a `Success(AnalyticsResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def analyticsQuery(
      statement: String,
      options: AnalyticsOptions
  ): Future[AnalyticsResult] = {
    couchbaseOps match {
      case core: Core =>
        val hp               = HandlerBasicParams(core)
        val analyticsHandler = new AnalyticsHandler(hp)

        analyticsHandler.request(statement, options, core, environment, None, None) match {
          case Success(request) => analyticsHandler.queryAsync(request)
          case Failure(err)     => Future.failed(err)
        }
      case _ => Future.failed(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the Analytics query to execute
    * @param parameters provides named or positional parameters for queries parameterised that way.
    * @param timeout sets a maximum timeout for processing.
    *
    * @return a `Future` containing a `Success(AnalyticsResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def analyticsQuery(
      statement: String,
      parameters: AnalyticsParameters = AnalyticsParameters.None,
      timeout: Duration = env.timeoutConfig.queryTimeout()
  ): Future[AnalyticsResult] = {
    val opts = AnalyticsOptions()
      .timeout(timeout)
      .parameters(parameters)
    analyticsQuery(statement, opts)
  }

  /** Performs a Full Text Search (FTS) search against the cluster, using default options.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    *
    * @return a `Future` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def search(
      indexName: String,
      request: SearchRequest
  ): Future[SearchResult] = {
    search(indexName, request, SearchOptions())
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    * @param options   see [[com.couchbase.client.scala.search.SearchOptions]]
    *
    * @return a `Future` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def search(
      indexName: String,
      request: SearchRequest,
      options: SearchOptions
  ): Future[SearchResult] = {
    request.toCore match {
      case Failure(err) => Future.failed(err)
      case Success(req) =>
        convert(searchOps.searchAsync(indexName, req, options.toCore))
          .map(result => SearchResult(result))
    }
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * New users should consider the newer `search(String, SearchRequest)` interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
    * and/or can also be used to perform a [[com.couchbase.client.scala.search.vector.VectorSearch]].
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param options           see [[com.couchbase.client.scala.search.SearchOptions]]
    *
    * @return a `Future` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      options: SearchOptions
  ): Future[SearchResult] = {
    convert(searchOps.searchQueryAsync(indexName, query.toCore, options.toCore))
      .map(result => SearchResult(result))
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.search.SearchOptions]] instead, which supports all available options.
    *
    * New users should consider the newer `search(String, SearchRequest)` interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
    * and/or can also be used to perform a [[com.couchbase.client.scala.search.vector.VectorSearch]].
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param timeout           how long the operation is allowed to take
    * @return a `Future` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      timeout: Duration = environment.timeoutConfig.searchTimeout()
  ): Future[SearchResult] = {
    searchQuery(indexName, query, SearchOptions(timeout = Some(timeout)))
  }

  /** Shutdown all cluster resources.
    *
    * This should be called before application exit.
    *
    * @param timeout how long the disconnect is allowed to take; defaults to `disconnectTimeout` on the environment
    */
  def disconnect(timeout: Duration = env.timeoutConfig.disconnectTimeout()): Future[Unit] = {
    FutureConversions
      .javaMonoToScalaFuture(couchbaseOps.shutdown(timeout))
      .map(_ => if (env.owned) env.shutdown(timeout))
  }

  /** Returns a `DiagnosticsResult`, reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param reportId        this will be returned in the `DiagnosticsResult`.  If not specified it defaults to a UUID.
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(reportId: String = UUID.randomUUID.toString): Future[DiagnosticsResult] = {
    diagnostics(DiagnosticsOptions(Some(reportId)))
  }

  /** Returns a `DiagnosticsResult`, reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param options options to customize the report generation
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(options: DiagnosticsOptions): Future[DiagnosticsResult] = {
    couchbaseOps match {
      case core: Core =>
        Future(
          new DiagnosticsResult(
            core.diagnostics.collect(
              Collectors
                .groupingBy[EndpointDiagnostics, ServiceType]((v1: EndpointDiagnostics) =>
                  v1.`type`()
                )
            ),
            core.context().environment().userAgent().formattedShort(),
            options.reportId.getOrElse(UUID.randomUUID.toString)
          )
        )

      case _ => Future.failed(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }

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
            options.retryStrategy.getOrElse(env.retryStrategy),
            if (options.serviceTypes.isEmpty) null else options.serviceTypes.asJava,
            options.reportId.toJava,
            Optional.empty()
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
        couchbaseOps.waitUntilReady(
          if (options.serviceTypes.isEmpty) null else options.serviceTypes.asJava,
          timeout,
          options.desiredState,
          null
        )
      )
      .map(_ => ())
  }
}

/** Functions to allow creating an `AsyncCluster`, which represents a connection to a Couchbase cluster.
  *
  * @define DeferredErrors Note that during opening of resources, all errors will be deferred until the first
  *                        attempted operation.
  */
object AsyncCluster {

  /** Connect to a Couchbase cluster with a username and a password as credentials.
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param username         the name of a user with appropriate permissions on the cluster.
    * @param password         the password of a user with appropriate permissions on the cluster.
    *
    * @return an [[AsyncCluster]] representing a connection to the cluster
    */
  def connect(
      connectionString: String,
      username: String,
      password: String
  ): Try[AsyncCluster] = {
    connect(connectionString, ClusterOptions(PasswordAuthenticator(username, password)))
  }

  /** Connect to a Couchbase cluster with custom `Authenticator`.
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param options custom options used when connecting to the cluster.
    *
    * @return an [[AsyncCluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, options: ClusterOptions): Try[AsyncCluster] = {
    extractClusterEnvironment(connectionString, options)
      .map(ce => {
        val connStr = ConnectionString.create(connectionString)
        val cluster = new AsyncCluster(ce, options.authenticator, connStr)
        cluster.performGlobalConnect()
        cluster
      })
  }

  /** Connect to a Couchbase cluster with a custom Set of [[com.couchbase.client.scala.env.SeedNode]].
    *
    * $DeferredErrors
    *
    * @param seedNodes known nodes from the Couchbase cluster to use for bootstrapping.
    * @param options custom options used when connecting to the cluster.
    *
    * @return an [[AsyncCluster]] representing a connection to the cluster
    */
  def connect(seedNodes: Set[SeedNode], options: ClusterOptions): Try[AsyncCluster] = {
    connect(asConnectionString(seedNodes.map(_.toCore).asJava).original(), options)
  }

  private[client] def extractClusterEnvironment(
      connectionString: String,
      opts: ClusterOptions
  ): Try[ClusterEnvironment] = {
    val result = opts.environment match {
      case Some(env) => Success(env)
      case _ => ClusterEnvironment.Builder(owned = true).connectionString(connectionString).build
    }

    if (result.isFailure) result
    else
      Try {
        val env = result.get
        checkConnectionString(env.core, env.owned, ConnectionString.create(connectionString))
        env
      }
  }

  private[client] def extractClusterEnvironment(opts: ClusterOptions): Try[ClusterEnvironment] = {
    opts.environment match {
      case Some(env) => Success(env)
      case _         => ClusterEnvironment.Builder(owned = true).build
    }
  }
}
