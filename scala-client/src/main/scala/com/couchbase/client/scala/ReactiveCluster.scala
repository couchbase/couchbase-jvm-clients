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
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.diagnostics.{DiagnosticsResult, PingResult}
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.transaction.CoreTransactionsReactive
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString
import com.couchbase.client.scala.AsyncCluster.extractClusterEnvironment
import com.couchbase.client.scala.analytics._
import com.couchbase.client.scala.diagnostics.{
  DiagnosticsOptions,
  PingOptions,
  WaitUntilReadyOptions
}
import com.couchbase.client.scala.env.{ClusterEnvironment, SeedNode}
import com.couchbase.client.scala.manager.analytics.ReactiveAnalyticsIndexManager
import com.couchbase.client.scala.manager.bucket.ReactiveBucketManager
import com.couchbase.client.scala.manager.eventing.ReactiveEventingFunctionManager
import com.couchbase.client.scala.manager.query.ReactiveQueryIndexManager
import com.couchbase.client.scala.manager.search.ReactiveSearchIndexManager
import com.couchbase.client.scala.manager.user.ReactiveUserManager
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.query.handlers.AnalyticsHandler
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.ReactiveSearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.transactions.{ReactiveTransactions, Transactions}
import com.couchbase.client.scala.transactions.config.TransactionsConfig
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions
import reactor.core.scala.publisher.SMono

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

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

  /** The environment used to create this cluster */
  val env: ClusterEnvironment = async.env

  /** The ReactiveUserManager provides programmatic access to and creation of users and groups. */
  lazy val users = new ReactiveUserManager(async.couchbaseOps)

  /** The ReactiveBucketManager provides access to creating and getting buckets. */
  lazy val buckets = new ReactiveBucketManager(async.couchbaseOps)

  /** The ReactiveQueryIndexManager provides access to creating and managing query indexes. */
  lazy val queryIndexes = new ReactiveQueryIndexManager(async.queryIndexes, this)

  lazy val searchIndexes    = new ReactiveSearchIndexManager(async.searchIndexes)
  lazy val analyticsIndexes = new ReactiveAnalyticsIndexManager(this, async.analyticsIndexes)

  @Stability.Uncommitted
  lazy val eventingFunctions = new ReactiveEventingFunctionManager(async.eventingFunctions)

  @Stability.Uncommitted
  lazy val transactions = new ReactiveTransactions(
    new CoreTransactionsReactive(
      async.core,
      env.transactionsConfig.map(v => v.toCore).getOrElse(TransactionsConfig().toCore)
    )
  )

  /** Performs a N1QL query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `Mono` containing a [[com.couchbase.client.scala.query.ReactiveQueryResult]] which includes a Flux giving streaming access to any
    *         returned rows
    **/
  def query(
      statement: String,
      options: QueryOptions
  ): SMono[ReactiveQueryResult] = {
    convert(async.queryOps.queryReactive(statement, options.toCore, null, null, null))
      .map(result => convert(result))
  }

  /** Performs a N1QL query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
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
    * @return a `Mono` containing a [[com.couchbase.client.scala.query.ReactiveQueryResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def query(
      statement: String,
      parameters: QueryParameters = QueryParameters.None,
      timeout: Duration = env.timeoutConfig.queryTimeout(),
      adhoc: Boolean = true
  ): SMono[ReactiveQueryResult] = {
    convert(
      async.queryOps.queryReactive(
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
    async.couchbaseOps match {
      case core: Core =>
        val hp               = HandlerBasicParams(core)
        val analyticsHandler = new AnalyticsHandler(hp)

        analyticsHandler.request(statement, options, core, async.env, None, None) match {
          case Success(request) => analyticsHandler.queryReactive(request)
          case Failure(err)     => SMono.error(err)
        }
      case _ => SMono.error(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
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

  /** Performs a Full Text Search (FTS) query against the cluster, using default options.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    *
    * @return an `SMono` containing a [[ReactiveSearchResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  @Volatile
  def search(
      indexName: String,
      request: SearchRequest
  ): SMono[ReactiveSearchResult] = {
    search(indexName, request, SearchOptions())
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    * @param options   see [[com.couchbase.client.scala.search.SearchOptions]]
    * @return an `SMono` containing a [[ReactiveSearchResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  @Volatile
  def search(
      indexName: String,
      request: SearchRequest,
      options: SearchOptions
  ): SMono[ReactiveSearchResult] = {
    request.toCore match {
      case Failure(err) => SMono.raiseError(err)
      case Success(req) =>
        convert(async.searchOps.searchReactive(indexName, req, options.toCore))
          .map(result => ReactiveSearchResult(result))
    }
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * New users should consider the newer `search(String, SearchRequest)` interface instead, which can do both the traditional FTS {@link SearchQuery} that this method performs,
    * and/or can also be used to perform a [[com.couchbase.client.scala.search.vector.VectorSearch]].
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param options           the FTS query to execute.  See [[com.couchbase.client.scala.search.SearchOptions]] for how to construct
    * @return an `SMono` containing a [[ReactiveSearchResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      options: SearchOptions
  ): SMono[ReactiveSearchResult] = {
    SMono(async.searchOps.searchQueryReactive(indexName, query.toCore, options.toCore))
      .map(result => ReactiveSearchResult(result))
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
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
    * @param timeout   how long the operation is allowed to take
    * @return a `Mono` containing a [[ReactiveSearchResult]] which includes a Flux giving streaming access to any
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
    *
    * @param timeout how long the disconnect is allowed to take; defaults to `disconnectTimeout` on the environment
    */
  def disconnect(timeout: Duration = env.timeoutConfig.disconnectTimeout()): SMono[Unit] = {
    // Take care not to use the implicit ExecutionContext `ec`, which is based on an executor that's about to be destroyed
    SMono.fromFuture(async.disconnect(timeout))(ExecutionContext.global)
  }

  /** Returns a `DiagnosticsResult`, reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param reportId        this will be returned in the `DiagnosticsResult`.  If not specified it defaults to a UUID.
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(reportId: String = UUID.randomUUID.toString): SMono[DiagnosticsResult] = {
    SMono.defer(() => SMono.fromFuture(async.diagnostics(reportId)))
  }

  /** Returns a `DiagnosticsResult`, reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param options options to customize the report generation
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(options: DiagnosticsOptions): SMono[DiagnosticsResult] = {
    SMono.defer(() => SMono.fromFuture(async.diagnostics(options)))
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
  def ping(
      timeout: Option[Duration] = None
  ): SMono[PingResult] = {
    SMono.defer(() => SMono.fromFuture(async.ping(timeout)))
  }

  /** Performs application-level ping requests with custom options against services in the Couchbase cluster.
    *
    * Note that this operation performs active I/O against services and endpoints to assess their health. If you do
    * not wish to perform I/O, consider using the `.diagnostics()` instead.
    *
    * @param options options to customize the ping
    *
    * @return the PingResult once complete.
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
        val connStr                               = ConnectionString.create(connectionString)
        val cluster = new ReactiveCluster(
          new AsyncCluster(ce, options.authenticator, connStr)
        )
        cluster.async.performGlobalConnect()
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
    * @return a Try[[ReactiveCluster]] representing a connection to the cluster
    */
  def connect(seedNodes: Set[SeedNode], options: ClusterOptions): Try[ReactiveCluster] = {
    connect(asConnectionString(seedNodes.map(_.toCore).asJava).original(), options)
  }

}
