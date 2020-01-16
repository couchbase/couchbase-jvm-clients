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
import com.couchbase.client.core.diagnostics._
import com.couchbase.client.core.env.{Authenticator, PasswordAuthenticator}
import com.couchbase.client.scala.AsyncCluster.seedNodesFromConnectionString
import com.couchbase.client.scala.analytics.{AnalyticsOptions, AnalyticsParameters, AnalyticsResult}
import com.couchbase.client.scala.diagnostics.{
  DiagnosticsOptions,
  PingOptions,
  WaitUntilReadyOptions
}
import com.couchbase.client.scala.env.{ClusterEnvironment, SeedNode}
import com.couchbase.client.scala.manager.analytics.AnalyticsIndexManager
import com.couchbase.client.scala.manager.bucket.BucketManager
import com.couchbase.client.scala.manager.query.QueryIndexManager
import com.couchbase.client.scala.manager.search.SearchIndexManager
import com.couchbase.client.scala.manager.user.UserManager
import com.couchbase.client.scala.query.{QueryOptions, QueryParameters, QueryResult}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

/** Represents a connection to a Couchbase cluster.
  *
  * These can be created through the functions in the companion object.
  *
  * @param _env the environment used to create this
  * @param ec  an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *            opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */
class Cluster private[scala] (
    _env: => ClusterEnvironment,
    authenticator: Authenticator,
    seedNodes: Set[SeedNode]
) {
  private[scala] implicit val ec: ExecutionContext = _env.ec

  /** Access an asynchronous version of this API. */
  val async = new AsyncCluster(_env, authenticator, seedNodes)

  /** Access a reactive version of this API. */
  lazy val reactive = new ReactiveCluster(async)

  /** The UserManager provides programmatic access to and creation of users and groups. */
  @Stability.Volatile
  lazy val users = new UserManager(async.users, reactive.users)

  /** The BucketManager provides access to creating and getting buckets. */
  @Stability.Volatile
  lazy val buckets = new BucketManager(async.buckets)

  @Stability.Volatile
  lazy val queryIndexes = new QueryIndexManager(async.queryIndexes)

  @Stability.Volatile
  lazy val searchIndexes = new SearchIndexManager(async.searchIndexes)

  @Stability.Volatile
  lazy val analyticsIndexes = new AnalyticsIndexManager(reactive.analyticsIndexes)

  /** The environment used to create this cluster */
  def env: ClusterEnvironment = _env

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param bucketName the name of the bucket to open
    */
  def bucket(bucketName: String): Bucket = {
    new Bucket(async.bucket(bucketName))
  }

  import com.couchbase.client.scala.util.DurationConversions._

  /** Performs a N1QL query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `Try` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(statement: String, options: QueryOptions): Try[QueryResult] = {
    val timeout: java.time.Duration = options.timeout match {
      case Some(v) => v
      case _       => _env.timeoutConfig.queryTimeout()
    }

    AsyncUtils.block(async.query(statement, options))
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
    * @return a `Try` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(
      statement: String,
      parameters: QueryParameters = QueryParameters.None,
      timeout: Duration = _env.timeoutConfig.queryTimeout(),
      adhoc: Boolean = true
  ): Try[QueryResult] = {
    AsyncUtils.block(async.query(statement, parameters, timeout, adhoc))
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[com.couchbase.client.scala.analytics.AnalyticsOptions]] for documentation
    *
    * @return a `Try` containing a `Success(AnalyticsResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def analyticsQuery(
      statement: String,
      options: AnalyticsOptions
  ): Try[AnalyticsResult] = {
    AsyncUtils.block(async.analyticsQuery(statement, options))
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.analytics.AnalyticsOptions]] instead, which supports all available options.
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
      timeout: Duration = _env.timeoutConfig.queryTimeout()
  ): Try[AnalyticsResult] = {
    AsyncUtils.block(
      async.analyticsQuery(statement, parameters, timeout)
    )
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param options           the FTS query to execute.  See [[com.couchbase.client.scala.search.SearchOptions]] for how to construct
    *
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      options: SearchOptions
  ): Try[SearchResult] = {
    AsyncUtils.block(async.searchQuery(indexName, query, options))
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * This overload provides only the most commonly used options.  If you need to configure something more
    * esoteric, use the overload that takes a [[com.couchbase.client.scala.search.SearchOptions]] instead, which supports all available options.
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param timeout   how long the operation is allowed to take
    *
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      timeout: Duration = async.env.timeoutConfig.searchTimeout()
  ): Try[SearchResult] = {
    AsyncUtils.block(async.searchQuery(indexName, query, timeout))
  }

  /** Shutdown all cluster resources.
    *
    * This should be called before application exit.
    *
    * @param timeout how long the disconnect is allowed to take; defaults to `disconnectTimeout` on the environment
    */
  def disconnect(timeout: Duration = _env.timeoutConfig.disconnectTimeout()): Unit = {
    reactive.disconnect(timeout).block()
  }

  /** Returns a `DiagnosticsResult`, reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param reportId        this will be returned in the `DiagnosticsResult`.  If not specified it defaults to a UUID.
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(reportId: String = UUID.randomUUID.toString): Try[DiagnosticsResult] = {
    AsyncUtils.block(async.diagnostics(reportId))
  }

  /** Returns a `DiagnosticsResult`, reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param options options to customize the report generation
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(options: DiagnosticsOptions): Try[DiagnosticsResult] = {
    AsyncUtils.block(async.diagnostics(options))
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
  def ping(
      timeout: Option[Duration] = None
  ): Try[PingResult] = {
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

/** Functions to allow creating a `Cluster`, which represents a connection to a Couchbase cluster.
  *
  * @define DeferredErrors Note that during opening of resources, all errors will be deferred until the first
  *                        attempted operation.
  */
object Cluster {

  /** Connect to a Couchbase cluster with a username and a password as credentials.
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param username         the name of a user with appropriate permissions on the cluster.
    * @param password         the password of a user with appropriate permissions on the cluster.
    *
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, username: String, password: String): Try[Cluster] = {
    connect(connectionString, ClusterOptions(PasswordAuthenticator.create(username, password)))
  }

  /** Connect to a Couchbase cluster with custom `Authenticator`.
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param options custom options used when connecting to the cluster.
    *
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, options: ClusterOptions): Try[Cluster] = {
    AsyncCluster
      .extractClusterEnvironment(connectionString, options)
      .map(ce => {
        val seedNodes = seedNodesFromConnectionString(connectionString, ce)
        val cluster   = new Cluster(ce, options.authenticator, seedNodes)
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
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(seedNodes: Set[SeedNode], options: ClusterOptions): Try[Cluster] = {
    AsyncCluster
      .extractClusterEnvironment(options)
      .map(ce => {
        val cluster = new Cluster(ce, options.authenticator, seedNodes)
        cluster.async.performGlobalConnect()
        cluster
      })
  }
}
