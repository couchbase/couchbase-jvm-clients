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

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.annotation.Stability.Uncommitted
import com.couchbase.client.core.diagnostics._
import com.couchbase.client.core.env.{Authenticator, PasswordAuthenticator}
import com.couchbase.client.core.transaction.CoreTransactionsReactive
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString
import com.couchbase.client.scala.diagnostics.{
  DiagnosticsOptions,
  PingOptions,
  WaitUntilReadyOptions
}
import com.couchbase.client.scala.env.{ClusterEnvironment, SeedNode}
import com.couchbase.client.scala.query.{QueryOptions, QueryParameters, QueryResult}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.transactions.Transactions
import com.couchbase.client.scala.transactions.config.TransactionsConfig
import com.couchbase.client.scala.util.AsyncUtils
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import com.couchbase.client.scala.util.DurationConversions._

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Represents a connection to a Couchbase cluster.
  *
  * These can be created through the functions in the companion object.
  */
class Cluster private[scala] (
    _env: => ClusterEnvironment,
    private[scala] val authenticator: Authenticator,
    private[scala] val connectionString: ConnectionString
) extends ClusterBase {
  private[scala] lazy val environment = _env

  /** Performs a SQL++ query against the cluster.
    *
    * @param statement the SQL++ statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `Try` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(statement: String, options: QueryOptions = QueryOptions.Default): Try[QueryResult] = {
    Try(async.queryOps.queryBlocking(statement, options.toCore, null, null, null))
      .map(result => convert(result))
  }

  /** Performs a Full Text Search (FTS) query.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * Use this to access global FTS indexes, and [[Scope.search]] for scoped indexes.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    * @param options   see [[com.couchbase.client.scala.search.SearchOptions]]
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def search(
      indexName: String,
      request: SearchRequest,
      options: SearchOptions = SearchOptions.Default
  ): Try[SearchResult] = {
    AsyncUtils.block(async.search(indexName, request, options))
  }

  /** Shutdown all cluster resources.
    *
    * This should be called before application exit.
    *
    * @param timeout how long the disconnect is allowed to take; defaults to `disconnectTimeout` on the environment
    */
  def disconnect(timeout: Duration = env.timeoutConfig.disconnectTimeout()): Unit = {
    AsyncUtils.block(async.disconnect(timeout))
  }

  /** Returns a `DiagnosticsResult`, reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param options options to customize the report generation
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(
      options: DiagnosticsOptions = DiagnosticsOptions.Default
  ): Try[DiagnosticsResult] = {
    AsyncUtils.block(async.diagnostics(options))
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
  def ping(options: PingOptions = PingOptions.Default): Try[PingResult] = {
    AsyncUtils.block(async.ping(options))
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
  ): Try[Unit] = {
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
        val connStr = ConnectionString.create(connectionString)
        val cluster = new Cluster(ce, options.authenticator, connStr)
        cluster.async.performGlobalConnect()
        cluster
      })
  }
}
