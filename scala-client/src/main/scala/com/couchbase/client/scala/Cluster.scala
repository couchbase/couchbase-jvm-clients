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

import com.couchbase.client.core.env.Credentials
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.analytics.{AnalyticsOptions, AnalyticsResult}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.{QueryOptions, QueryResult}
import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.util.AsyncUtils
import io.opentracing.Span

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

/** Represents a connection to a Couchbase cluster.
  *
  * These can be created through the functions in the companion object.
  *
  * @param env the environment used to create this
  * @param ec  an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *            opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */
class Cluster private[scala](env: => ClusterEnvironment)
                            (implicit ec: ExecutionContext) {

  /** Access an asynchronous version of this API. */
  lazy val async = new AsyncCluster(env)

  /** Access a reactive version of this API. */
  lazy val reactive = new ReactiveCluster(async)

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param name the name of the bucket to open
    */
  def bucket(name: String): Bucket = {
    AsyncUtils.block(async.bucket(name))
      .map(new Bucket(_))
      .get
  }

  import com.couchbase.client.scala.util.DurationConversions._

  /** Performs a N1QL query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[QueryOptions]] for documentation
    *
    * @return a `Try` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(statement: String, options: QueryOptions = QueryOptions()): Try[QueryResult] = {
    val timeout: java.time.Duration = options.timeout match {
      case Some(v) => v
      case _ => env.timeoutConfig.queryTimeout()
    }

    AsyncUtils.block(async.query(statement, options), timeout)
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[AnalyticsOptions]] for documentation
    *
    * @return a `Try` containing a `Success(AnalyticsResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def analyticsQuery(statement: String, options: AnalyticsOptions = AnalyticsOptions()): Try[AnalyticsResult] = {
    val timeout: java.time.Duration = options.timeout match {
      case Some(v) => v
      case _ => env.timeoutConfig.queryTimeout()
    }

    AsyncUtils.block(async.analyticsQuery(statement, options), timeout)
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
    *
    * @param query           the FTS query to execute.  See [[SearchQuery]] for how to construct
    * @param parentSpan      this SDK supports the [[https://opentracing.io/ Open Tracing]] initiative, which is a
    *                        way of
    *                        tracing complex distributed systems.  This field allows an OpenTracing parent span to be
    *                        provided, which will become the parent of any spans created by the SDK as a result of this
    *                        operation.  Note that if a span is not provided then the SDK will try to access any
    *                        thread-local parent span setup by a Scope.  Much of time this will `just work`, but it's
    *                        recommended to provide the parentSpan explicitly if possible, as thread-local is not a
    *                        100% reliable way of passing parameters.
    * @param timeout         when the operation will timeout.  This will default to `timeoutConfig().searchTimeout()` in the
    *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
    * @param retryStrategy   provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(query: SearchQuery,
                  parentSpan: Option[Span] = None,
                  timeout: Duration = async.searchTimeout,
                  retryStrategy: RetryStrategy = async.retryStrategy): Try[SearchResult] = {
    AsyncUtils.block(async.searchQuery(query, parentSpan, timeout, retryStrategy))
  }

  /** Shutdown all cluster resources.
    *
    * This should be called before application exit.
    */
  def shutdown(): Unit = {
    AsyncUtils.block(async.shutdown(), Duration.Inf)
  }
}

/** Functions to allow creating a `Cluster`, which represents a connection to a Couchbase cluster.
  *
  * @define DeferredErrors Note that during opening of resources, all errors will be deferred until the first
  *                        attempted operation.
  */
object Cluster {
  private[scala] implicit val ec = ClusterEnvironment.ec

  /** Connect to a Couchbase cluster with a username and a password as credentials.
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param username         the name of a user with appropriate permissions on the cluster.
    * @param password         the password of a user with appropriate permissions on the cluster.
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, username: String, password: String): Cluster = {
    val env = ClusterEnvironment.create(connectionString, username, password)
    new Cluster(env)
  }

  /** Connect to a Couchbase cluster with custom [[Credentials]].
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param credentials      custom credentials used when connecting to the cluster.
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, credentials: Credentials) = {
    new Cluster(ClusterEnvironment.create(connectionString, credentials))
  }

  /** Connect to a Couchbase cluster with a custom [[ClusterEnvironment]].
    *
    * $DeferredErrors
    *
    * @param environment the custom environment with its properties used to connect to the cluster.
    *
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(environment: ClusterEnvironment) = new Cluster(environment)
}
