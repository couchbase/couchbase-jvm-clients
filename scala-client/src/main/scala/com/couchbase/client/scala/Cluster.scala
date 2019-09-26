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

import com.couchbase.client.core.env.{Authenticator, OwnedSupplier, PasswordAuthenticator}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.AsyncCluster.{connect, seedNodesFromConnectionString}
import com.couchbase.client.scala.analytics.{AnalyticsOptions, AnalyticsResult}
import com.couchbase.client.scala.env.{ClusterEnvironment, SeedNode}
import com.couchbase.client.scala.manager.user.{AsyncUserManager, ReactiveUserManager, UserManager}
import com.couchbase.client.scala.manager.bucket.{AsyncBucketManager, BucketManager, ReactiveBucketManager}
import com.couchbase.client.scala.query.{QueryOptions, QueryResult}
import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._

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
class Cluster private[scala](env: => ClusterEnvironment, authenticator: Authenticator, seedNodes: Set[SeedNode]) {

  private[scala] implicit val ec: ExecutionContext = env.ec

  /** Access an asynchronous version of this API. */
  val async = new AsyncCluster(env, authenticator, seedNodes)

  /** Access a reactive version of this API. */
  lazy val reactive = new ReactiveCluster(async)

  private[scala] val reactiveUserManager = new ReactiveUserManager(async.core)
  private[scala] val asyncUserManager = new AsyncUserManager(reactiveUserManager)
  private val reactiveBucketManager = new ReactiveBucketManager(async.core)
  private val asyncBucketManager = new AsyncBucketManager(reactiveBucketManager)

  /** The UserManager provides programmatic access to and creation of users and groups. */
  val users = new UserManager(asyncUserManager, reactiveUserManager)

  /** The BucketManager provides access to creating and getting buckets. */
  val buckets = new BucketManager(asyncBucketManager)

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
    * @param timeout         when the operation will timeout.  This will default to `timeoutConfig().searchTimeout()` in the
    *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
    * @param retryStrategy   provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(query: SearchQuery,
                  timeout: Duration = async.searchTimeout,
                  retryStrategy: RetryStrategy = async.retryStrategy): Try[SearchResult] = {
    AsyncUtils.block(async.searchQuery(query, timeout, retryStrategy))
  }

  /** Shutdown all cluster resources.
    *
    * This should be called before application exit.
    */
  def disconnect(): Unit = {
    reactive.disconnect().block()
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
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, username: String, password: String): Try[Cluster] = {
    connect(connectionString, ClusterOptions(PasswordAuthenticator.create(username, password)))
  }

  /** Connect to a Couchbase cluster with custom [[Authenticator]].
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param options custom options used when connecting to the cluster.
    * @return a [[Cluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, options: ClusterOptions): Try[Cluster] = {
    AsyncCluster.extractClusterEnvironment(connectionString, options)
      .map(ce => {
        val seedNodes = seedNodesFromConnectionString(connectionString, ce)
        new Cluster(ce, options.authenticator, seedNodes)
      })
  }
}

