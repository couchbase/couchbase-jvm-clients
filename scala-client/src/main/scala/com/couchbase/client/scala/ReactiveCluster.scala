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
import com.couchbase.client.core.error.{AnalyticsServiceException, QueryException}
import com.couchbase.client.core.msg.query.QueryChunkRow
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.analytics._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.query.handlers.SearchHandler
import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.result.{ReactiveSearchResult, SearchMeta}
import com.couchbase.client.scala.util.FutureConversions
import io.opentracing.Span
import reactor.core.scala.publisher.{Flux => ScalaFlux, Mono => ScalaMono}

import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

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
  private val env = async.env

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
  def query(statement: String, options: QueryOptions = QueryOptions()): ScalaMono[ReactiveQueryResult] = {
    async.queryHandler.queryReactive(statement, options, env)
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[AnalyticsOptions]] for documentation
    *
    * @return a `Mono` containing a [[ReactiveAnalyticsResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def analyticsQuery(statement: String, options: AnalyticsOptions = AnalyticsOptions())
  : ScalaMono[ReactiveAnalyticsResult] = {
    async.analyticsHandler.request(statement, options, async.core, async.env) match {
      case Success(request) =>

        async.core.send(request)

        FutureConversions.javaCFToScalaMono(request, request.response(), false)
          .map(response => {
            val meta: ScalaMono[AnalyticsMeta] = FutureConversions.javaMonoToScalaMono(response.trailer())
              .map(trailer => {
                AnalyticsMeta(
                  response.header().requestId(),
                  response.header().clientContextId().asScala,
                  response.header().signature().asScala.map(AnalyticsSignature),
                  Some(AnalyticsMetrics.fromBytes(trailer.metrics())),
                  trailer.warnings.asScala.map(AnalyticsWarnings),
                  trailer.status
                )
              })

            val rows = FutureConversions.javaFluxToScalaFlux(response.rows())

            ReactiveAnalyticsResult(
              rows,
              meta
            )
          })

      case Failure(err) => ScalaMono.error(err)
    }
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is a reactive API.  See [[Cluster.async]] for an asynchronous version of this API, and
    * [[Cluster]] for a blocking version.
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
    * @return a `Mono` containing a [[ReactiveSearchResult]] which includes a Flux giving streaming access to any
    *         returned rows
    */
  def searchQuery(query: SearchQuery,
                  parentSpan: Option[Span] = None,
                  timeout: Duration = async.searchTimeout,
                  retryStrategy: RetryStrategy = async.retryStrategy): ScalaMono[ReactiveSearchResult] = {
    async.searchHandler.request(query, parentSpan, timeout, retryStrategy, async.core, async.env) match {
      case Success(request) =>

        async.core.send(request)

        FutureConversions.javaCFToScalaMono(request, request.response(), false)
          .map(response => {
            val meta: ScalaMono[SearchMeta] = FutureConversions.javaMonoToScalaMono(response.trailer())
              .map(trailer => {
                val rawStatus = response.header.getStatus
                val errors = SearchHandler.parseSearchErrors(rawStatus)
                // TODO errors need to be raised...
                val meta = SearchHandler.parseSearchMeta(response, trailer)

                meta
              })

            val rows = FutureConversions.javaFluxToScalaFlux(response.rows())

            ReactiveSearchResult(
              rows,
              meta
            )
          })

      case Failure(err) =>
        ScalaMono.error(err)
    }
  }

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param name the name of the bucket to open
    */
  def bucket(name: String): ScalaMono[ReactiveBucket] = {
    ScalaMono.fromFuture(async.bucket(name)).map(v => new ReactiveBucket(v))
  }

  /** Shutdown all cluster resources.search
    *
    * This should be called before application exit.
    */
  def shutdown(): ScalaMono[Unit] = {
    FutureConversions.javaMonoToScalaMono(async.core.shutdown())
      .then(ScalaMono.defer(() => {
        if (env.owned) {
          ScalaMono.fromRunnable(new Runnable {
            override def run(): Unit = env.shutdown()
          })
        }
        else {
          ScalaMono.empty[Unit]
        }
      }))
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
    * @return a Mono[ReactiveCluster] representing a connection to the cluster
    */
  def connect(connectionString: String, username: String, password: String): ScalaMono[ReactiveCluster] = {
    val cluster = Cluster.connect(connectionString, username, password)
    implicit val ec = cluster.ec

    ScalaMono.just(new ReactiveCluster(cluster.async))
  }

  /** Connect to a Couchbase cluster with custom [[Credentials]].
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param credentials      custom credentials used when connecting to the cluster.
    *
    * @return a Mono[ReactiveCluster] representing a connection to the cluster
    */
  def connect(connectionString: String, credentials: Credentials): ScalaMono[ReactiveCluster] = {
    val cluster = Cluster.connect(connectionString, credentials)
    implicit val ec = cluster.ec
    ScalaMono.just(new ReactiveCluster(cluster.async))
  }

  /** Connect to a Couchbase cluster with a custom [[ClusterEnvironment]].
    *
    * $DeferredErrors
    *
    * @param environment the custom environment with its properties used to connect to the cluster.
    *
    * @return a Mono[ReactiveCluster] representing a connection to the cluster
    */
  def connect(environment: ClusterEnvironment): ScalaMono[ReactiveCluster] = {
    val cluster = Cluster.connect(environment)
    implicit val ec = cluster.ec
    ScalaMono.just(new ReactiveCluster(cluster.async))
  }
}