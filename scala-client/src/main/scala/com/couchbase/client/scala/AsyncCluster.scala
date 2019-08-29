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
import com.couchbase.client.core.env.{Credentials, OwnedSupplier}
import com.couchbase.client.core.error.{AnalyticsException, QueryException}
import com.couchbase.client.core.msg.query.QueryChunkRow
import com.couchbase.client.core.msg.search.SearchRequest
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.analytics._
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.manager.user.{AsyncUserManager, ReactiveUserManager, UserManager}
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.query.handlers.{AnalyticsHandler, QueryHandler, SearchHandler}
import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.result.{SearchQueryRow, SearchResult}
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import com.couchbase.client.scala.util.{FunctionalUtil, FutureConversions, RowTraversalUtil}
import com.couchbase.client.scala.query.handlers.{AnalyticsHandler, QueryHandler, SearchHandler}
import com.couchbase.client.scala.search.SearchQuery
import com.couchbase.client.scala.search.result.{SearchQueryRow, SearchResult}
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import com.couchbase.client.scala.util.{FunctionalUtil, FutureConversions}
import com.couchbase.client.scala.query.handlers.{AnalyticsHandler, QueryHandler}
import reactor.core.scala.publisher.{Flux, Mono}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Represents a connection to a Couchbase cluster.
  *
  * This is the asynchronous version of the [[Cluster]] API.
  *
  * These can be created through the functions in the companion object, or through [[Cluster.async]].
  *
  * @param environment the environment used to create this
  * @param ec          an ExecutionContext to use for any Future.  Will be supplied automatically as long as
  *                    resources are
  *                    opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */
class AsyncCluster(environment: => ClusterEnvironment) {
  private[scala] implicit val ec: ExecutionContext = environment.ec
  private[scala] val core = Core.create(environment.coreEnv)
  private[scala] val env = environment
  private[scala] val kvTimeout = javaDurationToScala(env.timeoutConfig.kvTimeout())
  private[scala] val searchTimeout = javaDurationToScala(env.timeoutConfig.searchTimeout())
  private[scala] val analyticsTimeout = javaDurationToScala(env.timeoutConfig.analyticsTimeout())
  private[scala] val retryStrategy = env.retryStrategy
  private[scala] val queryHandler = new QueryHandler(core)
  private[scala] val analyticsHandler = new AnalyticsHandler()
  private[scala] val searchHandler = new SearchHandler()

  private[scala] val reactiveUserManager = new ReactiveUserManager(core)

  /** The AsyncUserManager provides programmatic access to and creation of users and groups. */
  val users = new AsyncUserManager(reactiveUserManager)

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param name the name of the bucket to open
    */
  def bucket(name: String): Future[AsyncBucket] = {
    FutureConversions.javaMonoToScalaFuture(core.openBucket(name))
      .map(v => new AsyncBucket(name, core, environment))
  }

  /** Performs a N1QL query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[QueryOptions]] for documentation
    *
    * @return a `Future` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(statement: String, options: QueryOptions = QueryOptions()): Future[QueryResult] = {
    queryHandler.queryAsync(statement, options, env)
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[AnalyticsOptions]] for documentation
    *
    * @return a `Future` containing a `Success(AnalyticsResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def analyticsQuery(statement: String, options: AnalyticsOptions = AnalyticsOptions()): Future[AnalyticsResult] = {

    analyticsHandler.request(statement, options, core, environment) match {
      case Success(request) =>
        core.send(request)

        import reactor.core.scala.publisher.{Mono => ScalaMono}

        val ret: Future[AnalyticsResult] = FutureConversions.javaCFToScalaMono(request, request.response(),
          propagateCancellation = true)
          .flatMap(response => FutureConversions.javaFluxToScalaFlux(response.rows())
            .collectSeq()
            .flatMap(rows =>

              FutureConversions.javaMonoToScalaMono(response.trailer())
                .map(trailer => AnalyticsResult(
                  rows,
                  AnalyticsMeta(
                    response.header().requestId(),
                    response.header().clientContextId().asScala,
                    response.header().signature.asScala.map(bytes => AnalyticsSignature(bytes)),
                    Some(AnalyticsMetrics.fromBytes(trailer.metrics)),
                    trailer.warnings.asScala.map(bytes => AnalyticsWarnings(bytes)),
                    trailer.status))
                )
            )
          )
          .onErrorResume(err => {
            err match {
              case e: AnalyticsException => ScalaMono.error(AnalyticsError(e.content))
              case _ => ScalaMono.error(err)
            }
          }).toFuture

        ret


      case Failure(err)
      => Future.failed(err)
    }
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param query           the FTS query to execute.  See [[SearchQuery]] for how to construct
    * @param timeout         when the operation will timeout.  This will default to `timeoutConfig().searchTimeout()` in the
    *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
    * @param retryStrategy   provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
    *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
    *
    * @return a `Future` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(query: SearchQuery,
                  timeout: Duration = searchTimeout,
                  retryStrategy: RetryStrategy = retryStrategy): Future[SearchResult] = {

    searchHandler.request(query, timeout, retryStrategy, core, environment) match {
      case Success(request) => AsyncCluster.searchQuery(request, core)
      case Failure(err) => Future.failed(err)
    }
  }


  /** Shutdown all cluster resources.
    *
    * This should be called before application exit.
    */
  def shutdown(): Future[Unit] = {
    FutureConversions.javaMonoToScalaMono(core.shutdown())
      .flatMap(_ => {
        if (env.owned) {
          env.shutdownReactive()
        }
        else {
          Mono.empty[Unit]
        }
      })
      .toFuture
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
    * @return a [[AsyncCluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, username: String, password: String): Future[AsyncCluster] = {
    Cluster.connect(connectionString, username, password) match {
      case Success(cluster) =>
        implicit val ec = cluster.ec
        Future {
          cluster.async
        }
      case Failure(err) => Future.failed(err)
    }
  }

  /** Connect to a Couchbase cluster with custom [[Credentials]].
    *
    * $DeferredErrors
    *
    * @param connectionString connection string used to locate the Couchbase cluster.
    * @param credentials      custom credentials used when connecting to the cluster.
    *
    * @return a [[AsyncCluster]] representing a connection to the cluster
    */
  def connect(connectionString: String, credentials: Credentials): Future[AsyncCluster] = {
    ClusterEnvironment.create(connectionString, credentials, true)
      .flatMap(env => Cluster.connect(env)) match {
      case Success(cluster) =>
        implicit val ec = cluster.ec
        Future {
          cluster.async
        }
      case Failure(err) => Future.failed(err)
    }
  }

  /** Connect to a Couchbase cluster with a custom [[ClusterEnvironment]].
    *
    * $DeferredErrors
    *
    * @param environment the custom environment with its properties used to connect to the cluster.
    *
    * @return a [[AsyncCluster]] representing a connection to the cluster
    */
  def connect(environment: ClusterEnvironment): Future[AsyncCluster] = {
    Cluster.connect(environment) match {
      case Success(cluster) =>
        implicit val ec = cluster.ec
        Future {
          cluster.async
        }
      case Failure(err) => Future.failed(err)
    }
  }

  private[client] def searchQuery(request: SearchRequest, core: Core): Future[SearchResult] = {
    core.send(request)

    val ret: Future[SearchResult] =
      FutureConversions.javaCFToScalaMono(request, request.response(), propagateCancellation = true)
        .flatMap(response => FutureConversions.javaFluxToScalaFlux(response.rows)
          // This can throw, which will return a failed Future as desired
          .map(row => SearchQueryRow.fromResponse(row))
          .collectSeq()
          .flatMap(rows =>

            FutureConversions.javaMonoToScalaMono(response.trailer())
              .map(trailer => {

                val rawStatus = response.header.getStatus
                val errors = SearchHandler.parseSearchErrors(rawStatus)
                val meta = SearchHandler.parseSearchMeta(response, trailer)

                SearchResult(
                  rows,
                  errors,
                  meta
                )
              })
          )
        )
        .toFuture

    ret
  }
}
