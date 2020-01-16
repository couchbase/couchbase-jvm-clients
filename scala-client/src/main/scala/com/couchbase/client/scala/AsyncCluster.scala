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

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.diagnostics._
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.error.ErrorCodeAndMessage
import com.couchbase.client.core.msg.search.SearchRequest
import com.couchbase.client.core.retry.{FailFastRetryStrategy, RetryStrategy}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.ConnectionStringUtil
import com.couchbase.client.scala.analytics._
import com.couchbase.client.scala.env.{ClusterEnvironment, PasswordAuthenticator, SeedNode}
import com.couchbase.client.scala.manager.analytics.{
  AsyncAnalyticsIndexManager,
  ReactiveAnalyticsIndexManager
}
import com.couchbase.client.scala.manager.bucket.{AsyncBucketManager, ReactiveBucketManager}
import com.couchbase.client.scala.manager.query.AsyncQueryIndexManager
import com.couchbase.client.scala.manager.search.AsyncSearchIndexManager
import com.couchbase.client.scala.manager.user.{AsyncUserManager, ReactiveUserManager}
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.query.handlers.{AnalyticsHandler, QueryHandler, SearchHandler}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.{SearchResult, SearchRow}
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import reactor.core.scala.publisher.SMono

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import com.couchbase.client.scala.util.DurationConversions._

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
class AsyncCluster(
    environment: => ClusterEnvironment,
    private[scala] val authenticator: Authenticator,
    private[scala] val seedNodes: Set[SeedNode]
) {
  private[scala] implicit val ec: ExecutionContext = environment.ec
  private[scala] val core =
    Core.create(environment.coreEnv, authenticator, seedNodes.map(_.toCore).asJava)
  private[scala] val env                        = environment
  private[scala] val hp                         = HandlerBasicParams(core, env)
  private[scala] val searchTimeout              = javaDurationToScala(env.timeoutConfig.searchTimeout())
  private[scala] val analyticsTimeout           = javaDurationToScala(env.timeoutConfig.analyticsTimeout())
  private[scala] val retryStrategy              = env.retryStrategy
  private[scala] val queryHandler               = new QueryHandler(hp)
  private[scala] val analyticsHandler           = new AnalyticsHandler(hp)
  private[scala] val searchHandler              = new SearchHandler(hp)
  private[scala] lazy val reactiveUserManager   = new ReactiveUserManager(core)
  private[scala] lazy val reactiveBucketManager = new ReactiveBucketManager(core)
  private[scala] lazy val reactiveAnalyticsIndexManager = new ReactiveAnalyticsIndexManager(
    new ReactiveCluster(this)
  )

  /** The AsyncBucketManager provides access to creating and getting buckets. */
  @Stability.Volatile
  lazy val buckets = new AsyncBucketManager(reactiveBucketManager)

  /** The AsyncUserManager provides programmatic access to and creation of users and groups. */
  @Stability.Volatile
  lazy val users = new AsyncUserManager(reactiveUserManager)

  @Stability.Volatile
  lazy val queryIndexes = new AsyncQueryIndexManager(this)

  @Stability.Volatile
  lazy val searchIndexes = new AsyncSearchIndexManager(this)

  @Stability.Volatile
  lazy val analyticsIndexes = new AsyncAnalyticsIndexManager(reactiveAnalyticsIndexManager)

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param bucketName the name of the bucket to open
    */
  def bucket(bucketName: String): AsyncBucket = {
    core.openBucket(bucketName)
    new AsyncBucket(bucketName, core, environment)
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
  def analyticsQuery(
      statement: String,
      options: AnalyticsOptions = AnalyticsOptions()
  ): Future[AnalyticsResult] = {

    analyticsHandler.request(statement, options, core, environment) match {
      case Success(request) =>
        core.send(request)

        val ret: Future[AnalyticsResult] = FutureConversions
          .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
          .flatMap(
            response =>
              FutureConversions
                .javaFluxToScalaFlux(response.rows())
                .collectSeq()
                .flatMap(
                  rows =>
                    FutureConversions
                      .javaMonoToScalaMono(response.trailer())
                      .map(trailer => {
                        val warnings: collection.Seq[AnalyticsWarning] = trailer.warnings.asScala
                          .map(
                            warnings =>
                              ErrorCodeAndMessage
                                .fromJsonArray(warnings)
                                .asScala
                                .map(codeAndMessage => AnalyticsWarning(codeAndMessage))
                          )
                          .getOrElse(Seq.empty)

                        AnalyticsResult(
                          rows,
                          AnalyticsMetaData(
                            response.header().requestId(),
                            response.header().clientContextId().orElse(""),
                            response.header().signature.asScala,
                            AnalyticsMetrics.fromBytes(trailer.metrics),
                            warnings,
                            AnalyticsStatus.from(trailer.status)
                          )
                        )
                      })
                )
          )
          .toFuture

        ret.onComplete(_ => request.context.logicallyComplete())
        ret

      case Failure(err) => Future.failed(err)
    }
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.
    *
    * @param indexName         the name of the search index to use
    * @param query             the FTS query to execute.  See
    *                          [[com.couchbase.client.scala.search.queries.SearchQuery]] for more
    * @param options           the FTS query to execute.  See [[SearchOptions]] for how to construct
    *
    * @return a `Future` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def searchQuery(
      indexName: String,
      query: SearchQuery,
      options: SearchOptions = SearchOptions()
  ): Future[SearchResult] = {

    searchHandler.request(indexName, query, options, core, environment) match {
      case Success(request) => AsyncCluster.searchQuery(request, core)
      case Failure(err)     => Future.failed(err)
    }
  }

  /** Shutdown all cluster resources.
    *
    * This should be called before application exit.
    */
  def disconnect(): Future[Unit] = {
    FutureConversions
      .javaMonoToScalaMono(core.shutdown(env.timeoutConfig.disconnectTimeout()))
      .`then`(SMono.defer(() => {
        if (env.owned) {
          env.shutdownReactive(env.timeoutConfig.disconnectTimeout())
        } else {
          SMono.empty[Unit]
        }
      }))
      .timeout(env.timeoutConfig.disconnectTimeout())
      .toFuture
  }

  /** Returns a [[DiagnosticsResult]], reflecting the SDK's current view of all its existing connections to the
    * cluster.
    *
    * @param reportId        this will be returned in the [[DiagnosticsResult]].  If not specified it defaults to a UUID.
    *
    * @return a { @link DiagnosticsResult}
    */
  def diagnostics(reportId: String = UUID.randomUUID.toString): Future[DiagnosticsResult] = {
    Future(
      new DiagnosticsResult(
        core.diagnostics.collect(
          Collectors
            .groupingBy[EndpointDiagnostics, ServiceType]((v1: EndpointDiagnostics) => v1.`type`())
        ),
        core.context().environment().userAgent().formattedShort(),
        reportId
      )
    )
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
  ): Future[PingResult] = {

    import scala.collection.JavaConverters._

    val future = HealthPinger
      .ping(
        core,
        timeout.map(scalaDurationToJava).asJava,
        retryStrategy,
        if (serviceTypes.isEmpty) null else serviceTypes.asJava,
        reportId.asJava,
        true
      )
      .toFuture

    FutureConversions.javaCFToScalaFuture(future)
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
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFuture(
        WaitUntilReadyHelper.waitUntilReady(
          core,
          if (serviceTypes.isEmpty) null else serviceTypes.asJava,
          timeout,
          desiredState,
          true
        )
      )
      .map(_ => ())
  }

  private[scala] def performGlobalConnect() {
    core.initGlobalConfig()
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

  /** Connect to a Couchbase cluster with custom [[Authenticator]].
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
        val seedNodes = if (options.seedNodes.isDefined) {
          options.seedNodes.get
        } else {
          seedNodesFromConnectionString(connectionString, ce)
        }
        val cluster = new AsyncCluster(ce, options.authenticator, seedNodes)
        cluster.performGlobalConnect()
        cluster
      })
  }

  private[client] def searchQuery(request: SearchRequest, core: Core)(
      implicit ec: ExecutionContext
  ): Future[SearchResult] = {
    core.send(request)

    val ret: Future[SearchResult] =
      FutureConversions
        .javaCFToScalaMono(request, request.response(), propagateCancellation = true)
        .flatMap(
          response =>
            FutureConversions
              .javaFluxToScalaFlux(response.rows())
              // This can throw, which will return a failed Future as desired
              .map(row => SearchRow.fromResponse(row))
              .collectSeq()
              .flatMap(
                rows =>
                  FutureConversions
                    .javaMonoToScalaMono(response.trailer())
                    .map(
                      trailer =>
                        SearchResult(
                          rows,
                          SearchHandler.parseSearchFacets(trailer),
                          SearchHandler.parseSearchMeta(response, trailer)
                        )
                    )
              )
        )
        .toFuture
    ret.onComplete(_ => request.context.logicallyComplete())

    ret
  }

  private[client] def extractClusterEnvironment(
      connectionString: String,
      opts: ClusterOptions
  ): Try[ClusterEnvironment] = {
    opts.environment match {
      case Some(env) => Success(env)
      case _         => ClusterEnvironment.Builder(owned = true).connectionString(connectionString).build
    }
  }

  private[client] def seedNodesFromConnectionString(
      cs: String,
      environment: ClusterEnvironment
  ): Set[SeedNode] = {
    ConnectionStringUtil
      .seedNodesFromConnectionString(
        cs,
        environment.coreEnv.ioConfig.dnsSrvEnabled,
        environment.coreEnv.securityConfig.tlsEnabled,
        environment.coreEnv.eventBus
      )
      .asScala
      .map(sn => SeedNode.fromCore(sn))
      .toSet
  }
}
