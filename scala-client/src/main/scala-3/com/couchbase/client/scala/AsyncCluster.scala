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
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionStringUtil.{
  asConnectionString,
  checkConnectionString
}
import com.couchbase.client.scala.env.{ClusterEnvironment, PasswordAuthenticator, SeedNode}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import com.couchbase.client.scala.util.DurationConversions.*

import scala.jdk.CollectionConverters.*
import com.couchbase.client.core.Core
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
import com.couchbase.client.scala.diagnostics.{
  DiagnosticsOptions,
  PingOptions,
  WaitUntilReadyOptions
}
import com.couchbase.client.scala.env.{ClusterEnvironment, PasswordAuthenticator, SeedNode}
import com.couchbase.client.scala.query.{QueryOptions, QueryResult}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import scala.compat.java8.OptionConverters._

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import com.couchbase.client.scala.util.DurationConversions.*
import com.couchbase.client.scala.util.FutureConversions

import java.util.{Optional, UUID}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.*

class AsyncCluster private[scala] (
    private[scala] _env: => ClusterEnvironment,
    private[scala] val authenticator: Authenticator,
    private[scala] val connectionString: ConnectionString
) extends AsyncClusterBase {
  private[scala] lazy val environment = _env

  /** Performs a N1QL query against the cluster.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `Future` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(
      statement: String,
      options: QueryOptions = QueryOptions.Default
  ): Future[QueryResult] = {
    convert(queryOps.queryAsync(statement, options.toCore, null, null, null))
      .map(result => convert(result))
  }

  /** Performs a Full Text Search (FTS) query against the cluster.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
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
      options: SearchOptions = SearchOptions.Default
  ): Future[SearchResult] = {
    request.toCore match {
      case Failure(err) => Future.failed(err)
      case Success(req) =>
        convert(searchOps.searchAsync(indexName, req, options.toCore))
          .map(result => SearchResult(result))
    }
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
    * @param options options to customize the report generation
    *
    * @return a `DiagnosticsResult`
    */
  def diagnostics(
      options: DiagnosticsOptions = DiagnosticsOptions.Default
  ): Future[DiagnosticsResult] = {
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
    * @param options options to customize the ping
    *
    * @return the `PingResult` once complete.
    */
  def ping(options: PingOptions = PingOptions.Default): Future[PingResult] = {
    couchbaseOps match {
      case core: Core =>
        val future = HealthPinger
          .ping(
            core,
            options.timeout.map(scalaDurationToJava).asJava,
            options.retryStrategy.getOrElse(env.retryStrategy),
            if (options.serviceTypes.isEmpty) null else options.serviceTypes.asJava,
            options.reportId.asJava,
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
    * @param timeout the maximum time to wait until readiness.
    * @param options options to customize the wait
    */
  def waitUntilReady(
      timeout: Duration,
      options: WaitUntilReadyOptions = WaitUntilReadyOptions.Default
  ): Future[Unit] = {
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

object AsyncCluster {
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
