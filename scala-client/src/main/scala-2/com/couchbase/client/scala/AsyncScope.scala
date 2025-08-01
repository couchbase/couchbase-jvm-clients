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
import com.couchbase.client.core.annotation.{SinceCouchbase, Stability}
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.api.manager.CoreBucketAndScope
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.scala.analytics.{AnalyticsOptions, AnalyticsResult}
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.handlers.AnalyticsHandler
import com.couchbase.client.scala.manager.eventing.AsyncScopeEventingFunctionManager
import com.couchbase.client.scala.manager.search.AsyncScopeSearchIndexManager
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.util.CoreCommonConverters.convert

import scala.concurrent.Future
import scala.util.{Failure, Success}

class AsyncScope private[scala] (
    private[scala] val scopeName: String,
    val bucketName: String,
    private[scala] val couchbaseOps: CoreCouchbaseOps,
    private[scala] val environment: ClusterEnvironment
) extends AsyncScopeBase {
  private val coreScope = new CoreBucketAndScope(bucketName, this.name)

  /** Allows managing scoped FTS indexes. */
  @SinceCouchbase("7.6")
  lazy val searchIndexes =
    new AsyncScopeSearchIndexManager(coreScope, couchbaseOps)

  /** Allows managing eventing functions on this scope.
    *
    * For managing eventing functions at the admin scope ("*.*") level, see [[com.couchbase.client.scala.manager.eventing.AsyncEventingFunctionManager]], accessed from
    * [[AsyncCluster.eventingFunctions]].
    */
  @Stability.Uncommitted
  @SinceCouchbase("7.1")
  lazy val eventingFunctions =
    new AsyncScopeEventingFunctionManager(environment, couchbaseOps, coreScope)

  /** Performs an Analytics query against the cluster.
    *
    * This is asynchronous.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster]] for a blocking version.  The reactive version includes backpressure-aware row streaming.
    *
    * The reason to use this Scope-based variant over `AsyncCluster.analyticsQuery` is that it will automatically provide
    * the "query_context" parameter to the query service, allowing queries to be performed on collections
    * without having to fully specify their bucket and scope names in the query statement.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[com.couchbase.client.scala.analytics.AnalyticsOptions]] for documentation
    *
    * @return a `Future` containing a `Success(AnalyticsResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def analyticsQuery(
      statement: String,
      options: AnalyticsOptions = AnalyticsOptions.Default
  ): Future[AnalyticsResult] = {
    couchbaseOps match {
      case core: Core =>
        val hp               = HandlerBasicParams(core)
        val analyticsHandler = new AnalyticsHandler(hp)

        analyticsHandler.request(
          statement,
          options,
          core,
          environment,
          Some(bucketName),
          Some(scopeName)
        ) match {
          case Success(request) => analyticsHandler.queryAsync(request)
          case Failure(err)     => Future.failed(err)
        }
      case _ => Future.failed(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }
  }

  /** Performs a Full Text Search (FTS) query.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * Use this to access scoped FTS indexes, and [[Cluster.search]] for global indexes.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  @SinceCouchbase("7.6")
  def search(
      indexName: String,
      request: SearchRequest
  ): Future[SearchResult] = {
    search(indexName, request, SearchOptions())
  }

  /** Performs a Full Text Search (FTS) query.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * Use this to access scoped FTS indexes, and [[Cluster.search]] for global indexes.
    *
    * @param indexName the name of the search index to use
    * @param request   the request to send to the FTS service.
    * @param options   see [[com.couchbase.client.scala.search.SearchOptions]]
    * @return a `Try` containing a `Success(SearchResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  @SinceCouchbase("7.6")
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
}
