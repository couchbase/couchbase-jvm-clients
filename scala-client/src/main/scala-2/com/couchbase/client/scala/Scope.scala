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

import com.couchbase.client.core.annotation.{SinceCouchbase, Stability}
import com.couchbase.client.scala.analytics.{AnalyticsOptions, AnalyticsResult}
import com.couchbase.client.scala.manager.eventing.ScopeEventingFunctionManager
import com.couchbase.client.scala.manager.search.ScopeSearchIndexManager
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.util.AsyncUtils

import scala.util.Try

class Scope private[scala] (val async: AsyncScope, val bucketName: String) extends ScopeBase {

  /** Access a Reactive version of this API. */
  lazy val reactive: ReactiveScope = new ReactiveScope(async, bucketName)

  /** Allows managing scoped FTS indexes. */
  @SinceCouchbase("7.6")
  lazy val searchIndexes = new ScopeSearchIndexManager(async.searchIndexes)

  /** Allows managing eventing functions on this scope.
    *
    * For managing eventing functions at the admin scope ("*.*") level, see [[EventingFunctionManager]], accessed from
    * [[Cluster.eventingFunctions]].
    */
  @Stability.Uncommitted
  @SinceCouchbase("7.1")
  lazy val eventingFunctions = new ScopeEventingFunctionManager(async.eventingFunctions)

  /** Performs an Analytics query against the cluster.
    *
    * This is a blocking API.  See [[Scope.async]] for an Future-based async version of this API, and
    * [[Scope.reactive]] for a reactive version.  The reactive version includes backpressure-aware row streaming.
    *
    * The reason to use this Scope-based variant over `Cluster.analyticsQuery` is that it will automatically provide
    * the "query_context" parameter to the query service, allowing queries to be performed on collections
    * without having to fully specify their bucket and scope names in the query statement.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[com.couchbase.client.scala.analytics.AnalyticsOptions]] for documentation
    *
    * @return a `Try` containing a `Success(AnalyticsResult)` (which includes any returned rows) if successful,
    *         else a `Failure`
    */
  def analyticsQuery(
      statement: String,
      options: AnalyticsOptions = AnalyticsOptions.Default
  ): Try[AnalyticsResult] = {
    AsyncUtils.block(async.analyticsQuery(statement, options))
  }

  /** Performs a Full Text Search (FTS) query.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * Use this to access scoped FTS indexes, and [[Cluster.search]] for global indexes.
    *
    * This is blocking.  See [[Cluster.reactive]] for a reactive streaming version of this API, and
    * [[Cluster.async]] for an asynchronous version.
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
  ): Try[SearchResult] = {
    search(indexName, request, SearchOptions())
  }

  /** Performs a Full Text Search (FTS) query.
    *
    * This can be used to perform a traditional FTS query, and/or a vector search.
    *
    * Use this to access scoped FTS indexes, and [[Cluster.search]] for global indexes.
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
  @SinceCouchbase("7.6")
  def search(
      indexName: String,
      request: SearchRequest,
      options: SearchOptions
  ): Try[SearchResult] = {
    AsyncUtils.block(async.search(indexName, request, options))
  }
}
