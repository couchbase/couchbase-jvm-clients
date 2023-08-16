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
import com.couchbase.client.core.api.query.CoreQueryContext
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.scala.analytics.{AnalyticsOptions, ReactiveAnalyticsResult}
import com.couchbase.client.scala.query.handlers.AnalyticsHandler
import com.couchbase.client.scala.query.{QueryOptions, ReactiveQueryResult}
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import reactor.core.scala.publisher.SMono

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Represents a Couchbase scope resource.
  *
  * This is a reactive version of the [[Scope]] interface.
  *
  * Applications should not create these manually, but instead first open a [[Cluster]] and through that a
  * [[Bucket]].
  *
  * @param bucketName the name of the bucket this scope is on
  * @author Graham Pople
  * @since 1.0.0
  */
class ReactiveScope(async: AsyncScope, bucketName: String) {
  private[scala] implicit val ec: ExecutionContext = async.ec

  /** The name of this scope. */
  def name = async.name

  /** Opens and returns the default collection on this scope. */
  private[scala] def defaultCollection: ReactiveCollection = {
    collection(DefaultResources.DefaultCollection)
  }

  /** Opens and returns a Couchbase collection resource, that exists on this scope. */
  def collection(collectionName: String): ReactiveCollection = {
    new ReactiveCollection(async.collection(collectionName))
  }

  /** Performs a N1QL query against the cluster.
    *
    * This is a reactive API.  See [[Scope.async]] for an Future-based async version of this API, and
    * [[Scope]] for a blocking version.
    *
    * The reason to use this Scope-based variant over `ReactiveCluster.query` is that it will automatically provide
    * the "query_context" parameter to the query service, allowing queries to be specified on scopes and collections
    * without having to fully reference them in the query statement.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `ReactiveQueryResult`
    */
  def query(
      statement: String,
      options: QueryOptions = QueryOptions()
  ): SMono[ReactiveQueryResult] = {
    convert(
      async.queryOps
        .queryReactive(statement, options.toCore, CoreQueryContext.of(bucketName, name), null, null)
    ).map(result => convert(result))
  }

  /** Performs an Analytics query against the cluster.
    *
    * This is a blocking API.  See [[Scope.async]] for an Future-based async version of this API, and
    * [[Scope.reactive]] for a reactive version.  The reactive version includes backpressure-aware row streaming.
    *
    * The reason to use this Scope-based variant over `ReactiveCluster.analyticsQuery` is that it will automatically provide
    * the "query_context" parameter to the query service, allowing queries to be performed on collections
    * without having to fully specify their bucket and scope names in the query statement.
    *
    * @param statement the Analytics query to execute
    * @param options   any query options - see [[com.couchbase.client.scala.analytics.AnalyticsOptions]] for documentation
    *
    * @return a `ReactiveAnalyticsResult`
    */
  def analyticsQuery(
      statement: String,
      options: AnalyticsOptions = AnalyticsOptions.Default
  ): SMono[ReactiveAnalyticsResult] = {
    async.couchbaseOps match {
      case core: Core =>
        val hp               = HandlerBasicParams(core)
        val analyticsHandler = new AnalyticsHandler(hp)

        analyticsHandler.request(
          statement,
          options,
          core,
          async.environment,
          Some(bucketName),
          Some(name)
        ) match {
          case Success(request) => analyticsHandler.queryReactive(request)
          case Failure(err)     => SMono.error(err)
        }
      case _ => SMono.error(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }
  }
}
