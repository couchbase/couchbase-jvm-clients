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
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.handlers.QueryHandler
import com.couchbase.client.scala.query.{QueryOptions, QueryResult}

import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}

/** Represents a Couchbase scope resource.
  *
  * This is an asynchronous version of the [[Scope]] interface.
  *
  * Applications should not create these manually, but instead first open a [[Cluster]] and through that a
  * [[Bucket]].
  *
  * @param bucketName the name of the bucket this scope is on
  * @param ec an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *           opened in the normal way, starting from functions in [[Cluster]]
  * @author Graham Pople
  * @since 1.0.0
  */
@Volatile
class AsyncScope private[scala] (
    scopeName: String,
    bucketName: String,
    private val core: Core,
    private[scala] val environment: ClusterEnvironment
) {
  private[scala] implicit val ec: ExecutionContext = environment.ec
  private[scala] val hp                            = HandlerBasicParams(core, environment)
  private[scala] val queryHandler                  = new QueryHandler(hp)

  /** The name of this scope. */
  def name = scopeName

  /** Opens and returns the default collection on this scope. */
  private[scala] def defaultCollection: AsyncCollection =
    collection(DefaultResources.DefaultCollection)

  /** Opens and returns a Couchbase collection resource, that exists on this scope. */
  def collection(collectionName: String): AsyncCollection = {
    val defaultScopeAndCollection = collectionName.equals(DefaultResources.DefaultCollection) &&
      scopeName.equals(DefaultResources.DefaultScope)

    if (!defaultScopeAndCollection) {
      core.configurationProvider.refreshCollectionId(
        new CollectionIdentifier(bucketName, Optional.of(scopeName), Optional.of(collectionName))
      )
    }

    new AsyncCollection(collectionName, bucketName, scopeName, core, environment)
  }

  /** Performs a N1QL query against the cluster.
    *
    * This is asynchronous.  See [[Scope.reactive]] for a reactive streaming version of this API, and
    * [[Scope]] for a blocking version.
    *
    * The reason to use this Scope-based variant over `AsyncCluster.query` is that it will automatically provide
    * the "query_context" parameter to the query service, allowing queries to be specified on scopes and collections
    * without having to fully reference them in the query statement.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `Future` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  @Volatile
  def query(statement: String, options: QueryOptions = QueryOptions()): Future[QueryResult] = {
    queryHandler.queryAsync(statement, options, environment, Some(bucketName), Some(scopeName))
  }
}
