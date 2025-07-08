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
import com.couchbase.client.core.api.query.CoreQueryContext
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.query.{QueryOptions, QueryResult}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.util.CoreCommonConverters.convert

import java.util.Optional
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

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
trait AsyncScopeBase { this: AsyncScope =>

  private[scala] implicit val ec: ExecutionContext = environment.ec
  private[scala] val queryOps                      = couchbaseOps.queryOps()
  private[scala] val searchOps =
    couchbaseOps.searchOps(new CoreBucketAndScope(bucketName, name))

  /** The name of this scope. */
  def name: String = this.scopeName

  /** Opens and returns the default collection on this scope. */
  private[scala] def defaultCollection: AsyncCollection =
    collection(DefaultResources.DefaultCollection)

  /** Opens and returns a Couchbase collection resource, that exists on this scope. */
  def collection(collectionName: String): AsyncCollection = {
    couchbaseOps match {
      case core: Core =>
        val defaultScopeAndCollection = collectionName.equals(DefaultResources.DefaultCollection) &&
          name.equals(DefaultResources.DefaultScope)

        if (!defaultScopeAndCollection) {
          core.configurationProvider.refreshCollectionId(
            new CollectionIdentifier(
              bucketName,
              Optional.of(name),
              Optional.of(collectionName)
            )
          )
        }
      case _ =>
    }

    new AsyncCollection(collectionName, bucketName, name, couchbaseOps, environment)
  }

  /** Performs a N1QL query against the cluster.
    *
    * This is asynchronous.  See [[Scope.reactive]] for a reactive streaming version of this API, and
    * [[Scope]] for a blocking version.  The reactive version includes backpressure-aware row streaming.
    *
    * The reason to use this Scope-based variant over `AsyncCluster.query` is that it will automatically provide
    * the "query_context" parameter to the query service, allowing queries to be performed on collections
    * without having to fully specify their bucket and scope names in the query statement.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `Future` containing a `Success(QueryResult)` (which includes any returned rows) if successful, else a
    *         `Failure`
    */
  def query(statement: String, options: QueryOptions = QueryOptions()): Future[QueryResult] = {
    convert(
      queryOps.queryAsync(
        statement,
        options.toCore,
        CoreQueryContext.of(bucketName, name),
        null,
        null
      )
    ).map(result => convert(result))
  }
}
