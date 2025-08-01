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
import com.couchbase.client.core.api.query.CoreQueryContext
import com.couchbase.client.scala.query.{QueryOptions, QueryResult}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.util.AsyncUtils
import com.couchbase.client.scala.util.CoreCommonConverters.convert

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Represents a Couchbase scope resource.
  *
  * Applications should not create these manually, but instead first open a [[Cluster]] and through that a
  * [[Bucket]].
  *
  * @param async an asynchronous version of this API
  * @param bucketName the name of the bucket this scope is on
  * @author Graham Pople
  * @since 1.0.0
  */
trait ScopeBase { this: Scope =>
  private[scala] implicit val ec: ExecutionContext = async.ec

  /** The name of this scope. */
  def name: String = async.name

  /** Opens and returns a Couchbase collection resource, that exists on this scope. */
  def collection(collectionName: String): Collection = {
    new Collection(async.collection(collectionName), bucketName)
  }

  /** Opens and returns the default collection on this scope. */
  private[scala] def defaultCollection = {
    collection(DefaultResources.DefaultCollection)
  }

  /** Performs a N1QL query against the cluster.
    *
    * The reason to use this Scope-based variant over `Cluster.query` is that it will automatically provide
    * the "query_context" parameter to the query service, allowing queries to be specified on scopes and collections
    * without having to fully reference them in the query statement.
    *
    * @param statement the N1QL statement to execute
    * @param options   any query options - see [[com.couchbase.client.scala.query.QueryOptions]] for documentation
    *
    * @return a `QueryResult`
    */
  def query(statement: String, options: QueryOptions = QueryOptions()): Try[QueryResult] = {
    Try(
      async.queryOps
        .queryBlocking(statement, options.toCore, CoreQueryContext.of(bucketName, name), null, null)
    ).map(result => convert(result))
  }
}
