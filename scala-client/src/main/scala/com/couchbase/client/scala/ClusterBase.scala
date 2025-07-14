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

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.annotation.Stability.Uncommitted
import com.couchbase.client.core.diagnostics._
import com.couchbase.client.core.env.{Authenticator, PasswordAuthenticator}
import com.couchbase.client.core.transaction.CoreTransactionsReactive
import com.couchbase.client.core.util.ConnectionString
import com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString
import com.couchbase.client.scala.diagnostics.{
  DiagnosticsOptions,
  PingOptions,
  WaitUntilReadyOptions
}
import com.couchbase.client.scala.env.{ClusterEnvironment, SeedNode}
import com.couchbase.client.scala.query.{QueryOptions, QueryParameters, QueryResult}
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.transactions.Transactions
import com.couchbase.client.scala.transactions.config.TransactionsConfig
import com.couchbase.client.scala.util.AsyncUtils
import com.couchbase.client.scala.util.CoreCommonConverters.convert

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Represents a connection to a Couchbase cluster.
  *
  * These can be created through the functions in the companion object.
  *
  * @param _env the environment used to create this
  * @param ec  an ExecutionContext to use for any Future.  Will be supplied automatically as long as resources are
  *            opened in the normal way, starting from functions in [[Cluster]]
  *
  * @author Graham Pople
  * @since 1.0.0
  */

trait ClusterBase { this: Cluster =>
  private[scala] implicit val ec: ExecutionContext = environment.ec

  /** Access an asynchronous version of this API. */
  val async = new AsyncCluster(environment, authenticator, connectionString)

  lazy val transactions = new Transactions(
    new CoreTransactionsReactive(
      async.core,
      env.transactionsConfig.map(v => v.toCore).getOrElse(TransactionsConfig().toCore)
    ),
    env
  )

  /** The environment used to create this cluster */
  def env: ClusterEnvironment = environment

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param bucketName the name of the bucket to open
    */
  def bucket(bucketName: String): Bucket = {
    new Bucket(async.bucket(bucketName))
  }

}
