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
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.annotation.Stability.Uncommitted
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.diagnostics._
import com.couchbase.client.core.env.{Authenticator, DelegatingAuthenticator}
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.transaction.CoreTransactionsReactive
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
import com.couchbase.client.scala.query._
import com.couchbase.client.scala.search.SearchOptions
import com.couchbase.client.scala.search.queries.SearchQuery
import com.couchbase.client.scala.search.result.SearchResult
import com.couchbase.client.scala.search.vector.SearchRequest
import com.couchbase.client.scala.transactions.AsyncTransactions
import com.couchbase.client.scala.transactions.config.TransactionsConfig
import com.couchbase.client.scala.util.CoreCommonConverters.convert
import com.couchbase.client.scala.util.DurationConversions.{javaDurationToScala, _}
import com.couchbase.client.scala.util.FutureConversions

import java.util.stream.Collectors
import java.util.{Optional, UUID}
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Represents a connection to a Couchbase cluster.
  *
  * This is the asynchronous version of the [[Cluster]] API.
  *
  * These can be created through the functions in the companion object, or through [[Cluster.async]].
  *
  * @param environment the environment used to create this
  *
  * @author Graham Pople
  * @since 1.0.0
  */
trait AsyncClusterBase { this: AsyncCluster =>
  private[scala] implicit lazy val ec: ExecutionContext = environment.ec

  /** The environment used to create this cluster */
  val env: ClusterEnvironment = environment

  private[scala] val couchbaseOps =
    CoreCouchbaseOps.create(environment.coreEnv, initialAuthenticator, connectionString)

  // Only used by tests now
  private[couchbase] def core: Core = couchbaseOps match {
    case core: Core => core
    case _          => throw CoreProtostellarUtil.unsupportedCurrentlyInProtostellar()
  }

  private[scala] val searchTimeout    = javaDurationToScala(env.timeoutConfig.searchTimeout())
  private[scala] val analyticsTimeout = javaDurationToScala(env.timeoutConfig.analyticsTimeout())
  private[scala] val retryStrategy    = env.retryStrategy
  private[scala] val searchOps        = couchbaseOps.searchOps(null)
  private[scala] val queryOps         = couchbaseOps.queryOps()

  lazy val transactions = new AsyncTransactions(
    new CoreTransactionsReactive(
      core,
      env.transactionsConfig.map(v => v.toCore).getOrElse(TransactionsConfig().toCore)
    ),
    env
  )

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param bucketName the name of the bucket to open
    */
  def bucket(bucketName: String): AsyncBucket = {
    couchbaseOps match {
      case core: Core => core.openBucket(bucketName)
      case _          =>
    }
    new AsyncBucket(bucketName, couchbaseOps, environment)
  }

  /** @see [[ClusterBase.authenticator]] */
  def authenticator(authenticator: Authenticator): Try[Unit] = {
    Try(couchbaseOps.authenticator(authenticator))
  }

  private[scala] def performGlobalConnect(): Unit = {
    couchbaseOps match {
      case core: Core => core.initGlobalConfig()
      case _          =>
    }
  }
}
