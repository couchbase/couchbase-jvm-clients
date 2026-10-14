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
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.transaction.CoreTransactionsReactive
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.transactions.AsyncTransactions
import com.couchbase.client.scala.transactions.config.TransactionsConfig
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.control.NonFatal

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
    try CoreCouchbaseOps.create(environment.coreEnv, initialAuthenticator, connectionString)
    catch {
      case NonFatal(t) =>
        // Nobody else can shut down an environment the SDK created, so do it here.
        if (environment.owned) Try(environment.shutdown()).failed.foreach(t.addSuppressed)
        throw t
    }

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

  private val bucketCache = new ConcurrentHashMap[String, AsyncBucket]()

  /** Opens and returns a Couchbase bucket resource that exists on this cluster.
    *
    * @param bucketName the name of the bucket to open
    */
  def bucket(bucketName: String): AsyncBucket = {
    bucketCache.computeIfAbsent(
      bucketName,
      _ => {
        couchbaseOps match {
          case core: Core => core.openBucket(bucketName)
          case _          =>
        }
        new AsyncBucket(bucketName, couchbaseOps, environment)
      }
    )
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
