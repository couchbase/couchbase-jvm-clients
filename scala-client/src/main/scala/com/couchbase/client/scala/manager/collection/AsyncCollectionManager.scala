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
package com.couchbase.client.scala.manager.collection

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.AsyncBucket
import com.couchbase.client.scala.util.CoreCommonConverters.makeCommonOptions
import com.couchbase.client.scala.util.DurationConversions.javaDurationToScala
import com.couchbase.client.scala.util.FutureConversions

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class AsyncCollectionManager(private val bucket: AsyncBucket)(
    implicit val ec: ExecutionContext
) {
  private[scala] val defaultManagerTimeout: Duration =
    bucket.couchbaseOps.environment.timeoutConfig.managementTimeout
  private[scala] val defaultRetryStrategy = bucket.couchbaseOps.environment.retryStrategy

  private def coreCollectionManager = bucket.couchbaseOps.collectionManager(bucket.name)

  @deprecated(message = "use getAllScopes instead", since = "1.1.2")
  def getScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[ScopeSpec] = {
    getAllScopes(timeout, retryStrategy)
      .map(scopes => scopes.filter(v => v.name == scopeName).head)
  }

  def getAllScopes(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[ScopeSpec]] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreCollectionManager.getAllScopes(makeCommonOptions(timeout, retryStrategy))
      )
      .map(
        v =>
          v.scopes()
            .asScala
            .toSeq // Required for 2.13
            .map(
              scope =>
                ScopeSpec(
                  scope.name,
                  scope.collections.asScala.map(
                    coll =>
                      CollectionSpec(
                        coll.name,
                        scope.name,
                        Option(coll.maxExpiry).map(v => Duration(v.longValue, TimeUnit.SECONDS)),
                        Option(coll.history)
                      )
                  )
                )
            )
      )
  }

  @deprecated("Please use the overload that takes a CreateCollectionSettings", since = "1.4.11")
  def createCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreCollectionManager.createCollection(
          collection.scopeName,
          collection.name,
          null,
          makeCommonOptions(timeout, retryStrategy)
        )
      )
      .map(_ => ())
  }

  @Volatile
  def createCollection(
      scopeName: String,
      collectionName: String,
      settings: CreateCollectionSettings
  ): Future[Unit] = {
    createCollection(
      scopeName,
      collectionName,
      settings,
      defaultManagerTimeout,
      defaultRetryStrategy
    )
  }

  @Volatile
  def createCollection(
      scopeName: String,
      collectionName: String,
      settings: CreateCollectionSettings,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreCollectionManager.createCollection(
          scopeName,
          collectionName,
          settings.toCore,
          makeCommonOptions(timeout, retryStrategy)
        )
      )
      .map(_ => ())
  }

  @Volatile
  def updateCollection(
      scopeName: String,
      collectionName: String,
      settings: UpdateCollectionSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreCollectionManager.updateCollection(
          scopeName,
          collectionName,
          settings.toCore,
          makeCommonOptions(timeout, retryStrategy)
        )
      )
      .map(_ => ())
  }

  @deprecated(
    "Please use the overload that takes separate scopeName and collectionName",
    since = "1.4.11"
  )
  def dropCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    dropCollection(collection.scopeName, collection.name, timeout, retryStrategy)
  }

  @Volatile
  def dropCollection(
      scopeName: String,
      collectionName: String
  ): Future[Unit] = {
    dropCollection(scopeName, collectionName, defaultManagerTimeout, defaultRetryStrategy)
  }

  @Volatile
  def dropCollection(
      scopeName: String,
      collectionName: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreCollectionManager.dropCollection(
          scopeName,
          collectionName,
          makeCommonOptions(timeout, retryStrategy)
        )
      )
      .map(_ => ())
  }

  def createScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreCollectionManager.createScope(
          scopeName,
          makeCommonOptions(timeout, retryStrategy)
        )
      )
      .map(_ => ())
  }

  def dropScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreCollectionManager.dropScope(
          scopeName,
          makeCommonOptions(timeout, retryStrategy)
        )
      )
      .map(_ => ())
  }
}
