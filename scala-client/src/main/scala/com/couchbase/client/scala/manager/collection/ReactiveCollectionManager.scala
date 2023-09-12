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
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

class ReactiveCollectionManager(private[scala] val async: AsyncCollectionManager)(
    implicit val ec: ExecutionContext
) {
  private val defaultManagerTimeout = async.defaultManagerTimeout
  private val defaultRetryStrategy  = async.defaultRetryStrategy

  @deprecated(message = "use getAllScopes instead", since = "1.1.2")
  def getScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[ScopeSpec] = {
    SMono.fromFuture(async.getScope(scopeName, timeout, retryStrategy))
  }

  def getAllScopes(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[ScopeSpec] = {
    SMono
      .fromFuture(async.getAllScopes(timeout, retryStrategy))
      .flatMapMany(v => SFlux.fromIterable(v))
  }

  @deprecated("Please use the overload that takes a CreateCollectionSettings", since = "1.4.11")
  def createCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.createCollection(collection, timeout, retryStrategy))
  }

  @Volatile
  def createCollection(
      scopeName: String,
      collectionName: String,
      settings: CreateCollectionSettings
  ): SMono[Unit] = {
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
  ): SMono[Unit] = {
    SMono.fromFuture(
      async.createCollection(scopeName, collectionName, settings, timeout, retryStrategy)
    )
  }

  @Volatile
  def updateCollection(
      scopeName: String,
      collectionName: String,
      settings: UpdateCollectionSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(
      async.updateCollection(scopeName, collectionName, settings, timeout, retryStrategy)
    )
  }

  @deprecated(
    "Please use the overload that takes separate scopeName and collectionName",
    since = "1.4.11"
  )
  def dropCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.dropCollection(collection, timeout, retryStrategy))
  }

  @Volatile
  def dropCollection(
      scopeName: String,
      collectionName: String
  ): SMono[Unit] = {
    dropCollection(scopeName, collectionName, defaultManagerTimeout, defaultRetryStrategy)
  }

  @Volatile
  def dropCollection(
      scopeName: String,
      collectionName: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.dropCollection(scopeName, collectionName, timeout, retryStrategy))
  }

  def createScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.createScope(scopeName, timeout, retryStrategy))
  }

  def dropScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.dropScope(scopeName, timeout, retryStrategy))
  }
}
