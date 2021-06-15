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

import com.couchbase.client.core.retry.RetryStrategy

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class AsyncCollectionManager(reactive: ReactiveCollectionManager) {
  private val bucket                       = reactive.bucket
  private val core                         = bucket.core
  private[scala] val defaultManagerTimeout = reactive.defaultManagerTimeout
  private[scala] val defaultRetryStrategy  = reactive.defaultRetryStrategy

  private[scala] def collectionExists(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Boolean] = {
    reactive.collectionExists(collection, timeout, retryStrategy).toFuture
  }

  private[scala] def scopeExists(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Boolean] = {
    reactive.scopeExists(scopeName, timeout, retryStrategy).toFuture
  }

  @deprecated(message = "use getAllScopes instead", since = "1.1.2")
  def getScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[ScopeSpec] = {
    reactive.getScope(scopeName, timeout, retryStrategy).toFuture
  }

  def getAllScopes(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[ScopeSpec]] = {
    reactive.getAllScopes(timeout, retryStrategy).collectSeq().toFuture
  }

  def createCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.createCollection(collection, timeout, retryStrategy).toFuture
  }

  def dropCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.dropCollection(collection, timeout, retryStrategy).toFuture
  }

  def createScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.createScope(scopeName, timeout, retryStrategy).toFuture
  }

  def dropScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.dropScope(scopeName, timeout, retryStrategy).toFuture
  }
}
