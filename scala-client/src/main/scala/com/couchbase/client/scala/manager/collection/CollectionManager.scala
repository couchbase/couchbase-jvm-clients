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

import scala.concurrent.duration.Duration
import scala.util.Try

class CollectionManager(reactive: ReactiveCollectionManager) {
  private val bucket                       = reactive.bucket
  private val core                         = bucket.core
  private[scala] val defaultManagerTimeout = reactive.defaultManagerTimeout
  private[scala] val defaultRetryStrategy  = reactive.defaultRetryStrategy

  private[scala] def collectionExists(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Boolean] = {
    Try(reactive.collectionExists(collection, timeout, retryStrategy).block())
  }

  private[scala] def scopeExists(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Boolean] = {
    Try(reactive.scopeExists(scopeName, timeout, retryStrategy).block())
  }

  def getScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[ScopeSpec] = {
    Try(reactive.getScope(scopeName, timeout, retryStrategy).block())
  }

  def getAllScopes(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Seq[ScopeSpec]] = {
    Try(reactive.getAllScopes(timeout, retryStrategy).collectSeq.block())
  }

  def createCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.createCollection(collection, timeout, retryStrategy).block())
  }

  def dropCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.dropCollection(collection, timeout, retryStrategy).block())
  }

  def createScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.createScope(scopeName, timeout, retryStrategy).block())
  }

  def dropScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.dropScope(scopeName, timeout, retryStrategy).block())
  }
}
