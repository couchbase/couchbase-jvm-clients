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
import com.couchbase.client.scala.Collection

import scala.concurrent.duration.Duration
import scala.util.Try

class CollectionManager(async: AsyncCollectionManager) {
  private[scala] val defaultManagerTimeout = async.defaultManagerTimeout
  private[scala] val defaultRetryStrategy  = async.defaultRetryStrategy

  @deprecated(message = "use getAllScopes instead", since = "1.1.2")
  def getScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[ScopeSpec] = {
    Collection.block(async.getScope(scopeName, timeout, retryStrategy))
  }

  def getAllScopes(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Seq[ScopeSpec]] = {
    Collection.block(async.getAllScopes(timeout, retryStrategy))
  }

  def createCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.createCollection(collection, timeout, retryStrategy))
  }

  def dropCollection(
      collection: CollectionSpec,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.dropCollection(collection, timeout, retryStrategy))
  }

  def createScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.createScope(scopeName, timeout, retryStrategy))
  }

  def dropScope(
      scopeName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.dropScope(scopeName, timeout, retryStrategy))
  }
}
