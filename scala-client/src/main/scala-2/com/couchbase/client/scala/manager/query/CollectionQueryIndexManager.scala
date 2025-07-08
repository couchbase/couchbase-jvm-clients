/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.scala.manager.query

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.util.AsyncUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

/** Allows query indexes to be managed, at the Collection level.
  *
  * @define Timeout        when the operation will timeout.  This will default to `timeoutConfig().managementTimeout
  *                        ()` in the
  *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy  provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  */
class CollectionQueryIndexManager(async: AsyncCollectionQueryIndexManager)(
    implicit val ec: ExecutionContext
) {
  private[client] val DefaultTimeout: Duration            = async.DefaultTimeout
  private[client] val DefaultRetryStrategy: RetryStrategy = async.DefaultRetryStrategy

  /** Gets all indexes on this collection.
    *
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[collection.Seq[QueryIndex]] = {
    AsyncUtils.block(
      async.getAllIndexes(timeout, retryStrategy)
    )
  }

  /** Creates a new query index on this collection, with the specified parameters.
    *
    * @param indexName      the name of the index.
    * @param ignoreIfExists if an index with the same name already exists, the operation will fail.
    * @param numReplicas    how many replicas of the index to create.
    * @param deferred       set to true to defer building the index until [[buildDeferredIndexes]] is called.  This can
    *                       provide improved performance when creating multiple indexes.
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def createIndex(
      indexName: String,
      fields: Iterable[String],
      ignoreIfExists: Boolean = false,
      numReplicas: Option[Int] = None,
      deferred: Option[Boolean] = None,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(
      async.createIndex(
        indexName,
        fields,
        ignoreIfExists,
        numReplicas,
        deferred,
        timeout,
        retryStrategy
      )
    )
  }

  /** Creates a new primary query index on this collection, with the specified parameters.
    *
    * @param indexName      the name of the index.  If not set the server assigns the default primary index name.
    * @param ignoreIfExists if a primary index already exists, the operation will fail.
    * @param numReplicas    how many replicas of the index to create.
    * @param deferred       set to true to defer building the index until [[buildDeferredIndexes]] is called.  This can
    *                       provide improved performance when creating multiple indexes.
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def createPrimaryIndex(
      indexName: Option[String] = None,
      ignoreIfExists: Boolean = false,
      numReplicas: Option[Int] = None,
      deferred: Option[Boolean] = None,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(
      async.createPrimaryIndex(
        indexName,
        ignoreIfExists,
        numReplicas,
        deferred,
        timeout,
        retryStrategy
      )
    )

  }

  /** Drops an existing index on this collection.
    *
    * @param indexName         the name of the index.
    * @param ignoreIfNotExists sets whether the operation should fail if the index does not exists
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    */
  def dropIndex(
      indexName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(
      async.dropIndex(
        indexName,
        ignoreIfNotExists,
        timeout
      )
    )

  }

  /** Drops an existing primary index on this collection.
    *
    * @param ignoreIfNotExists sets whether the operation should fail if the index does not exists
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    */
  def dropPrimaryIndex(
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(
      async.dropPrimaryIndex(
        ignoreIfNotExists,
        timeout,
        retryStrategy
      )
    )
  }

  /** Polls the specified indexes on this collection until they are all online.
    *
    * @param indexNames        the indexes to poll.
    * @param watchPrimary      include the bucket's primary index.  If the bucket has no primary index, the operation
    *                          will fail with `IndexNotFoundException`
    * @param timeout           when the operation will timeout.
    * @param retryStrategy     $RetryStrategy
    */
  def watchIndexes(
      indexNames: Iterable[String],
      timeout: Duration,
      watchPrimary: Boolean = false,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(
      async.watchIndexes(
        indexNames,
        timeout,
        watchPrimary,
        retryStrategy
      )
    )
  }

  /** Build all deferred indexes on this collection .
    *
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    */
  def buildDeferredIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(
      async.buildDeferredIndexes(timeout, retryStrategy)
    )
  }
}
