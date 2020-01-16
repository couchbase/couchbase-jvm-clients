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
package com.couchbase.client.scala.manager.query

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.error.IndexNotFoundException
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

/** Allows query indexes to be managed.
  *
  * @define Timeout        when the operation will timeout.  This will default to `timeoutConfig().managementTimeout
  *                        ()` in the
  *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy  provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  */
@Stability.Volatile
class QueryIndexManager(async: AsyncQueryIndexManager)(implicit val ec: ExecutionContext) {
  private val core = async.cluster.core
  private val DefaultTimeout: Duration =
    core.context().environment().timeoutConfig().managementTimeout()
  private val DefaultRetryStrategy: RetryStrategy = core.context().environment().retryStrategy()

  /** Retries all indexes on a bucket.
    *
    * @param bucketName     the bucket to get indexes on
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def getAllIndexes(
      bucketName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[collection.Seq[QueryIndex]] = {
    Collection.block(async.getAllIndexes(bucketName, timeout, retryStrategy))
  }

  /** Creates a new query index with the specified parameters.
    *
    * @param bucketName     the bucket to create the index on.
    * @param indexName      the name of the index.
    * @param ignoreIfExists if an index with the same name already exists, the operation will fail.
    * @param numReplicas    how many replicas of the index to create.
    * @param deferred       set to true to defer building the index until [[buildDeferredIndexes]] is called.  This can
    *                       provide improved performance when creating multiple indexes.
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def createIndex(
      bucketName: String,
      indexName: String,
      fields: Iterable[String],
      ignoreIfExists: Boolean = false,
      numReplicas: Option[Int] = None,
      deferred: Option[Boolean] = None,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(
      async.createIndex(
        bucketName,
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

  /** Creates a new primary query index with the specified parameters.
    *
    * @param bucketName     the bucket to create the index on.
    * @param indexName      the name of the index.  If not set the server assigns the default primary index name.
    * @param ignoreIfExists if a primary index already exists, the operation will fail.
    * @param numReplicas    how many replicas of the index to create.
    * @param deferred       set to true to defer building the index until [[buildDeferredIndexes]] is called.  This can
    *                       provide improved performance when creating multiple indexes.
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def createPrimaryIndex(
      bucketName: String,
      indexName: Option[String] = None,
      ignoreIfExists: Boolean = false,
      numReplicas: Option[Int] = None,
      deferred: Option[Boolean] = None,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(
      async.createPrimaryIndex(
        bucketName,
        indexName,
        ignoreIfExists,
        numReplicas,
        deferred,
        timeout,
        retryStrategy
      )
    )

  }

  /** Drops an existing index.
    *
    * @param bucketName        the bucket to remove the index from.
    * @param indexName         the name of the index.
    * @param ignoreIfNotExists sets whether the operation should fail if the index does not exists
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    */
  def dropIndex(
      bucketName: String,
      indexName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(
      async.dropIndex(bucketName, indexName, ignoreIfNotExists, timeout, retryStrategy)
    )

  }

  /** Drops an existing primary index.
    *
    * @param bucketName        the bucket to remove the index from.
    * @param ignoreIfNotExists sets whether the operation should fail if the index does not exists
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    */
  def dropPrimaryIndex(
      bucketName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.dropPrimaryIndex(bucketName, ignoreIfNotExists, timeout, retryStrategy))
  }

  /** Polls the specified indexes until they are all online.
    *
    * @param bucketName        the bucket to watch on
    * @param indexNames        the indexes to poll.
    * @param watchPrimary      include the bucket's primary index.  If the bucket has no primary index, the operation
    *                          will fail with `IndexNotFoundException`
    * @param timeout           when the operation will timeout.
    * @param retryStrategy     $RetryStrategy
    */
  def watchIndexes(
      bucketName: String,
      indexNames: Iterable[String],
      timeout: Duration,
      watchPrimary: Boolean = false,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(
      async.watchIndexes(bucketName, indexNames, timeout, watchPrimary, retryStrategy)
    )
  }

  /** Build all deferred indexes.
    *
    * @param bucketName        the bucket to build indexes on.
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    */
  def buildDeferredIndexes(
      bucketName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.buildDeferredIndexes(bucketName, timeout, retryStrategy))
  }
}
