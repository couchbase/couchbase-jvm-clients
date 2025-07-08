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

import com.couchbase.client.core.{Core, CoreKeyspace}
import com.couchbase.client.core.api.manager._
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.manager.CoreCollectionQueryIndexManager
import com.couchbase.client.core.retry.{BestEffortRetryStrategy, RetryStrategy}
import com.couchbase.client.scala.AsyncCollection
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions

import java.{lang, util}
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** Allows query indexes to be managed on this collection.
  *
  * @define Timeout        when the operation will timeout.  This will default to `timeoutConfig().managementTimeout
  *                        ()` in the
  *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy  provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  */
class AsyncCollectionQueryIndexManager(
    private[scala] val collection: AsyncCollection,
    private val keyspace: CoreKeyspace
)(
    implicit val ec: ExecutionContext
) {
  private[scala] val internal = new CoreCollectionQueryIndexManager(
    collection.couchbaseOps.queryOps(),
    collection.core.coreResources.requestTracer,
    keyspace
  )
  private[scala] val DefaultTimeout: Duration =
    collection.couchbaseOps.environment.timeoutConfig.managementTimeout
  private[scala] val DefaultRetryStrategy = collection.couchbaseOps.environment.retryStrategy

  /** Gets all indexes on this collection.
    *
    * @param timeout       $Timeout
    * @param retryStrategy $RetryStrategy
    */
  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Seq[QueryIndex]] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.getAllIndexes(
          new CoreGetAllQueryIndexesOptions {
            override def scopeName(): String = null

            override def collectionName(): String = null

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(
        result =>
          // toSeq required for Scala 2.13
          result.asScala.toSeq.map(
            i =>
              QueryIndex(
                i.name,
                Some(i.primary),
                Option(i.`type`),
                i.state,
                i.keyspace,
                i.indexKey.asScala.toSeq.map(v => v.textValue),
                i.condition.asScala,
                i.partition.asScala,
                Option(i.bucketName),
                i.scopeName.asScala
              )
          )
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
  ): Future[Unit] = {
    val _deferred       = deferred
    val _numReplicas    = numReplicas
    val _ignoreIfExists = ignoreIfExists

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.createIndex(
          indexName,
          fields.toSeq.asJava,
          new CoreCreateQueryIndexOptions {
            override def ignoreIfExists(): Boolean = _ignoreIfExists

            override def `with`(): util.Map[String, AnyRef] = null

            override def scopeAndCollection(): CoreScopeAndCollection = null

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)

            override def numReplicas(): Integer = _numReplicas.map(Integer.valueOf).orNull

            override def deferred(): lang.Boolean =
              _deferred.map(v => lang.Boolean.valueOf(v)).orNull
          }
        )
      )
      .map(_ => ())
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
  ): Future[Unit] = {
    val _deferred       = deferred
    val _numReplicas    = numReplicas
    val _ignoreIfExists = ignoreIfExists
    val _indexName      = indexName

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.createPrimaryIndex(
          new CoreCreatePrimaryQueryIndexOptions {
            override def ignoreIfExists(): Boolean = _ignoreIfExists

            override def `with`(): util.Map[String, AnyRef] = null

            override def scopeAndCollection(): CoreScopeAndCollection = null

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)

            override def numReplicas(): Integer = _numReplicas.map(Integer.valueOf).orNull

            override def deferred(): lang.Boolean =
              _deferred.map(v => lang.Boolean.valueOf(v)).orNull

            override def indexName(): String = _indexName.orNull
          }
        )
      )
      .map(_ => ())
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
  ): Future[Unit] = {
    val _ignoreIfNotExists = ignoreIfNotExists

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.dropIndex(
          indexName,
          new CoreDropQueryIndexOptions {
            override def ignoreIfNotExists(): Boolean = _ignoreIfNotExists

            override def scopeAndCollection(): CoreScopeAndCollection = null

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
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
  ): Future[Unit] = {
    val _ignoreIfNotExists = ignoreIfNotExists

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.dropPrimaryIndex(
          new CoreDropPrimaryQueryIndexOptions {
            override def ignoreIfNotExists(): Boolean = _ignoreIfNotExists

            override def scopeAndCollection(): CoreScopeAndCollection = null

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
  }

  /** Polls the specified indexes on this collection until they are all online.
    *
    * @param indexNames    the indexes to poll.
    * @param watchPrimary  include the bucket's primary index.  If the bucket has no primary index, the operation
    *                      will fail with `IndexNotFoundException`
    * @param timeout       when the operation will timeout.
    * @param retryStrategy $RetryStrategy
    */
  def watchIndexes(
      indexNames: Iterable[String],
      timeout: Duration,
      watchPrimary: Boolean = false,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    val _watchPrimary = watchPrimary

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.watchIndexes(
          indexNames.toSeq.asJava,
          timeout,
          new CoreWatchQueryIndexesOptions {
            override def watchPrimary(): Boolean = _watchPrimary

            override def scopeAndCollection(): CoreScopeAndCollection = null

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
  }

  /** Build all deferred indexes on this collection.
    *
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def buildDeferredIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.buildDeferredIndexes(
          new CoreBuildQueryIndexOptions {
            override def scopeAndCollection(): CoreScopeAndCollection = null

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
  }

  def makeCommonOptions(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): CoreCommonOptions = {
    CoreCommonOptions.of(
      if (timeout == DefaultTimeout) null else timeout,
      if (retryStrategy == DefaultRetryStrategy) null else retryStrategy,
      null
    )
  }
}
