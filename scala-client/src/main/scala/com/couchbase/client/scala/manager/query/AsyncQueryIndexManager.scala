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

import com.couchbase.client.core.Core
import com.couchbase.client.core.api.manager._
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.manager.CoreQueryIndexManager
import com.couchbase.client.core.retry.{BestEffortRetryStrategy, RetryStrategy}
import com.couchbase.client.scala.AsyncCluster
import com.couchbase.client.scala.implicits.Codec
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions

import java.{lang, util}
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** Allows query indexes to be managed.
  *
  * Operations take a bucketName, scopeName and collectionName.
  *
  * If only bucketName is provided, the indexes affected will be those on the bucket's default scope and collection.
  * If bucketName and scopeName are provided, the indexes affected will be all those on collections under that scope.
  * If bucketName, scopeName and collectionName are provided, the affected fetched will be on that specific collection.
  *
  * @define Timeout        when the operation will timeout.  This will default to `timeoutConfig().managementTimeout
  *                        ()` in the
  *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy  provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define ScopeName      if specified, this operation will work on a given [[com.couchbase.client.scala.Scope]] and
  *                        [[com.couchbase.client.scala.Collection]].  If specified, `collectionName` must also be specified.
  * @define CollectionName if specified, this operation will work on a given [[com.couchbase.client.scala.Scope]] and
  *                        [[com.couchbase.client.scala.Collection]].  If specified, `scopeName` must also be specified.
  */
class AsyncQueryIndexManager(private[scala] val cluster: AsyncCluster)(
    implicit val ec: ExecutionContext
) {
  private[scala] val internal = new CoreQueryIndexManager(
    cluster.couchbaseOps.queryOps(),
    cluster.core.coreResources.requestTracer
  )

  private[scala] val DefaultTimeout =
    cluster.couchbaseOps.environment.timeoutConfig.managementTimeout
  private[scala] val DefaultRetryStrategy = cluster.couchbaseOps.environment.retryStrategy

  /** Gets all indexes.
    *
    * If only bucketName is provided, all indexes for that bucket will be fetched, for all scopes and collections.
    * If bucketName and scopeName are provided, the indexes fetched will be all those on collections under that scope.
    * If bucketName, scopeName and collectionName are provided, the indexes fetched will be on that specific collection.
    *
    * @param bucketName     the bucket to get indexes on
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    * @param scopeName      if specified, the indexes fetched will be limited to those on collections under this [[com.couchbase.client.scala.Scope]]
    * @param collectionName if specified, the indexes fetched will be limited to those on this specific [[com.couchbase.client.scala.Collection]]
    */
  def getAllIndexes(
      bucketName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      @deprecated("Users should use `collection.queryIndexes()` instead") scopeName: Option[
        String
      ] = None,
      @deprecated("Users should use `collection.queryIndexes()` instead") collectionName: Option[
        String
      ] = None
  ): Future[collection.Seq[QueryIndex]] = {
    val sn = scopeName
    val cn = collectionName

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.getAllIndexes(
          bucketName,
          new CoreGetAllQueryIndexesOptions {
            override def scopeName(): String = sn.orNull

            override def collectionName(): String = cn.orNull

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(
        result =>
          result.asScala.map(
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
    * @param scopeName      $ScopeName
    * @param collectionName $CollectionName
    */
  def createIndex(
      bucketName: String,
      indexName: String,
      fields: Iterable[String],
      ignoreIfExists: Boolean = false,
      numReplicas: Option[Int] = None,
      deferred: Option[Boolean] = None,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      @deprecated("Users should use `collection.queryIndexes()` instead") scopeName: Option[
        String
      ] = None,
      @deprecated("Users should use `collection.queryIndexes()` instead") collectionName: Option[
        String
      ] = None
  ): Future[Unit] = {
    val _deferred       = deferred
    val _numReplicas    = numReplicas
    val _ignoreIfExists = ignoreIfExists

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.createIndex(
          bucketName,
          indexName,
          fields.toSeq.asJava,
          new CoreCreateQueryIndexOptions {
            override def ignoreIfExists(): Boolean = _ignoreIfExists

            override def `with`(): util.Map[String, AnyRef] = null

            override def scopeAndCollection(): CoreScopeAndCollection =
              makeScopeAndCollection(scopeName, collectionName)

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
    * @param scopeName      $ScopeName
    * @param collectionName $CollectionName
    */
  def createPrimaryIndex(
      bucketName: String,
      indexName: Option[String] = None,
      ignoreIfExists: Boolean = false,
      numReplicas: Option[Int] = None,
      deferred: Option[Boolean] = None,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      @deprecated("Users should use `collection.queryIndexes()` instead") scopeName: Option[
        String
      ] = None,
      @deprecated("Users should use `collection.queryIndexes()` instead") collectionName: Option[
        String
      ] = None
  ): Future[Unit] = {
    val _deferred       = deferred
    val _numReplicas    = numReplicas
    val _ignoreIfExists = ignoreIfExists
    val _indexName      = indexName

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.createPrimaryIndex(
          bucketName,
          new CoreCreatePrimaryQueryIndexOptions {
            override def ignoreIfExists(): Boolean = _ignoreIfExists

            override def `with`(): util.Map[String, AnyRef] = null

            override def scopeAndCollection(): CoreScopeAndCollection =
              makeScopeAndCollection(scopeName, collectionName)

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

  /** Drops an existing index.
    *
    * @param bucketName        the bucket to remove the index from.
    * @param indexName         the name of the index.
    * @param ignoreIfNotExists sets whether the operation should fail if the index does not exists
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    * @param scopeName         $ScopeName
    * @param collectionName    $CollectionName
    */
  def dropIndex(
      bucketName: String,
      indexName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      @deprecated("Users should use `collection.queryIndexes()` instead") scopeName: Option[
        String
      ] = None,
      @deprecated("Users should use `collection.queryIndexes()` instead") collectionName: Option[
        String
      ] = None
  ): Future[Unit] = {
    val _ignoreIfNotExists = ignoreIfNotExists

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.dropIndex(
          bucketName,
          indexName,
          new CoreDropQueryIndexOptions {
            override def ignoreIfNotExists(): Boolean = _ignoreIfNotExists

            override def scopeAndCollection(): CoreScopeAndCollection =
              makeScopeAndCollection(scopeName, collectionName)

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
  }

  /** Drops an existing primary index.
    *
    * @param bucketName        the bucket to remove the index from.
    * @param ignoreIfNotExists sets whether the operation should fail if the index does not exists
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    * @param scopeName         $ScopeName
    * @param collectionName    $CollectionName
    */
  def dropPrimaryIndex(
      bucketName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      @deprecated("Users should use `collection.queryIndexes()` instead") scopeName: Option[
        String
      ] = None,
      @deprecated("Users should use `collection.queryIndexes()` instead") collectionName: Option[
        String
      ] = None
  ): Future[Unit] = {
    val _ignoreIfNotExists = ignoreIfNotExists

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.dropPrimaryIndex(
          bucketName,
          new CoreDropPrimaryQueryIndexOptions {
            override def ignoreIfNotExists(): Boolean = _ignoreIfNotExists

            override def scopeAndCollection(): CoreScopeAndCollection =
              makeScopeAndCollection(scopeName, collectionName)

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
  }

  /** Polls the specified indexes until they are all online.
    *
    * @param bucketName        the bucket to remove the index from.
    * @param indexNames        the indexes to poll.
    * @param watchPrimary      include the bucket's primary index.  If the bucket has no primary index, the operation
    *                          will fail with `IndexNotFoundException`
    * @param timeout           when the operation will timeout.
    * @param retryStrategy     $RetryStrategy
    * @param scopeName         $ScopeName
    * @param collectionName    $CollectionName
    */
  def watchIndexes(
      bucketName: String,
      indexNames: Iterable[String],
      timeout: Duration,
      watchPrimary: Boolean = false,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      @deprecated("Users should use `collection.queryIndexes()` instead") scopeName: Option[
        String
      ] = None,
      @deprecated("Users should use `collection.queryIndexes()` instead") collectionName: Option[
        String
      ] = None
  ): Future[Unit] = {
    val _watchPrimary = watchPrimary

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.watchIndexes(
          bucketName,
          indexNames.toSeq.asJava,
          timeout,
          new CoreWatchQueryIndexesOptions {
            override def watchPrimary(): Boolean = _watchPrimary

            override def scopeAndCollection(): CoreScopeAndCollection =
              makeScopeAndCollection(scopeName, collectionName)

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
  }

  /** Build all deferred indexes.
    *
    * @param bucketName        the bucket to build indexes on.
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    * @param scopeName         $ScopeName
    * @param collectionName    $CollectionName
    */
  def buildDeferredIndexes(
      bucketName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      @deprecated("Users should use `collection.queryIndexes()` instead") scopeName: Option[
        String
      ] = None,
      @deprecated("Users should use `collection.queryIndexes()` instead") collectionName: Option[
        String
      ] = None
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        internal.buildDeferredIndexes(
          bucketName,
          new CoreBuildQueryIndexOptions {
            override def scopeAndCollection(): CoreScopeAndCollection =
              makeScopeAndCollection(scopeName, collectionName)

            override def commonOptions(): CoreCommonOptions =
              makeCommonOptions(timeout, retryStrategy)
          }
        )
      )
      .map(_ => ())
  }

  def makeScopeAndCollection(
      scopeName: Option[String] = None,
      collectionName: Option[String] = None
  ): CoreScopeAndCollection = {
    if (scopeName.isDefined && collectionName.isDefined) {
      new CoreScopeAndCollection(scopeName.get, collectionName.get)
    } else if (scopeName.isDefined || collectionName.isDefined) {
      // Will only be thrown inside core-io, so will be safely wrapped into a Try/Future/SMono during conversion
      throw new InvalidArgumentException(
        "If either scopeName or collectionName is provided, both must be.  However, it's recommended that users use CollectionQueryIndexManager instead",
        null,
        null
      )
    } else {
      null
    }
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

case class QueryIndex(
    name: String,
    private val is_primary: Option[Boolean],
    private val using: Option[String],
    state: String,
    private val keyspace_id: String,
    private val index_key: Seq[String],
    condition: Option[String],
    partition: Option[String],
    private val bucket_id: Option[String],
    private val scope_id: Option[String]
) {

  /** If this is a collection-level index then this will be the collection's name, otherwise the bucket name. */
  def keyspaceId: String = keyspace_id

  /** If this is a collection-level index then this will be the collection's scope's name, otherwise empty. */
  def scopeName: Option[String] = scope_id

  /** The name of the bucket the index is on. */
  def bucketName: String = scope_id match {
    case Some(_) => bucket_id.get // .get is ok, must be present
    case _       => keyspace_id
  }

  /** If this is a collection-level index then this will be the collection's name, otherwise empty. */
  def collectionName: Option[String] = scope_id match {
    case Some(_) => Some(keyspace_id)
    case _       => None
  }

  def isPrimary: Boolean = is_primary.getOrElse(false)

  def indexKey: Seq[String] = index_key

  def typ: QueryIndexType = using match {
    case Some("gsi") => QueryIndexType.GSI
    case _           => QueryIndexType.View
  }
}

object QueryIndex {
  implicit val codec: Codec[QueryIndex] = Codec.codec[QueryIndex]
}

sealed trait QueryIndexType

object QueryIndexType {
  case object View extends QueryIndexType

  case object GSI extends QueryIndexType
}
