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

import java.util.concurrent.TimeoutException
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.error.{IndexExistsException, IndexNotFoundException}
import com.couchbase.client.core.logging.RedactableArgument.redactMeta
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.retry.reactor.{Retry, RetryContext, RetryExhaustedException}
import com.couchbase.client.core.util.CbThrowables.hasCause
import com.couchbase.client.scala.AsyncCluster
import com.couchbase.client.scala.implicits.Codec
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query.{QueryOptions, QueryParameters, QueryResult}
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.{FutureConversions, RowTraversalUtil}
import reactor.core.scala.publisher.SMono

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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
  private val core = cluster.core
  private val DefaultTimeout: Duration =
    core.context().environment().timeoutConfig().managementTimeout()
  private val DefaultRetryStrategy: RetryStrategy = core.context().environment().retryStrategy()
  private val PrimaryIndexName                    = "#primary"

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
      @Stability.Uncommitted
      scopeName: Option[String] = None,
      @Stability.Uncommitted
      collectionName: Option[String] = None
  ): Future[collection.Seq[QueryIndex]] = {
    if (collectionName.isDefined && scopeName.isEmpty) {
      Future.failed(
        new IllegalArgumentException("scopeName must be specified if collectionName is")
      )
    } else {
      val (statement: String, options: QueryOptions) = AsyncQueryIndexManager
        .getStatementAndOptions(bucketName, timeout, retryStrategy, scopeName, collectionName)

      cluster
        .query(statement, options)
        .map(_.rowsAs[QueryIndex])
        .flatMap {
          case Success(z)   => Future.successful(z)
          case Failure(err) => Future.failed(err)
        }
    }
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
      @Stability.Uncommitted
      scopeName: Option[String] = None,
      @Stability.Uncommitted
      collectionName: Option[String] = None
  ): Future[Unit] = {

    val withOptions = JsonObject.create

    numReplicas.foreach(value => withOptions.put("num_replica", value))
    deferred.foreach(value => withOptions.put("defer_build", value))

    val statement: Try[String] = for {
      quotedKeyspace  <- quote(bucketName, scopeName, collectionName)
      quotedIndexName <- quote(indexName)
      statement <- Success(
        s"CREATE INDEX $quotedIndexName ON $quotedKeyspace ${fields.mkString("(", ",", ")")}"
      )
    } yield statement

    statement match {
      case Success(st) =>
        exec(
          readonly = false,
          st,
          Some(withOptions),
          ignoreIfExists,
          ignoreIfNotExists = false,
          timeout,
          retryStrategy
        )
      case Failure(err) => Future.failed(err)
    }
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
      @Stability.Uncommitted
      scopeName: Option[String] = None,
      @Stability.Uncommitted
      collectionName: Option[String] = None
  ): Future[Unit] = {

    val withOptions = JsonObject.create

    numReplicas.foreach(value => withOptions.put("num_replica", value))
    deferred.foreach(value => withOptions.put("defer_build", value))

    val statement: Try[String] = indexName match {
      case Some(in) =>
        for {
          quotedKeyspace  <- quote(bucketName, scopeName, collectionName)
          quotedIndexName <- quote(in)
          statement       <- Success(s"CREATE PRIMARY INDEX $quotedIndexName ON $quotedKeyspace")
        } yield statement
      case _ =>
        for {
          quotedKeyspace <- quote(bucketName, scopeName, collectionName)
          statement      <- Success(s"CREATE PRIMARY INDEX ON $quotedKeyspace")
        } yield statement
    }

    statement match {
      case Success(st) =>
        exec(
          readonly = false,
          st,
          Some(withOptions),
          ignoreIfExists,
          ignoreIfNotExists = false,
          timeout,
          retryStrategy
        )
      case Failure(err) => Future.failed(err)
    }
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
      @Stability.Uncommitted
      scopeName: Option[String] = None,
      @Stability.Uncommitted
      collectionName: Option[String] = None
  ): Future[Unit] = {
    val statement: Try[String] = scopeName match {
      case Some(_) =>
        for {
          quotedKeyspace  <- quote(bucketName, scopeName, collectionName)
          quotedIndexName <- quote(indexName)
          statement       <- Success(s"DROP INDEX $quotedIndexName ON $quotedKeyspace")
        } yield statement
      case _ =>
        for {
          quotedBucket    <- quote(bucketName)
          quotedIndexName <- quote(indexName)
          statement       <- Success(s"DROP INDEX $quotedBucket.$quotedIndexName")
        } yield statement
    }

    statement match {
      case Success(st) =>
        exec(
          readonly = false,
          st,
          None,
          ignoreIfExists = false,
          ignoreIfNotExists,
          timeout,
          retryStrategy
        )
      case Failure(err) => Future.failed(err)
    }
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
      @Stability.Uncommitted
      scopeName: Option[String] = None,
      @Stability.Uncommitted
      collectionName: Option[String] = None
  ): Future[Unit] = {
    val statement: Try[String] = for {
      quotedKeyspace <- quote(bucketName, scopeName, collectionName)
      statement      <- Success(s"DROP PRIMARY INDEX ON $quotedKeyspace")
    } yield statement

    statement match {
      case Success(st) =>
        exec(
          readonly = false,
          st,
          None,
          ignoreIfExists = false,
          ignoreIfNotExists,
          timeout,
          retryStrategy
        )
      case Failure(err) => Future.failed(err)
    }
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
      @Stability.Uncommitted
      scopeName: Option[String] = None,
      @Stability.Uncommitted
      collectionName: Option[String] = None
  ): Future[Unit] = {

    import scala.compat.java8.FunctionConverters._

    SMono
      .defer(
        () =>
          SMono.fromFuture(
            getAllIndexes(bucketName, timeout, retryStrategy, scopeName, collectionName)
          )
      )
      .doOnNext((allIndexes: collection.Seq[QueryIndex]) => {

        val matchingIndexes: collection.Seq[QueryIndex] = allIndexes
          .filter(v => indexNames.exists(_ == v.name) || (watchPrimary && v.isPrimary))

        val primaryIndexPresent: Boolean = matchingIndexes.exists(_.isPrimary)

        if (watchPrimary && !primaryIndexPresent) {
          throw new IndexNotFoundException(PrimaryIndexName)
        } else {
          val matchingIndexNames: Set[String] = matchingIndexes.map(_.name).toSet

          val missingIndexNames: Set[String] = indexNames.toSet.diff(matchingIndexNames)

          if (missingIndexNames.nonEmpty) {
            throw new IndexNotFoundException(missingIndexNames.mkString(","))
          } else {
            val offlineIndexes = matchingIndexes.filter(_.state != "online")

            if (offlineIndexes.nonEmpty) {
              throw new IndexesNotReadyException()
            }
          }
        }
      })
      .retryWhen(
        Retry
          .onlyIf(
            asJavaPredicate(
              (ctx: RetryContext[Unit]) =>
                hasCause(ctx.exception, classOf[IndexesNotReadyException])
            )
          )
          .exponentialBackoff(50.milliseconds, 1.seconds)
          .timeout(timeout)
          .toReactorRetry
      )
      .map(_ => ())
      .onErrorMap {
        case err @ (_: RetryExhaustedException) => toWatchTimeoutException(err, timeout)
        case err                                => err
      }
      .toFuture
  }

  private def toWatchTimeoutException(t: Throwable, timeout: Duration): TimeoutException = {
    val msg = new StringBuilder(s"A requested index is still not ready after $timeout.")
    new TimeoutException(msg.toString)
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
      @Stability.Uncommitted
      scopeName: Option[String] = None,
      @Stability.Uncommitted
      collectionName: Option[String] = None
  ): Future[Unit] = {
    val ret: Try[(String, QueryOptions)] = scopeName match {
      case Some(sn) =>
        for {
          quotedKeyspace <- quote(bucketName, scopeName, collectionName)
          statement      <- Success(s"""BUILD INDEX ON ${quotedKeyspace} (
            (
              SELECT RAW name FROM system:indexes
              WHERE bucket_id = ?
                AND scope_id = ?
                AND keyspace_id = ?
                AND state = "deferred"
            )
          )""")
          opts <- Success(
            QueryOptions()
              .timeout(timeout)
              .retryStrategy(retryStrategy)
              // ok to use .get as we can only get here if quotedKeyspace is Success()
              .parameters(QueryParameters.Positional(bucketName, sn, collectionName.get))
          )
        } yield (statement, opts)

      case _ =>
        quote(bucketName)
          .map(bn => {
            val statement = s"""BUILD INDEX ON ${bn} (
              (
                SELECT RAW name FROM system:indexes
                  WHERE (keyspace_id = ? AND bucket_id IS MISSING)
                AND state = "deferred"
              )
            )"""
            val opts = QueryOptions()
              .timeout(timeout)
              .retryStrategy(retryStrategy)
              .parameters(QueryParameters.Positional(bucketName))

            (statement, opts)
          })
    }

    ret match {
      case Success((st, opts)) =>
        cluster
          .query(st, opts)
          .map(_.rowsAs[QueryIndex])
          .flatMap {
            case Success(z)   => Future.successful(z)
            case Failure(err) => Future.failed(err)
          }
      case Failure(err) => Future.failed(err)
    }
  }

  private def quote(s: String): Try[String] = {
    if (s.contains("`")) {
      Failure(new IllegalArgumentException(s"Value [${redactMeta(s)}] may not contain backticks."))
    } else Success("`" + s + "`")
  }

  private def quote(
      bucketName: String,
      scopeName: Option[String],
      collectionName: Option[String]
  ): Try[String] = {
    if ((scopeName.isDefined && collectionName.isEmpty) || (collectionName.isDefined && scopeName.isEmpty)) {
      Failure(
        new IllegalArgumentException(
          "scopeName and collectionName must both be specified if either is"
        )
      )
    } else {
      scopeName match {
        case Some(sn) =>
          val cn = collectionName.get
          for {
            quotedBucketName     <- quote(bucketName)
            quotedScopeName      <- quote(sn)
            quotedCollectionName <- quote(cn)
            statement <- Success(
              s"$quotedBucketName.$quotedScopeName.$quotedCollectionName"
            )
          } yield statement

        case _ => quote(bucketName)
      }
    }
  }

  private def exec(
      readonly: Boolean,
      statement: String,
      withOptions: Option[JsonObject],
      ignoreIfExists: Boolean,
      ignoreIfNotExists: Boolean,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    val out = if (withOptions.isEmpty || withOptions.get.isEmpty) {
      execInternal(readonly, statement, timeout, retryStrategy)
    } else {
      val revisedStatement = statement + " WITH " + JacksonTransformers.MAPPER.writeValueAsString(
        withOptions.get
      )
      execInternal(readonly, revisedStatement, timeout, retryStrategy)
    }

    wrap(out, ignoreIfExists, ignoreIfNotExists)
  }

  private[scala] def execInternal(
      readonly: Boolean,
      statement: CharSequence,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): Future[QueryResult] = {
    val queryOpts: QueryOptions = QueryOptions()
      .readonly(readonly)
      .timeout(timeout)
      .retryStrategy(retryStrategy)

    cluster.query(statement.toString, queryOpts)
  }

  def wrap(
      in: Future[QueryResult],
      ignoreIfExists: Boolean,
      ignoreIfNotExists: Boolean
  ): Future[Unit] = {
    in.map(_ => ()) recover {
      case _: IndexNotFoundException if ignoreIfNotExists => ()
      case _: IndexExistsException if ignoreIfExists      => ()
    }
  }

}

object AsyncQueryIndexManager {
  private[scala] def getStatementAndOptions(
      bucketName: String,
      timeout: Duration,
      retryStrategy: RetryStrategy,
      scopeName: Option[String],
      collectionName: Option[String]
  ) = {
    val (statement, options) = scopeName match {
      case Some(sn) =>
        collectionName match {
          case Some(cn) =>
            val statement =
              s"""SELECT idx.* FROM system:indexes AS idx WHERE bucket_id = ?
                 | AND scope_id = ? and keyspace_id = ?
                 | AND `using`="gsi" ORDER BY is_primary
                 | DESC, name ASC""".stripMargin
            val options = QueryOptions()
              .readonly(true)
              .timeout(timeout)
              .retryStrategy(retryStrategy)
              .parameters(QueryParameters.Positional(bucketName, sn, cn))
            (statement, options)

          case _ =>
            val statement =
              s"""SELECT idx.* FROM system:indexes AS idx WHERE bucket_id = ?
                 | AND scope_id = ?
                 | AND `using`="gsi" ORDER BY is_primary
                 | DESC, name ASC""".stripMargin
            val options = QueryOptions()
              .readonly(true)
              .timeout(timeout)
              .retryStrategy(retryStrategy)
              .parameters(QueryParameters.Positional(bucketName, sn))
            (statement, options)
        }
      case _ =>
        val statement =
          s"""SELECT idx.* FROM system:indexes AS idx
             | WHERE ((bucket_id IS MISSING AND keyspace_id = ?) OR bucket_id = ?)
             | AND `using`="gsi" ORDER BY is_primary
             | DESC, name ASC""".stripMargin
        val options = QueryOptions()
          .readonly(true)
          .timeout(timeout)
          .retryStrategy(retryStrategy)
          .parameters(QueryParameters.Positional(bucketName, bucketName))
        (statement, options)
    }
    (statement, options)
  }
}

private class IndexesNotReadyException extends RuntimeException

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
  @Stability.Uncommitted
  def scopeName: Option[String] = scope_id

  /** The name of the bucket the index is on. */
  @Stability.Uncommitted
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
