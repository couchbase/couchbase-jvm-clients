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
import com.couchbase.client.core.error.{ErrorCodeAndMessage, QueryException}
import com.couchbase.client.core.logging.RedactableArgument.redactMeta
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.retry.reactor.{Retry, RetryContext, RetryExhaustedException}
import com.couchbase.client.core.util.CbThrowables.hasCause
import com.couchbase.client.scala.AsyncCluster
import com.couchbase.client.scala.implicits.Codec
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.query.{QueryOptions, QueryResult}
import com.couchbase.client.scala.transformers.JacksonTransformers
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.{FutureConversions, RowTraversalUtil}
import org.reactivestreams.Publisher
import reactor.core.Exceptions
import reactor.core.scala.publisher.{Flux, Mono}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


/** Allows query indexes to be managed.
  *
  * @define Timeout        when the operation will timeout.  This will default to `timeoutConfig().managementTimeout
  *                        ()` in the
  *                        provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  * @define RetryStrategy  provides some control over how the SDK handles failures.  Will default to `retryStrategy()`
  *                        in the provided [[com.couchbase.client.scala.env.ClusterEnvironment]].
  */
@Stability.Volatile
class AsyncQueryIndexManager(private[scala] val cluster: AsyncCluster)
                            (implicit val ec: ExecutionContext) {
  private val core = cluster.core
  private val DefaultTimeout: Duration = core.context().environment().timeoutConfig().managementTimeout()
  private val DefaultRetryStrategy: RetryStrategy = core.context().environment().retryStrategy()
  private val IndexAlreadyExists: Regex = "ndex (.*) already exists".r
  private val IndexNotFound: Regex = "ndex (.*) not found".r
  private val ErrorIndexAlreadyExists = 4300
  private val ErrorPrimaryIndexNotFound = 12004
  private val ErrorIndexNotFound = 12016
  private val PrimaryIndexName = "#primary"

  /** Retries all indexes on a bucket.
    *
    * @param bucketName     the bucket to get indexes on
    * @param timeout        $Timeout
    * @param retryStrategy  $RetryStrategy
    */
  def getAllIndexes(bucketName: String,
                    timeout: Duration = DefaultTimeout,
                    retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Seq[QueryIndex]] = {
    val statement =
      s"""SELECT idx.* FROM system:indexes AS idx WHERE keyspace_id = "$bucketName" ORDER BY is_primary
         | DESC, name ASC""".stripMargin

    execInternal(readonly = true, statement, timeout, retryStrategy)
      .map(_.allRowsAs[QueryIndex])
      .flatMap {
        case Success(z) => Future.successful(z)
        case Failure(err) => Future.failed(err)
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
    */
  def createIndex(bucketName: String,
                  indexName: String,
                  fields: Seq[String],
                  ignoreIfExists: Boolean = false,
                  numReplicas: Option[Int] = None,
                  deferred: Option[Boolean] = None,
                  timeout: Duration = DefaultTimeout,
                  retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Unit] = {

    val withOptions = JsonObject.create

    numReplicas.foreach(value => withOptions.put("num_replica", value))
    deferred.foreach(value => withOptions.put("defer_build", value))

    val statement: Try[String] = for {
      quotedBucketName <- quote(bucketName)
      quotedIndexName <- quote(indexName)
      statement <- Success(s"CREATE INDEX $quotedIndexName ON $quotedBucketName ${fields.mkString("(", ",", ")")}")
    } yield statement

    statement match {
      case Success(st) => exec(readonly = false, st, Some(withOptions), ignoreIfExists, ignoreIfNotExists = false,
        timeout,
        retryStrategy)
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
    */
  def createPrimaryIndex(bucketName: String,
                         indexName: Option[String] = None,
                         ignoreIfExists: Boolean = false,
                         numReplicas: Option[Int] = None,
                         deferred: Option[Boolean] = None,
                         timeout: Duration = DefaultTimeout,
                         retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Unit] = {

    val withOptions = JsonObject.create

    numReplicas.foreach(value => withOptions.put("num_replica", value))
    deferred.foreach(value => withOptions.put("defer_build", value))

    val statement: Try[String] = indexName match {
      case Some(in) => for {
        quotedBucketName <- quote(bucketName)
        quotedIndexName <- quote(in)
        statement <- Success(s"CREATE PRIMARY INDEX $quotedIndexName ON $quotedBucketName")
      } yield statement
      case _ =>
        for {
          quotedBucketName <- quote(bucketName)
          statement <- Success(s"CREATE PRIMARY INDEX ON $quotedBucketName")
        } yield statement
    }

    statement match {
      case Success(st) => exec(readonly = false, st, Some(withOptions), ignoreIfExists, ignoreIfNotExists = false,
        timeout,
        retryStrategy)
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
    */
  def dropIndex(bucketName: String,
                indexName: String,
                ignoreIfNotExists: Boolean = false,
                timeout: Duration = DefaultTimeout,
                retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Unit] = {
    val statement: Try[String] = for {
      quotedBucketName <- quote(bucketName)
      quotedIndexName <- quote(indexName)
      statement <- Success(s"DROP INDEX $quotedBucketName.$quotedIndexName")
    } yield statement

    statement match {
      case Success(st) => exec(readonly = false, st, None,
        ignoreIfExists = false, ignoreIfNotExists, timeout, retryStrategy)
      case Failure(err) => Future.failed(err)
    }
  }

  /** Drops an existing primary index.
    *
    * @param bucketName        the bucket to remove the index from.
    * @param ignoreIfNotExists sets whether the operation should fail if the index does not exists
    * @param timeout           $Timeout
    * @param retryStrategy     $RetryStrategy
    */
  def dropPrimaryIndex(bucketName: String,
                       ignoreIfNotExists: Boolean = false,
                       timeout: Duration = DefaultTimeout,
                       retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Unit] = {
    val statement: Try[String] = for {
      quotedBucketName <- quote(bucketName)
      statement <- Success(s"DROP PRIMARY INDEX ON $quotedBucketName")
    } yield statement

    statement match {
      case Success(st) => exec(readonly = false, st, None,
        ignoreIfExists = false, ignoreIfNotExists, timeout, retryStrategy)
      case Failure(err) => Future.failed(err)
    }
  }

  /** Polls the specified indexes until they are all online.
    *
    * @param bucketName        the bucket to remove the index from.
    * @param indexNames        the indexes to poll.
    * @param watchPrimary      include the bucket's primary index.  If the bucket has no primary index, the operation
    *                          will fail with [[QueryIndexNotFoundException]]
    * @param timeout           when the operation will timeout.
    * @param retryStrategy     $RetryStrategy
    */
  def watchIndexes(bucketName: String,
                   indexNames: Seq[String],
                   timeout: Duration,
                   watchPrimary: Boolean = false,
                   retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Unit] = {

    import reactor.core.scala.publisher.PimpMyPublisher._

    FutureConversions.javaMonoToScalaMono(
      Mono.defer(() => {
        Mono.fromFuture(getAllIndexes(bucketName, timeout, retryStrategy))
          .doOnNext((allIndexes: Seq[QueryIndex]) => {

            val matchingIndexes: Seq[QueryIndex] = allIndexes
              .filter(v => indexNames.contains(v.name) || (watchPrimary && v.isPrimary))

            val primaryIndexPresent: Boolean = matchingIndexes.exists(_.isPrimary)

            if (watchPrimary && !primaryIndexPresent) {
              throw QueryIndexNotFoundException.notFound(PrimaryIndexName)
            }
            else {
              val matchingIndexNames: Set[String] = matchingIndexes.map(_.name).toSet

              val missingIndexNames: Set[String] = indexNames.toSet.diff(matchingIndexNames)

              if (missingIndexNames.nonEmpty) {
                throw QueryIndexNotFoundException.notFound(missingIndexNames.mkString(","))
              }
              else {
                val offlineIndexes = matchingIndexes.filter(_.state != "online")

                if (offlineIndexes.nonEmpty) {
                  throw new IndexesNotReadyException()
                }
              }
            }
          })
      })

        .retryWhen(Retry.onlyIf((ctx: RetryContext[Unit]) => hasCause(ctx.exception, classOf[IndexesNotReadyException]))
          .exponentialBackoff(50.milliseconds, 1.seconds)
          .timeout(timeout)))

      .onErrorMap(err => {
        if (err.isInstanceOf[RetryExhaustedException]) toWatchTimeoutException(err, timeout)
        else err
      })

      .toFuture
      .map(_ => Unit)
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
    */
  def buildDeferredIndexes(bucketName: String,
                           timeout: Duration = DefaultTimeout,
                           retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Unit] = {
    getAllIndexes(bucketName, timeout, retryStrategy)
      .flatMap(allIndexes => {
        val deferred = allIndexes
          .filter(_.state == "deferred")
          .map(v => quote(v.name))

        if (deferred.isEmpty) Future.successful(Unit)
        else {
          val statement = for {
            quotedDefers <- RowTraversalUtil.traverse(deferred.iterator)
            quotedBucketName <- quote(bucketName)
            statement <- Success(s"BUILD INDEX ON $quotedBucketName(${quotedDefers.mkString(",")})")
          } yield statement

          statement match {
            case Success(st) => exec(readonly = false, st, None,
              ignoreIfExists = false, ignoreIfNotExists = false, timeout, retryStrategy)
            case Failure(err) => Future.failed(err)
          }
        }
      })
  }

  private def quote(s: String): Try[String] = {
    if (s.contains("`")) {
      Failure(new IllegalArgumentException(s"Value [${redactMeta(s)}] may not contain backticks."))
    }
    else Success("`" + s + "`")
  }

  private def exec(readonly: Boolean,
                   statement: String,
                   withOptions: Option[JsonObject],
                   ignoreIfExists: Boolean,
                   ignoreIfNotExists: Boolean,
                   timeout: Duration = DefaultTimeout,
                   retryStrategy: RetryStrategy = DefaultRetryStrategy): Future[Unit] = {
    val out = if (withOptions.isEmpty || withOptions.get.isEmpty) {
      execInternal(readonly, statement, timeout, retryStrategy)
    }
    else {
      val revisedStatement = statement + " WITH " + JacksonTransformers.MAPPER.writeValueAsString(withOptions.get)
      execInternal(readonly, revisedStatement, timeout, retryStrategy)
    }

    wrap(out, ignoreIfExists, ignoreIfNotExists)
  }

  private[scala] def execInternal(readonly: Boolean,
                                  statement: CharSequence,
                                  timeout: Duration,
                                  retryStrategy: RetryStrategy): Future[QueryResult] = {
    val queryOpts: QueryOptions = QueryOptions()
      .readonly(readonly)
      .timeout(timeout)
      .retryStrategy(retryStrategy)

    cluster.query(statement.toString, queryOpts) transform {
      case s@Success(_) => s
      case Failure(err: QueryException) =>
        val transformed = err.code() match {
          case ErrorIndexAlreadyExists => new QueryIndexAlreadyExistsException(err)
          case ErrorPrimaryIndexNotFound => QueryIndexNotFoundException.notFound(PrimaryIndexName)
          case ErrorIndexNotFound => new QueryIndexNotFoundException("Index not found")
          case _ =>
            IndexAlreadyExists.findFirstMatchIn(err.msg()) match {
              case Some(m) => new QueryIndexAlreadyExistsException(err)
              case _ =>
                IndexNotFound.findFirstMatchIn(err.msg()) match {
                  case Some(m) => QueryIndexNotFoundException.notFound(m.group(1))
                  case _ => err
                }
            }
        }
        Failure(transformed)
      case Failure(cause) => Failure(cause)
    }
  }

  def wrap(in: Future[QueryResult],
           ignoreIfExists: Boolean,
           ignoreIfNotExists: Boolean): Future[Unit] = {
    val out = in transform {
      case s@Success(_) => s
      case Failure(err: QueryIndexNotFoundException) =>
        if (ignoreIfNotExists) Success(Unit)
        else Failure(err)
      case Failure(err: QueryIndexAlreadyExistsException) =>
        if (ignoreIfExists) Success(Unit)
        else Failure(err)
      case Failure(err) => Failure(err)
    }

    out.map(_ => Unit)
  }

}


@Stability.Volatile
class QueryIndexNotFoundException(msg: String, errors: Seq[ErrorCodeAndMessage] = Seq())
  extends QueryException(msg, errors.asJava) {
}

object QueryIndexNotFoundException {
  def notFound(indexName: String) = new QueryIndexNotFoundException(
    "Index " + redactMeta(indexName) + " not found",
    Seq(new ErrorCodeAndMessage(12004, "Index " + redactMeta(indexName) + " not found")))
}

private class IndexesNotReadyException extends RuntimeException

@Stability.Volatile
class QueryIndexAlreadyExistsException(cause: QueryException)
  extends QueryException(cause)

@Stability.Volatile
case class QueryIndex(name: String,
                      private val is_primary: Option[Boolean],
                      state: String,
                      private val keyspace_id: String,
                      private val index_key: Seq[String],
                      condition: Option[String]) {
  def keyspaceId: String = keyspace_id

  def isPrimary: Boolean = is_primary.getOrElse(false)

  def indexKey: Seq[String] = index_key
}

object QueryIndex {
  implicit val codec: Codec[QueryIndex] = Codec.codec[QueryIndex]
}

