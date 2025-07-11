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
package com.couchbase.client.scala.manager.analytics

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus
import com.couchbase.client.core.error.{FeatureNotAvailableException, HttpStatusCodeException}
import com.couchbase.client.core.logging.RedactableArgument.redactMeta
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.CbThrowables.findCause
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.ReactiveCluster
import com.couchbase.client.scala.analytics.{AnalyticsOptions, ReactiveAnalyticsResult}
import com.couchbase.client.scala.manager.analytics.ReactiveAnalyticsIndexManager.{
  DefaultDataverse,
  quote,
  quoteDataverse
}
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.RowTraversalUtil
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object ReactiveAnalyticsIndexManager {
  val DefaultDataverse = "Default"

  private[scala] def pathForLinks = "/analytics/link/"

  private[scala] def pathForLink(scopeName: String, linkName: String) =
    pathForLinks + urlEncode(scopeName.replace('.', '/')) + "/" + urlEncode(linkName)

  private def quote(s: String): Try[String] = {
    if (s.contains("`")) {
      Failure(new IllegalArgumentException(s"Value [${redactMeta(s)}] may not contain backticks."))
    } else {
      Success("`" + s + "`")
    }
  }

  /** SCBC-231 - Handle compound dataverse names.
    */
  private[scala] def quoteDataverse(
      dataverseName: String,
      otherComponents: String*
  ): Try[String] = {
    val t1 = (dataverseName.split("/", -1) ++ otherComponents)
      .map(v => quote(v))
      .toIterator
    val t2 = RowTraversalUtil.traverse(t1)
    t2.map(v => v.mkString("."))
  }
}

class ReactiveAnalyticsIndexManager(
    private[scala] val cluster: ReactiveCluster,
    async: => AsyncAnalyticsIndexManager
) {
  private[scala] val DefaultTimeout: Duration = cluster.async.env.timeoutConfig.managementTimeout()
  private[scala] val DefaultRetryStrategy     = cluster.async.env.retryStrategy

  def createDataverse(
      dataverseName: String,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    quoteDataverse(dataverseName) match {
      case Success(quoted) =>
        val statement: String = {
          val sb = new StringBuilder("CREATE DATAVERSE " + quoted)
          if (ignoreIfExists) sb.append(" IF NOT EXISTS")

          sb.toString
        }

        exec(statement, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.error(err)
    }
  }

  def dropDataverse(
      dataverseName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    quoteDataverse(dataverseName) match {
      case Success(quoted) =>
        val statement = {
          val out = "DROP DATAVERSE " + quoted
          if (ignoreIfNotExists) out + " IF EXISTS"
          else out
        }

        exec(statement, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.error(err)
    }
  }

  def createDataset(
      datasetName: String,
      bucketName: String,
      dataverseName: Option[String] = None,
      condition: Option[String] = None,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    val statement: Try[String] = for {
      quoted1 <- quoteDataverse(dataverseName.getOrElse(DefaultDataverse), datasetName)
      quoted2 <- quote(bucketName)
      statement <- {
        val statement = {
          val out = "CREATE DATASET "
          val next =
            if (ignoreIfExists) out + " IF NOT EXISTS "
            else out
          val cond: String = condition match {
            case Some(c) => " WHERE " + c
            case _       => ""
          }

          next + quoted1 + " ON " + quoted2 + cond
        }
        Success(statement)
      }
    } yield statement

    statement match {
      case Success(st)  => exec(st, timeout, retryStrategy).map(_ => ())
      case Failure(err) => SMono.error(err)
    }
  }

  def dropDataset(
      datasetName: String,
      dataverseName: Option[String] = None,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    quoteDataverse(dataverseName.getOrElse(DefaultDataverse), datasetName) match {
      case Success(quoted) =>
        val statement = {
          val out = "DROP DATASET " + quoted
          if (ignoreIfNotExists) out + " IF EXISTS"
          else out
        }

        exec(statement, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.error(err)
    }
  }

  def getAllDatasets(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SFlux[AnalyticsDataset] = {
    cluster
      .analyticsQuery("SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"")
      .flatMapMany(result => result.rowsAs[AnalyticsDataset])
  }

  def createIndex(
      indexName: String,
      datasetName: String,
      fields: collection.Map[String, AnalyticsDataType],
      dataverseName: Option[String] = None,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    val statement: Try[String] = for {
      quoted1 <- quote(indexName)
      quoted2 <- quoteDataverse(dataverseName.getOrElse(DefaultDataverse), datasetName)
      statement <- {
        val out = "CREATE INDEX " + quoted1
        val next =
          if (ignoreIfExists) out + " IF NOT EXISTS "
          else out

        Success(next + " ON " + quoted2 + " " + formatIndexFields(fields))
      }
    } yield statement

    statement match {
      case Success(st)  => exec(st, timeout, retryStrategy).map(_ => ())
      case Failure(err) => SMono.error(err)
    }
  }

  private def formatIndexFields(fields: collection.Map[String, AnalyticsDataType]) = {
    fields
      .map(v => {
        val dt: String = v._2 match {
          case AnalyticsDataType.AnalyticsInt64  => "int64"
          case AnalyticsDataType.AnalyticsString => "string"
          case AnalyticsDataType.AnalyticsDouble => "double"
        }
        v._1 + ":" + dt
      })
      .mkString("(", ",", ")")
  }

  def dropIndex(
      indexName: String,
      datasetName: String,
      dataverseName: Option[String] = None,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    quoteDataverse(dataverseName.getOrElse(DefaultDataverse), datasetName, indexName) match {
      case Success(quoted) =>
        val statement = {
          val out = "DROP INDEX " + quoted
          if (ignoreIfNotExists) out + " IF EXISTS"
          else out
        }

        exec(statement, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.error(err)
    }
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SFlux[AnalyticsIndex] = {
    cluster
      .analyticsQuery("SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> \"Metadata\"")
      .flatMapMany(result => result.rowsAs[AnalyticsIndex])
  }

  private def exec(
      statement: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[ReactiveAnalyticsResult] = {
    cluster
      .analyticsQuery(
        statement,
        AnalyticsOptions()
          .timeout(timeout)
          .retryStrategy(retryStrategy)
      )
      .onErrorResume(t => {

        val err = findCause(t, classOf[HttpStatusCodeException]).asScala match {
          case Some(httpException) =>
            if (httpException.httpStatusCode == HttpResponseStatus.NOT_FOUND.code) {
              Some(new FeatureNotAvailableException(t))
            } else None
          case _ => None
        }

        val errToRaise = err match {
          case Some(e) => e
          case _ =>
            t match {
              case e: RuntimeException => e
              case _                   => new RuntimeException(t)
            }
        }

        SMono.error(errToRaise)
      })
  }

  /** Create an analytics link.  See the [[AnalyticsLink]] documentation for the types of links that can be created.
    *
    * If a link with the same name already exists, a `LinkExistsException` will be raised. */
  def createLink(
      link: AnalyticsLink,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Void] = {
    SMono.fromFuture(async.createLink(link, timeout, retryStrategy, parentSpan))(async.ec)
  }

  /** Replaces an existing analytics link.
    *
    * Note that on fetching an existing link e.g. with [[GetAllLinks]], some returned fields will intentionally
    * be blanked out (empty strings) for security reasons.  It may be necessarily to reconstruct the original
    * [[AnalyticsLink]] with this security information before calling this method.
    *
    * If no such link exists, a `LinkNotFoundException` will be raised. */
  def replaceLink(
      link: AnalyticsLink,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Void] = {
    SMono.fromFuture(async.replaceLink(link, timeout, retryStrategy, parentSpan))(async.ec)
  }

  /** Drops (deletes) an existing analytics link.
    *
    * If no such link exists, a `LinkNotFoundException` will be raised. */
  def dropLink(
      linkName: String,
      dataverse: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SMono[Void] = {
    SMono.fromFuture(async.dropLink(linkName, dataverse, timeout, retryStrategy, parentSpan))(
      async.ec
    )
  }

  /** Gets analytics links.
    *
    * If `dataverse`, `name` and `linkType` are all `None`, then all links are returned.
    * If `dataverse` is specified then links returned will all be from that dataverse.
    * If `dataverse` and `name` specified then a maximum of one link will be returned, matching that name.
    * (It is illegal to specify `name` but not `dataverse` - a InvalidArgumentException will be raised in this situation.)
    * If `linkType` is specified then links returned will all match that link type.
    *
    * It is legal to combine `linkType` and `dataverse`.
    */
  def getLinks(
      dataverse: Option[String] = None,
      name: Option[String] = None,
      linkType: Option[AnalyticsLinkType] = None,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): SFlux[AnalyticsLink] =
    SMono
      .fromFuture(async.getLinks(dataverse, name, linkType, timeout, retryStrategy, parentSpan))(
        async.ec
      )
      .flatMapMany(SFlux.fromIterable)
}
