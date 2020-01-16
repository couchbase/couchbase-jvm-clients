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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus
import com.couchbase.client.core.error.{FeatureNotAvailableException, HttpStatusCodeException}
import com.couchbase.client.core.logging.RedactableArgument.redactMeta
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.CbThrowables.findCause
import com.couchbase.client.scala.ReactiveCluster
import com.couchbase.client.scala.analytics.{AnalyticsOptions, ReactiveAnalyticsResult}
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.RowTraversalUtil
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class ReactiveAnalyticsIndexManager(cluster: ReactiveCluster) {

  private[scala] val DefaultTimeout: Duration = cluster.async.env.timeoutConfig.managementTimeout()
  private[scala] val DefaultRetryStrategy     = cluster.async.env.retryStrategy

  def createDataverse(
      dataverseName: String,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    quote(dataverseName) match {
      case Success(quoted) =>
        val statement: String = {
          val out = "CREATE DATAVERSE " + quoted
          if (ignoreIfExists) out + " IF NOT EXISTS"
          else out
        }

        exec(statement, dataverseName, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.raiseError(err)
    }
  }

  def dropDataverse(
      dataverseName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    quote(dataverseName) match {
      case Success(quoted) =>
        val statement = {
          val out = "DROP DATAVERSE " + quoted
          if (ignoreIfNotExists) out + " IF EXISTS"
          else out
        }

        exec(statement, dataverseName, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.raiseError(err)
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
      quoted1 <- quoteMulti(dataverseName, Some(datasetName))
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
      case Success(st)  => exec(st, datasetName, timeout, retryStrategy).map(_ => ())
      case Failure(err) => SMono.raiseError(err)
    }
  }

  def dropDataset(
      datasetName: String,
      dataverseName: Option[String] = None,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): SMono[Unit] = {
    quoteMulti(dataverseName, Some(datasetName)) match {
      case Success(quoted) =>
        val statement = {
          val out = "DROP DATASET " + quoted
          if (ignoreIfNotExists) out + " IF EXISTS"
          else out
        }

        exec(statement, datasetName, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.raiseError(err)
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
      quoted2 <- quoteMulti(dataverseName, Some(datasetName))
      statement <- {
        val out = "CREATE INDEX " + quoted1
        val next =
          if (ignoreIfExists) out + " IF NOT EXISTS "
          else out

        Success(next + " ON " + quoted2 + " " + formatIndexFields(fields))
      }
    } yield statement

    statement match {
      case Success(st)  => exec(st, indexName, timeout, retryStrategy).map(_ => ())
      case Failure(err) => SMono.raiseError(err)
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
    quoteMulti(dataverseName, Some(datasetName), Some(indexName)) match {
      case Success(quoted) =>
        val statement = {
          val out = "DROP INDEX " + quoted
          if (ignoreIfNotExists) out + " IF EXISTS"
          else out
        }

        exec(statement, datasetName, timeout, retryStrategy).map(_ => ())

      case Failure(err) => SMono.raiseError(err)
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

  private def quote(s: String): Try[String] = {
    if (s.contains("`")) {
      Failure(new IllegalArgumentException(s"Value [${redactMeta(s)}] may not contain backticks."))
    } else {
      Success("`" + s + "`")
    }
  }

  private def quoteMulti(s: Option[String]*): Try[String] = {
    val quoted: Seq[Try[String]] = s
      .filter(_.isDefined)
      .map(v => quote(v.get))

    RowTraversalUtil
      .traverse(quoted.iterator)
      .map(_.mkString("."))
  }

  private def exec(
      statement: String,
      itemName: String,
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
            if (httpException.code == HttpResponseStatus.NOT_FOUND.code) {
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

        SMono.raiseError(errToRaise)
      })
  }
}

sealed trait AnalyticsDataType

object AnalyticsDataType {
  case object AnalyticsString extends AnalyticsDataType
  case object AnalyticsInt64  extends AnalyticsDataType
  case object AnalyticsDouble extends AnalyticsDataType
}
