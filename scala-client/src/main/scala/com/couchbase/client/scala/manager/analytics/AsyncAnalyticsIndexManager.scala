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

import com.couchbase.client.core.Core
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.manager.CoreAnalyticsLinkManager
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.manager.analytics.AnalyticsIndexManager.makeCoreOptions
import com.couchbase.client.scala.util.{CouchbasePickler, FutureConversions}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class AsyncAnalyticsIndexManager private[scala] (
    reactive: => ReactiveAnalyticsIndexManager,
    private val couchbaseOps: CoreCouchbaseOps
)(
    implicit val ec: ExecutionContext
) {
  private val DefaultTimeout       = reactive.DefaultTimeout
  private val DefaultRetryStrategy = reactive.DefaultRetryStrategy
  private def linkManagerTry: Future[CoreAnalyticsLinkManager] = couchbaseOps match {
    case core: Core => Future.successful(new CoreAnalyticsLinkManager(core))
    case _          => Future.failed(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
  }

  def createDataverse(
      dataverseName: String,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    reactive.createDataverse(dataverseName, ignoreIfExists, timeout, retryStrategy).toFuture
  }

  def dropDataverse(
      dataverseName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {
    reactive.dropDataverse(dataverseName, ignoreIfNotExists, timeout, retryStrategy).toFuture
  }

  def createDataset(
      datasetName: String,
      bucketName: String,
      dataverseName: Option[String] = None,
      condition: Option[String] = None,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {

    reactive
      .createDataset(
        datasetName,
        bucketName,
        dataverseName,
        condition,
        ignoreIfExists,
        timeout,
        retryStrategy
      )
      .toFuture
  }

  def dropDataset(
      datasetName: String,
      dataverseName: Option[String] = None,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {

    reactive
      .dropDataset(datasetName, dataverseName, ignoreIfNotExists, timeout, retryStrategy)
      .toFuture
  }

  def getAllDatasets(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Seq[AnalyticsDataset]] = {
    reactive.getAllDatasets(timeout, retryStrategy).collectSeq().toFuture
  }

  def createIndex(
      indexName: String,
      datasetName: String,
      fields: collection.Map[String, AnalyticsDataType],
      dataverseName: Option[String] = None,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {

    reactive
      .createIndex(
        indexName,
        datasetName,
        fields,
        dataverseName,
        ignoreIfExists,
        timeout,
        retryStrategy
      )
      .toFuture
  }

  def dropIndex(
      indexName: String,
      datasetName: String,
      dataverseName: Option[String] = None,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Unit] = {

    reactive
      .dropIndex(indexName, datasetName, dataverseName, ignoreIfNotExists, timeout, retryStrategy)
      .toFuture
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Future[Seq[AnalyticsIndex]] = {
    reactive.getAllIndexes(timeout, retryStrategy).collectSeq().toFuture
  }

  /** Create an analytics link.  See the [[AnalyticsLink]] documentation for the types of links that can be created.
    *
    * If a link with the same name already exists, a `LinkExistsException` will be raised. */
  def createLink(
      link: AnalyticsLink,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Future[Void] = {
    linkManagerTry.flatMap(linkManager => {
      link.toMap match {
        case Success(v) =>
          val jf =
            linkManager.createLink(v.asJava, makeCoreOptions(timeout, retryStrategy, parentSpan))
          FutureConversions.javaCFToScalaFutureMappingExceptions(jf)
        case Failure(err) => Future.failed(err)
      }
    })
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
  ): Future[Void] = {
    linkManagerTry.flatMap(
      linkManager =>
        link.toMap match {
          case Success(v) =>
            val jf =
              linkManager.replaceLink(v.asJava, makeCoreOptions(timeout, retryStrategy, parentSpan))
            FutureConversions.javaCFToScalaFutureMappingExceptions(jf)
          case Failure(err) => Future.failed(err)
        }
    )
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
  ): Future[Void] = {
    linkManagerTry.flatMap(linkManager => {
      val jf =
        linkManager
          .dropLink(linkName, dataverse, makeCoreOptions(timeout, retryStrategy, parentSpan))
      FutureConversions.javaCFToScalaFutureMappingExceptions(jf)
    })
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
  ): Future[Seq[AnalyticsLink]] = {
    linkManagerTry.flatMap(linkManager => {
      val opts                  = makeCoreOptions(timeout, retryStrategy, parentSpan)
      val dataverseName: String = dataverse.getOrElse(null)
      val linkTypeStr: String   = linkType.map(v => v.encode).getOrElse(null)
      Try(linkManager.getLinks(dataverseName, linkTypeStr, name.getOrElse(null), opts)) match {
        case Success(jf) =>
          FutureConversions
            .javaCFToScalaFutureMappingExceptions(jf)
            .map((responseBytes: Array[Byte]) => {
              val links = CouchbasePickler.read[Seq[AnalyticsLink]](responseBytes)
              links
            })
        case Failure(err) => Future.failed(err)
      }
    })
  }
}
