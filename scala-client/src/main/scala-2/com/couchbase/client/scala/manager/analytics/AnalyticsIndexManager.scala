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
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.util.AsyncUtils

import java.time
import java.util.Optional
import scala.concurrent.duration.Duration
import scala.util.Try

class AnalyticsIndexManager(
    async: AsyncAnalyticsIndexManager,
    reactive: ReactiveAnalyticsIndexManager
) {
  private val DefaultTimeout       = reactive.DefaultTimeout
  private val DefaultRetryStrategy = reactive.DefaultRetryStrategy

  def createDataverse(
      dataverseName: String,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.createDataverse(dataverseName, ignoreIfExists, timeout, retryStrategy).block())
  }

  def dropDataverse(
      dataverseName: String,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.dropDataverse(dataverseName, ignoreIfNotExists, timeout, retryStrategy).block())
  }

  def createDataset(
      datasetName: String,
      bucketName: String,
      dataverseName: Option[String] = None,
      condition: Option[String] = None,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(
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
        .block()
    )
  }

  def dropDataset(
      datasetName: String,
      dataverseName: Option[String] = None,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(
      reactive
        .dropDataset(datasetName, dataverseName, ignoreIfNotExists, timeout, retryStrategy)
        .block()
    )
  }

  def getAllDatasets(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Seq[AnalyticsDataset]] = {
    Try(reactive.getAllDatasets(timeout, retryStrategy).collectSeq().block())
  }

  def createIndex(
      indexName: String,
      datasetName: String,
      fields: collection.Map[String, AnalyticsDataType],
      dataverseName: Option[String] = None,
      ignoreIfExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(
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
        .block()
    )
  }

  def dropIndex(
      indexName: String,
      datasetName: String,
      dataverseName: Option[String] = None,
      ignoreIfNotExists: Boolean = false,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(
      reactive
        .dropIndex(indexName, datasetName, dataverseName, ignoreIfNotExists, timeout, retryStrategy)
        .block()
    )
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Seq[AnalyticsIndex]] = {
    Try(reactive.getAllIndexes(timeout, retryStrategy).collectSeq().block())
  }

  /** Create an analytics link.  See the [[AnalyticsLink]] documentation for the types of links that can be created.
    *
    * If a link with the same name already exists, a `LinkExistsException` will be raised.
    */
  def createLink(
      link: AnalyticsLink,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Void] = {
    AsyncUtils.block(async.createLink(link, timeout, retryStrategy, parentSpan))
  }

  /** Replaces an existing analytics link.
    *
    * Note that on fetching an existing link e.g. with [[GetAllLinks]], some returned fields will intentionally
    * be blanked out (empty strings) for security reasons.  It may be necessarily to reconstruct the original
    * [[AnalyticsLink]] with this security information before calling this method.
    *
    * If no such link exists, a `LinkNotFoundException` will be raised.
    */
  def replaceLink(
      link: AnalyticsLink,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Void] = {
    AsyncUtils.block(async.replaceLink(link, timeout, retryStrategy, parentSpan))
  }

  /** Drops (deletes) an existing analytics link.
    *
    * If no such link exists, a `LinkNotFoundException` will be raised.
    */
  def dropLink(
      linkName: String,
      dataverse: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy,
      parentSpan: Option[RequestSpan] = None
  ): Try[Void] = {
    AsyncUtils.block(async.dropLink(linkName, dataverse, timeout, retryStrategy, parentSpan))
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
  ): Try[Seq[AnalyticsLink]] = {
    AsyncUtils.block(
      async.getLinks(dataverse, name, linkType, timeout, retryStrategy, parentSpan)
    )
  }

}

object AnalyticsIndexManager {
  private[scala] def makeCoreOptions(
      _timeout: Duration,
      _retryStrategy: RetryStrategy,
      _parentSpan: Option[RequestSpan]
  ): CoreCommonOptions = {
    import com.couchbase.client.scala.util.DurationConversions._

    import scala.compat.java8.OptionConverters._

    new CoreCommonOptions {
      override def timeout(): Optional[time.Duration] = Optional.of(_timeout)

      override def retryStrategy(): Optional[RetryStrategy] = Optional.of(_retryStrategy)

      override def parentSpan(): Optional[RequestSpan] = _parentSpan.asJava
    }
  }
}
