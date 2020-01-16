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

import com.couchbase.client.core.retry.RetryStrategy

import scala.concurrent.duration.Duration
import scala.util.Try

class AnalyticsIndexManager(reactive: ReactiveAnalyticsIndexManager) {
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
}
