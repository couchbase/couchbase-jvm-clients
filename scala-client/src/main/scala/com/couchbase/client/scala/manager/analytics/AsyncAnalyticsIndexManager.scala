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

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class AsyncAnalyticsIndexManager(reactive: ReactiveAnalyticsIndexManager) {
  private val DefaultTimeout       = reactive.DefaultTimeout
  private val DefaultRetryStrategy = reactive.DefaultRetryStrategy

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
}
