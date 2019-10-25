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
package com.couchbase.client.scala.manager.view

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.view.DesignDocumentNamespace

import scala.concurrent.duration.Duration
import scala.util.Try

class ViewIndexManager(private[scala] val reactive: ReactiveViewIndexManager) {
  private val core = reactive.core
  private val DefaultTimeout: Duration =
    core.context().environment().timeoutConfig().managementTimeout()
  private val DefaultRetryStrategy: RetryStrategy = core.context().environment().retryStrategy()

  def getDesignDocument(
      designDocName: String,
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[DesignDocument] = {
    Try(reactive.getDesignDocument(designDocName, namespace, timeout, retryStrategy).block())
  }

  def getAllDesignDocuments(
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Seq[DesignDocument]] = {
    Try(reactive.getAllDesignDocuments(namespace, timeout, retryStrategy).collectSeq().block())
  }

  def upsertDesignDocument(
      indexData: DesignDocument,
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.upsertDesignDocument(indexData, namespace, timeout, retryStrategy).block())
  }

  def dropDesignDocument(
      designDocName: String,
      namespace: DesignDocumentNamespace,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.dropDesignDocument(designDocName, namespace, timeout, retryStrategy).block())
  }

  def publishDesignDocument(
      designDocName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Try(reactive.publishDesignDocument(designDocName, timeout, retryStrategy).block())
  }
}
