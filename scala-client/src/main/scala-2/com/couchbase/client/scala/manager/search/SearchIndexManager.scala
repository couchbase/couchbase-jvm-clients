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
package com.couchbase.client.scala.manager.search

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.client.scala.util.AsyncUtils
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration
import scala.util.Try

/** Allows indexes for Full Text Search (FTS) to be managed.
  *
  * This interface is for global indexes.  For scoped indexes, use [[ScopeSearchIndexManager]].
  */
class SearchIndexManager(private[scala] val async: AsyncSearchIndexManager) {
  private val DefaultTimeout: Duration            = async.DefaultTimeout
  private val DefaultRetryStrategy: RetryStrategy = async.DefaultRetryStrategy

  def getIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[SearchIndex] = {
    AsyncUtils.block(async.getIndex(indexName, timeout, retryStrategy))
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Seq[SearchIndex]] = {
    AsyncUtils.block(async.getAllIndexes(timeout, retryStrategy))
  }

  def upsertIndex(
      indexDefinition: SearchIndex,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.upsertIndex(indexDefinition, timeout, retryStrategy))
  }

  def dropIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.dropIndex(indexName, timeout, retryStrategy))
  }

  def getIndexedDocumentsCount(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Long] = {
    AsyncUtils.block(async.getIndexedDocumentsCount(indexName, timeout, retryStrategy))
  }

  def pauseIngest(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.pauseIngest(indexName, timeout, retryStrategy))
  }

  def resumeIngest(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.resumeIngest(indexName, timeout, retryStrategy))
  }

  def allowQuerying(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.allowQuerying(indexName, timeout, retryStrategy))
  }

  def disallowQuerying(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.disallowQuerying(indexName, timeout, retryStrategy))
  }

  def freezePlan(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.freezePlan(indexName, timeout, retryStrategy))
  }

  def unfreezePlan(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    AsyncUtils.block(async.unfreezePlan(indexName, timeout, retryStrategy))
  }

  def analyzeDocument(
      indexName: String,
      document: JsonObject,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Seq[JsonObject]] = {
    AsyncUtils.block(async.analyzeDocument(indexName, document, timeout, retryStrategy))
  }
}
