/*
 * Copyright (c) 2024 Couchbase, Inc.
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

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.json.JsonObject

import scala.concurrent.duration.Duration
import scala.util.Try

/** Allows indexes for Full Text Search (FTS) to be managed.
  *
  * This interface is for scoped indexes.  For global indexes, use [[SearchIndexManager]].
  */
class ScopeSearchIndexManager(private[scala] val async: AsyncScopeSearchIndexManager) {
  private val DefaultTimeout: Duration            = async.DefaultTimeout
  private val DefaultRetryStrategy: RetryStrategy = async.DefaultRetryStrategy

  def getIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[SearchIndex] = {
    Collection.block(async.getIndex(indexName, timeout, retryStrategy))
  }

  def getAllIndexes(
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Seq[SearchIndex]] = {
    Collection.block(async.getAllIndexes(timeout, retryStrategy))
  }

  def upsertIndex(
      indexDefinition: SearchIndex,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.upsertIndex(indexDefinition, timeout, retryStrategy))
  }

  def dropIndex(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.dropIndex(indexName, timeout, retryStrategy))
  }

  def getIndexedDocumentsCount(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Long] = {
    Collection.block(async.getIndexedDocumentsCount(indexName, timeout, retryStrategy))
  }

  def pauseIngest(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.pauseIngest(indexName, timeout, retryStrategy))
  }

  def resumeIngest(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.resumeIngest(indexName, timeout, retryStrategy))
  }

  def allowQuerying(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.allowQuerying(indexName, timeout, retryStrategy))
  }

  def disallowQuerying(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.disallowQuerying(indexName, timeout, retryStrategy))
  }

  def freezePlan(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.freezePlan(indexName, timeout, retryStrategy))
  }

  def unfreezePlan(
      indexName: String,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Unit] = {
    Collection.block(async.unfreezePlan(indexName, timeout, retryStrategy))
  }

  def analyzeDocument(
      indexName: String,
      document: JsonObject,
      timeout: Duration = DefaultTimeout,
      retryStrategy: RetryStrategy = DefaultRetryStrategy
  ): Try[Seq[JsonObject]] = {
    Collection.block(async.analyzeDocument(indexName, document, timeout, retryStrategy))
  }
}
