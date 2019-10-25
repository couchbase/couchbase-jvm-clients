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
package com.couchbase.client.scala.manager.bucket

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
@Volatile
class AsyncBucketManager(val reactive: ReactiveBucketManager)(implicit val ec: ExecutionContext) {
  private[scala] val defaultManagerTimeout = reactive.defaultManagerTimeout
  private[scala] val defaultRetryStrategy  = reactive.defaultRetryStrategy

  def create(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.create(settings, timeout, retryStrategy).toFuture
  }

  def updateBucket(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.updateBucket(settings, timeout, retryStrategy).toFuture
  }

  def dropBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.dropBucket(bucketName, timeout, retryStrategy).toFuture
  }

  def getBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[BucketSettings] = {
    reactive.getBucket(bucketName, timeout, retryStrategy).toFuture
  }

  def getAllBuckets(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[BucketSettings]] = {
    reactive.getAllBuckets(timeout, retryStrategy).collectSeq().toFuture
  }

  def flushBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.flushBucket(bucketName, timeout, retryStrategy).toFuture
  }
}
