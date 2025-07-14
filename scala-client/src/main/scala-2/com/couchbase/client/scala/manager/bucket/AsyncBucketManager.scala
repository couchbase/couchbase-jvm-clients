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

import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.CoreCommonConverters.makeCommonOptions
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

class AsyncBucketManager(couchbaseOps: CoreCouchbaseOps)(implicit val ec: ExecutionContext) {
  private[scala] val defaultManagerTimeout =
    couchbaseOps.environment.timeoutConfig.managementTimeout
  private[scala] val defaultRetryStrategy = couchbaseOps.environment.retryStrategy
  private[scala] val coreBucketManager    = couchbaseOps.bucketManager()

  def create(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreBucketManager
          .createBucket(
            settings.toCore,
            makeCommonOptions(timeout, retryStrategy)
          )
      )
      .map(_ => ())
  }

  def updateBucket(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreBucketManager.updateBucket(settings.toCore, makeCommonOptions(timeout, retryStrategy))
      )
      .map(_ => ())
  }

  def dropBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreBucketManager.dropBucket(bucketName, makeCommonOptions(timeout, retryStrategy))
      )
      .map(_ => ())
  }

  def getBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[BucketSettings] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreBucketManager.getBucket(bucketName, makeCommonOptions(timeout, retryStrategy))
      )
      .map(response => BucketSettings.fromCore(response))
  }

  def getAllBuckets(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[BucketSettings]] = {
    import scala.jdk.CollectionConverters._

    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreBucketManager.getAllBuckets(makeCommonOptions(timeout, retryStrategy))
      )
      .map(response => response.asScala.toSeq.map(kv => BucketSettings.fromCore(kv._2)))
  }

  def flushBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    FutureConversions
      .javaCFToScalaFutureMappingExceptions(
        coreBucketManager.flushBucket(bucketName, makeCommonOptions(timeout, retryStrategy))
      )
      .map(_ => ())
  }
}
