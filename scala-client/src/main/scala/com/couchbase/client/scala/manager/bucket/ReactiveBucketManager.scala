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
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.manager.bucket.ReactiveBucketManager.toCommonOptions
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions
import reactor.core.scala.publisher.{SFlux, SMono}

import java.time
import java.util.Optional
import scala.concurrent.duration.Duration

@Volatile
class ReactiveBucketManager(couchbaseOps: CoreCouchbaseOps) {
  private[scala] val defaultManagerTimeout =
    couchbaseOps.environment.timeoutConfig.managementTimeout
  private[scala] val defaultRetryStrategy = couchbaseOps.environment.retryStrategy
  private[scala] val coreBucketManager    = couchbaseOps.bucketManager()

  def create(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    FutureConversions
      .javaCFToScalaMono(
        coreBucketManager
          .createBucket(settings.toCore, null, toCommonOptions(timeout, retryStrategy))
      )
      .`then`()
  }

  def updateBucket(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    FutureConversions
      .javaCFToScalaMono(
        coreBucketManager.updateBucket(settings.toCore, toCommonOptions(timeout, retryStrategy))
      )
      .`then`()
  }

  def dropBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    FutureConversions
      .javaCFToScalaMono(
        coreBucketManager.dropBucket(bucketName, toCommonOptions(timeout, retryStrategy))
      )
      .`then`()
  }

  def getBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[BucketSettings] = {
    FutureConversions
      .javaCFToScalaMono(
        coreBucketManager.getBucket(bucketName, toCommonOptions(timeout, retryStrategy))
      )
      .map(response => BucketSettings.fromCore(response))
  }

  def getAllBuckets(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[BucketSettings] = {
    import scala.jdk.CollectionConverters._

    FutureConversions
      .javaCFToScalaMono(coreBucketManager.getAllBuckets(toCommonOptions(timeout, retryStrategy)))
      .flatMapMany(response => SFlux.fromIterable(response.asScala))
      .map(response => BucketSettings.fromCore(response._2))
  }

  def flushBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    FutureConversions
      .javaCFToScalaMono(
        coreBucketManager.flushBucket(bucketName, toCommonOptions(timeout, retryStrategy))
      )
      .`then`()
  }
}

object ReactiveBucketManager {

  private[scala] def toCommonOptions(to: Duration, rs: RetryStrategy): CoreCommonOptions = {
    new CoreCommonOptions {
      override def timeout(): Optional[time.Duration] = Optional.of(to)

      override def retryStrategy(): Optional[RetryStrategy] = Optional.of(rs)

      override def parentSpan(): Optional[RequestSpan] = Optional.empty()
    }
  }
}
