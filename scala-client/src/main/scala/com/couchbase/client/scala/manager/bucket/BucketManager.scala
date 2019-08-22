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
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration
import scala.util.Try


@Volatile
class BucketManager(val async: AsyncBucketManager) {
  private val defaultManagerTimeout = async.defaultManagerTimeout
  private val defaultRetryStrategy = async.defaultRetryStrategy

  def create(settings: CreateBucketSettings,
             timeout: Duration = defaultManagerTimeout,
             retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    val future = async.create(settings, timeout, retryStrategy)
    Collection.blockForever(future)
  }

  def updateBucket(settings: CreateBucketSettings,
                   timeout: Duration = defaultManagerTimeout,
                   retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    Collection.blockForever(async.updateBucket(settings, timeout, retryStrategy))
  }

  def dropBucket(bucketName: String,
                 timeout: Duration = defaultManagerTimeout,
                 retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    Collection.blockForever(async.dropBucket(bucketName, timeout, retryStrategy))
  }

  def getBucket(bucketName: String,
                timeout: Duration = defaultManagerTimeout,
                retryStrategy: RetryStrategy = defaultRetryStrategy): Try[BucketSettings] = {
    Collection.blockForever(async.getBucket(bucketName, timeout, retryStrategy))
  }


  def getAllBuckets(timeout: Duration = defaultManagerTimeout,
                    retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Seq[BucketSettings]] = {
    Collection.blockForever(async.getAllBuckets(timeout, retryStrategy))
  }

  def flushBucket(bucketName: String,
                  timeout: Duration = defaultManagerTimeout,
                  retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    Collection.blockForever(async.flushBucket(bucketName, timeout, retryStrategy))
  }
}
