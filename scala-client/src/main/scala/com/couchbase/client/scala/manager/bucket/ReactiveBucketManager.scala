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

import java.nio.charset.StandardCharsets

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.{DELETE, GET, POST}
import com.couchbase.client.core.error.{
  BucketExistsException,
  BucketNotFoundException,
  CouchbaseException
}
import com.couchbase.client.core.logging.RedactableArgument.redactMeta
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.manager.ManagerUtil
import com.couchbase.client.scala.util.CouchbasePickler
import com.couchbase.client.scala.util.DurationConversions._
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.util.Failure

@Volatile
class ReactiveBucketManager(core: Core) {
  private[scala] val defaultManagerTimeout =
    core.context().environment().timeoutConfig().managementTimeout()
  private[scala] val defaultRetryStrategy = core.context().environment().retryStrategy()

  private def pathForBuckets = "/pools/default/buckets/"

  private def pathForBucket(bucketName: String) = pathForBuckets + urlEncode(bucketName)

  private def pathForBucketFlush(bucketName: String) = {
    "/pools/default/buckets/" + urlEncode(bucketName) + "/controller/doFlush"
  }

  def create(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    createUpdateBucketShared(settings, pathForBuckets, timeout, retryStrategy, update = false)
  }

  private def createUpdateBucketShared(
      settings: CreateBucketSettings,
      path: String,
      timeout: Duration,
      retryStrategy: RetryStrategy,
      update: Boolean
  ): SMono[Unit] = {
    val params = convertSettingsToParams(settings, update)

    ManagerUtil
      .sendRequest(core, POST, path, params, timeout, retryStrategy)
      .flatMap(response => {
        if ((response.status == ResponseStatus.INVALID_ARGS) && response.content != null) {
          val content = new String(response.content, StandardCharsets.UTF_8)
          if (content.contains("Bucket with given name already exists")) {
            SMono.raiseError(BucketExistsException.forBucket(settings.name))
          } else {
            SMono.raiseError(new CouchbaseException(content))
          }
        } else {
          ManagerUtil.checkStatus(response, "create bucket [" + redactMeta(settings) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _            => SMono.just(())
          }
        }
      })
  }

  def updateBucket(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {

    getAllBuckets(timeout, retryStrategy)
      .collectSeq()
      .map(buckets => buckets.exists(_.name == settings.name))
      .flatMap(bucketExists => {
        createUpdateBucketShared(
          settings,
          pathForBucket(settings.name),
          timeout,
          retryStrategy,
          bucketExists
        )
      })
  }

  private def convertSettingsToParams(settings: CreateBucketSettings, update: Boolean) = {
    val params = UrlQueryStringBuilder.createForUrlSafeNames
    params.add("ramQuotaMB", settings.ramQuotaMB)
    settings.numReplicas.foreach(v => params.add("replicaNumber", v))
    settings.flushEnabled.foreach(v => params.add("flushEnabled", if (v) 1 else 0))
    settings.maxTTL.foreach(v => params.add("maxTTL", v))
    settings.ejectionMethod.foreach(v => params.add("evictionPolicy", v.alias))
    settings.compressionMode.foreach(v => params.add("compressionMode", v.alias))
    // The following values must not be changed on update
    if (!update) {
      params.add("name", settings.name)
      settings.bucketType.foreach(v => params.add("bucketType", v.alias))
      settings.conflictResolutionType.foreach(v => params.add("conflictResolutionType", v.alias))
      settings.replicaIndexes.foreach(v => params.add("replicaIndex", if (v) 1 else 0))
    }
    params
  }

  def dropBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    ManagerUtil
      .sendRequest(core, DELETE, pathForBucket(bucketName), timeout, retryStrategy)
      .flatMap(response => {
        if (response.status == ResponseStatus.NOT_FOUND) {
          SMono.raiseError(BucketNotFoundException.forBucket(bucketName))
        } else {
          ManagerUtil.checkStatus(response, "drop bucket [" + redactMeta(bucketName) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _            => SMono.just(())
          }
        }
      })
  }

  def getBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[BucketSettings] = {
    ManagerUtil
      .sendRequest(core, GET, pathForBucket(bucketName), timeout, retryStrategy)
      .flatMap(response => {
        if (response.status == ResponseStatus.NOT_FOUND) {
          SMono.raiseError(BucketNotFoundException.forBucket(bucketName))
        } else {
          ManagerUtil.checkStatus(response, "get bucket [" + redactMeta(bucketName) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _ =>
              val value = CouchbasePickler.read[BucketSettings](response.content)
              SMono.just(value)
          }
        }
      })
  }

  def getAllBuckets(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[BucketSettings] = {
    ManagerUtil
      .sendRequest(core, GET, pathForBuckets, timeout, retryStrategy)
      .flatMapMany(response => {
        ManagerUtil.checkStatus(response, "get all buckets") match {
          case Failure(err) => SFlux.raiseError(err)
          case _ =>
            val value = CouchbasePickler.read[Seq[BucketSettings]](response.content)
            SFlux.fromIterable(value)
        }
      })
  }

  def flushBucket(
      bucketName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    ManagerUtil
      .sendRequest(core, POST, pathForBucketFlush(bucketName), timeout, retryStrategy)
      .flatMap(response => {
        if (response.status == ResponseStatus.NOT_FOUND) {
          SMono.raiseError(BucketNotFoundException.forBucket(bucketName))
        } else {
          ManagerUtil.checkStatus(response, "flush bucket [" + redactMeta(bucketName) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _            => SMono.just(())
          }
        }
      })
  }
}
