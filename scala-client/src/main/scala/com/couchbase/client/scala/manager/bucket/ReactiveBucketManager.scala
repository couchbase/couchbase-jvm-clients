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

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.endpoint.http.CoreCommonOptions
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.manager.CoreBucketManager
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.durability.Durability
import com.couchbase.client.scala.manager.bucket.BucketType.{Couchbase, Ephemeral}
import com.couchbase.client.scala.manager.bucket.EjectionMethod.{
  FullEviction,
  NoEviction,
  NotRecentlyUsed,
  ValueOnly
}
import com.couchbase.client.scala.manager.bucket.ReactiveBucketManager.{toCommonOptions, toMap}
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.FutureConversions
import reactor.core.scala.publisher.{SFlux, SMono}

import java.util.Optional
import java.{time, util}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

@Volatile
class ReactiveBucketManager(core: Core) {
  private[scala] val defaultManagerTimeout =
    core.context().environment().timeoutConfig().managementTimeout()
  private[scala] val defaultRetryStrategy = core.context().environment().retryStrategy()
  private[scala] val coreBucketManager    = new CoreBucketManager(core)

  def create(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    toMap(settings) match {
      case Success(params) =>
        FutureConversions
          .javaCFToScalaMono(
            coreBucketManager.createBucket(params, toCommonOptions(timeout, retryStrategy))
          )
          .`then`()
      case Failure(err) => SMono.error(err)
    }
  }

  def updateBucket(
      settings: CreateBucketSettings,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    toMap(settings) match {
      case Success(params) =>
        FutureConversions
          .javaCFToScalaMono(
            coreBucketManager.updateBucket(params, toCommonOptions(timeout, retryStrategy))
          )
          .`then`()
      case Failure(err) => SMono.error(err)
    }
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
      .map(response => {
        BucketSettings.parseFrom(response)
      })
  }

  def getAllBuckets(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[BucketSettings] = {
    import scala.jdk.CollectionConverters._

    FutureConversions
      .javaCFToScalaMono(coreBucketManager.getAllBuckets(toCommonOptions(timeout, retryStrategy)))
      .flatMapMany(response => SFlux.fromIterable(response.asScala))
      .map(response => {
        BucketSettings.parseFrom(response._2)
      })
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
  private[scala] def toMap(settings: CreateBucketSettings): Try[util.HashMap[String, String]] = {
    checkValidEjectionMethod(settings)
      .map(_ => {
        val params = new java.util.HashMap[String, String]
        params.put("ramQuotaMB", String.valueOf(settings.ramQuotaMB))
        if (!settings.bucketType.contains(BucketType.Memcached)) {
          // "The replica number must be specified and must be a non-negative integer."
          params.put("replicaNumber", String.valueOf(settings.numReplicas.getOrElse(1)))
        }
        settings.flushEnabled.foreach(
          fe => params.put("flushEnabled", String.valueOf(if (fe) 1 else 0))
        )
        // Do not send if it's been left at default, else will get an error on CE
        settings.maxTTL.foreach(maxTTL => params.put("maxTTL", String.valueOf(maxTTL)))
        settings.ejectionMethod.foreach(em => params.put("evictionPolicy", em.alias))
        settings.compressionMode.foreach(cm => params.put("compressionMode", cm.alias))
        settings.minimumDurabilityLevel match {
          case Some(Durability.Majority) => params.put("durabilityMinLevel", "majority")
          case Some(Durability.MajorityAndPersistToActive) =>
            params.put("durabilityMinLevel", "majorityAndPersistActive")
          case Some(Durability.PersistToMajority) =>
            params.put("durabilityMinLevel", "persistToMajority")
          case _ =>
        }
        settings.storageBackend match {
          case Some(StorageBackend.Couchstore) => params.put("storageBackend", "couchstore")
          case Some(StorageBackend.Magma)      => params.put("storageBackend", "magma")
          case None                            =>
        }
        params.put("name", settings.name)
        settings.bucketType.foreach(bt => {
          params.put("bucketType", bt.alias)
          if (bt != BucketType.Ephemeral && settings.replicaIndexes.isDefined) {
            params.put("replicaIndex", String.valueOf(if (settings.replicaIndexes.get) 1 else 0))
          }
        })
        //    if (settings.bucketType != BucketType.Ephemeral) {
        //
        //    }
        params
      })
  }

  private[scala] def toCommonOptions(to: Duration, rs: RetryStrategy): CoreCommonOptions = {
    new CoreCommonOptions {
      override def timeout(): Optional[time.Duration] = Optional.of(to)

      override def retryStrategy(): Optional[RetryStrategy] = Optional.of(rs)

      override def parentSpan(): Optional[RequestSpan] = Optional.empty()
    }
  }

  private[scala] def checkValidEjectionMethod(settings: CreateBucketSettings): Try[Unit] = {
    val validEjectionType = settings.ejectionMethod match {
      case Some(FullEviction) | Some(ValueOnly) =>
        settings.bucketType match {
          case Some(Couchbase) | None => true
          case _                      => false
        }
      case Some(NoEviction) | Some(NotRecentlyUsed) =>
        settings.bucketType match {
          case Some(Ephemeral) => true
          case _               => false
        }
      case None => true
    }

    if (!validEjectionType) {
      Failure(
        new InvalidArgumentException(
          s"Cannot use ejection policy ${settings.ejectionMethod} together with bucket type ${settings.bucketType
            .getOrElse(Couchbase)}",
          null,
          null
        )
      );
    } else {
      Success(())
    }
  }
}
