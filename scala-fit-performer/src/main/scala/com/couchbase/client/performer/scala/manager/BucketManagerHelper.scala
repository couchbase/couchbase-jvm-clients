/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.performer.scala.manager

import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.{convertDurability, setSuccess}
import com.couchbase.client.performer.scala.util.OptionsUtil.{
  DefaultManagementTimeout,
  convertDuration
}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.shared.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.manager.bucket
import com.couchbase.client.scala.manager.bucket.BucketType.{Couchbase, Ephemeral, Memcached}
import com.couchbase.client.scala.manager.bucket.CompressionMode.{Active, Off}
import com.couchbase.client.scala.manager.bucket.{
  BucketSettings,
  CompressionMode,
  ConflictResolutionType,
  CreateBucketSettings,
  EjectionMethod,
  StorageBackend
}
import com.couchbase.client.scala.{Cluster, ReactiveCluster}
import com.google.protobuf.Duration
import reactor.core.scala.publisher.SMono

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._

object BucketManagerHelper {

  def handleBucketManager(cluster: Cluster, command: Command): Result.Builder = {
    val bm = command.getClusterCommand.getBucketManager

    val result = Result.newBuilder()

    if (bm.hasGetBucket) {
      val bucketName = bm.getGetBucket.getBucketName

      val response = cluster.buckets.getBucket(
        bucketName,
        if (bm.getGetBucket.hasOptions && bm.getGetBucket.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(bucketSettings) => populateResult(result, bucketSettings)
        case Failure(e)              => throw e
      }
    } else if (bm.hasGetAllBuckets) {
      val req = bm.getGetAllBuckets
      val response = cluster.buckets.getAllBuckets(
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(buckets) => populateResult(result, buckets)
        case Failure(e)       => throw e
      }
    } else if (bm.hasCreateBucket) {
      val req = bm.getCreateBucket
      val response = cluster.buckets.create(
        createBucketSettings(req.getSettings.getSettings, Some(req.getSettings)),
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else if (bm.hasDropBucket) {
      val req = bm.getDropBucket
      val response = cluster.buckets.dropBucket(
        req.getBucketName,
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else if (bm.hasFlushBucket) {
      val req = bm.getFlushBucket
      val response = cluster.buckets.flushBucket(
        req.getBucketName,
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else if (bm.hasUpdateBucket) {
      val req = bm.getUpdateBucket
      val response = cluster.buckets.updateBucket(
        createBucketSettings(req.getSettings, None),
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else {
      throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))
    }

    result
  }

  def handleBucketManagerReactive(cluster: ReactiveCluster, command: Command): SMono[Result] = {
    val bm = command.getClusterCommand.getBucketManager

    val result = Result.newBuilder()

    if (bm.hasGetBucket) {
      val bucketName = bm.getGetBucket.getBucketName

      val response: SMono[bucket.BucketSettings] = cluster.buckets.getBucket(
        bucketName,
        if (bm.getGetBucket.hasOptions && bm.getGetBucket.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response.map(r => {
        populateResult(result, r)
        result.build()
      })
    } else if (bm.hasGetAllBuckets) {
      val req = bm.getGetAllBuckets
      val response = cluster.buckets.getAllBuckets(
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response.collectSeq.map(buckets => populateResult(result, buckets).build)
    } else if (bm.hasCreateBucket) {
      val req = bm.getCreateBucket
      val response = cluster.buckets.create(
        createBucketSettings(req.getSettings.getSettings, Some(req.getSettings)),
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response.map(_ => {
        setSuccess(result)
        result.build
      })
    } else if (bm.hasDropBucket) {
      val req = bm.getDropBucket
      val response = cluster.buckets.dropBucket(
        req.getBucketName,
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response.map(_ => {
        setSuccess(result)
        result.build
      })
    } else if (bm.hasFlushBucket) {
      val req = bm.getFlushBucket
      val response = cluster.buckets.flushBucket(
        req.getBucketName,
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response.map(_ => {
        setSuccess(result)
        result.build
      })
    } else if (bm.hasUpdateBucket) {
      val req = bm.getUpdateBucket
      val response = cluster.buckets.updateBucket(
        createBucketSettings(req.getSettings, None),
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response.map(_ => {
        setSuccess(result)
        result.build
      })
    } else {
      SMono.error(new IllegalArgumentException("Unknown operation"))
    }
  }

  def createBucketSettings(
      bs: com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketSettings,
      cbs: Option[com.couchbase.client.protocol.sdk.cluster.bucketmanager.CreateBucketSettings]
  ): CreateBucketSettings = {
    var cs = CreateBucketSettings(bs.getName, bs.getRamQuotaMB.toInt)
    if (bs.hasFlushEnabled) cs = cs.flushEnabled(bs.getFlushEnabled)
    if (bs.hasNumReplicas) cs = cs.numReplicas(bs.getNumReplicas)
    if (bs.hasReplicaIndexes) cs = cs.replicaIndexes(bs.getReplicaIndexes)
    if (bs.hasBucketType) cs = cs.bucketType(bs.getBucketType match {
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType.COUCHBASE =>
        bucket.BucketType.Couchbase
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType.EPHEMERAL =>
        bucket.BucketType.Ephemeral
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType.MEMCACHED =>
        bucket.BucketType.Memcached
      case _ => throw new UnsupportedOperationException("Unknown bucket type")
    })
    if (bs.hasEvictionPolicy) cs = cs.ejectionMethod(bs.getEvictionPolicy match {
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.FULL =>
        EjectionMethod.FullEviction
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.NO_EVICTION =>
        EjectionMethod.NoEviction
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.NOT_RECENTLY_USED =>
        EjectionMethod.NotRecentlyUsed
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.VALUE_ONLY =>
        EjectionMethod.ValueOnly
      case _ => throw new UnsupportedOperationException("Unknown eviction policy type")
    })
    if (bs.hasMaxExpirySeconds) cs = cs.maxTTL(bs.getMaxExpirySeconds)
    if (bs.hasCompressionMode) cs = cs.compressionMode(bs.getCompressionMode match {
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode.ACTIVE =>
        CompressionMode.Active
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode.OFF =>
        CompressionMode.Off
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode.PASSIVE =>
        CompressionMode.Passive
      case _ => throw new UnsupportedOperationException("Unknown compression mode")
    })
    if (bs.hasMinimumDurabilityLevel)
      cs = cs.minimumDurabilityLevel(convertDurability(bs.getMinimumDurabilityLevel))
    if (bs.hasStorageBackend) cs = cs.storageBackend(bs.getStorageBackend match {
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.StorageBackend.COUCHSTORE =>
        StorageBackend.Couchstore
      case com.couchbase.client.protocol.sdk.cluster.bucketmanager.StorageBackend.MAGMA =>
        StorageBackend.Magma
      case _ => throw new UnsupportedOperationException("Unknown storage backend")
    })
    cbs match {
      case Some(value) =>
        if (value.hasConflictResolutionType)
          cs = cs.conflictResolutionType(value.getConflictResolutionType match {
            case com.couchbase.client.protocol.sdk.cluster.bucketmanager.ConflictResolutionType.TIMESTAMP =>
              ConflictResolutionType.Timestamp
            case com.couchbase.client.protocol.sdk.cluster.bucketmanager.ConflictResolutionType.SEQUENCE_NUMBER =>
              ConflictResolutionType.SequenceNumber
            case com.couchbase.client.protocol.sdk.cluster.bucketmanager.ConflictResolutionType.CUSTOM =>
              ConflictResolutionType.Custom
            case _ => throw new UnsupportedOperationException("Unknown conflict resolution type")
          })
      case _ =>
    }
    cs
  }

  def populateResult(
      result: Result.Builder,
      response: com.couchbase.client.scala.manager.bucket.BucketSettings
  ): Result.Builder = {
    result.setSdk(
      com.couchbase.client.protocol.sdk.Result
        .newBuilder()
        .setBucketManagerResult(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result
            .newBuilder()
            .setBucketSettings(populateBucketSettings(response))
        )
    )
  }

  private def populateBucketSettings(
      response: bucket.BucketSettings
  ): com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketSettings = {
    val builder =
      com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketSettings.newBuilder()

    builder
      .setName(response.name)
      .setFlushEnabled(response.flushEnabled)
      .setRamQuotaMB(response.ramQuotaMB)
      .setNumReplicas(response.numReplicas)
      .setReplicaIndexes(response.replicaIndexes)
      .setMaxExpirySeconds(response.maxTTL.getOrElse(0))

    response.bucketType match {
      case Couchbase =>
        builder.setBucketType(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType.COUCHBASE
        )
      case Memcached =>
        builder.setBucketType(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType.MEMCACHED
        )
      case Ephemeral =>
        builder.setBucketType(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.BucketType.EPHEMERAL
        )
    }

    response.ejectionMethod match {
      case EjectionMethod.FullEviction =>
        builder.setEvictionPolicy(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.FULL
        )
      case EjectionMethod.NoEviction =>
        builder.setEvictionPolicy(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.NO_EVICTION
        )
      case EjectionMethod.NotRecentlyUsed =>
        builder.setEvictionPolicy(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.NOT_RECENTLY_USED
        )
      case EjectionMethod.ValueOnly =>
        builder.setEvictionPolicy(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.EvictionPolicyType.VALUE_ONLY
        )
    }

    response.compressionMode.orNull match {
      case Off =>
        builder.setCompressionMode(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode.OFF
        )
      case Active =>
        builder.setCompressionMode(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode.ACTIVE
        )
      case _ =>
        builder.setCompressionMode(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.CompressionMode.PASSIVE
        )
    }

    response.minimumDurabilityLevel match {
      case Disabled             => builder.setMinimumDurabilityLevel(Durability.NONE)
      case ClientVerified(_, _) => builder.setMinimumDurabilityLevel(Durability.NONE)
      case Majority             => builder.setMinimumDurabilityLevel(Durability.MAJORITY)
      case MajorityAndPersistToActive =>
        builder.setMinimumDurabilityLevel(Durability.MAJORITY_AND_PERSIST_TO_ACTIVE)
      case PersistToMajority => builder.setMinimumDurabilityLevel(Durability.PERSIST_TO_MAJORITY)
    }

    builder.build
  }

  def populateResult(
      result: Result.Builder,
      response: Seq[com.couchbase.client.scala.manager.bucket.BucketSettings]
  ): Result.Builder = {

    result.setSdk(
      com.couchbase.client.protocol.sdk.Result
        .newBuilder()
        .setBucketManagerResult(
          com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result
            .newBuilder()
            .setGetAllBucketsResult(
              com.couchbase.client.protocol.sdk.cluster.bucketmanager.GetAllBucketsResult.newBuilder
                .putAllResult(
                  response.map(x => x.name -> populateBucketSettings(x)).toMap.asJava
                )
            )
        )
    )
  }
}
