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

// [skip:<1.4.1]

import com.couchbase.client.performer.scala.util.OptionsUtil.{DefaultManagementTimeout, convertDuration}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.sdk.cluster.bucketmanager._
import com.couchbase.client.protocol.shared.Durability
import com.couchbase.client.scala.durability.Durability._
import com.couchbase.client.scala.manager.bucket
import com.couchbase.client.scala.manager.bucket.BucketType.{Couchbase, Ephemeral, Memcached}
import com.couchbase.client.scala.manager.bucket.CompressionMode.{Active, Off}
import com.couchbase.client.scala.manager.bucket.EjectionMethod
import com.couchbase.client.scala.{Cluster, ReactiveCluster}
import com.google.protobuf.Duration
import reactor.core.scala.publisher.SMono

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object BucketManagerHelper {

    def handleBucketManager(cluster: Cluster, command: Command): Result.Builder = {
        val bm = command.getClusterCommand.getBucketManager

        val result = Result.newBuilder()

        if (bm.hasGetBucket) {
            val bucketName = bm.getGetBucket.getBucketName

            val response = cluster.buckets.getBucket(bucketName,
                if (bm.getGetBucket.hasOptions && bm.getGetBucket.getOptions.hasTimeoutMsecs) Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
                else DefaultManagementTimeout)

            response match {
                case Success(bucketSettings: com.couchbase.client.scala.manager.bucket.BucketSettings) => populateResult(result, bucketSettings)
                case Failure(e) => throw e
            }
        } else {
            throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))
        }
    }

    def handleBucketManagerReactive(cluster: ReactiveCluster, command: Command): SMono[Result] = {
        val bm = command.getClusterCommand.getBucketManager

        val result = Result.newBuilder()

        if (bm.hasGetBucket) {
            val bucketName = bm.getGetBucket.getBucketName

            val response: SMono[bucket.BucketSettings] = cluster.buckets.getBucket(bucketName,
                if (bm.getGetBucket.hasOptions && bm.getGetBucket.getOptions.getTimeoutMsecs) Duration(bm.getGetBucket.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
                else DefaultManagementTimeout)

            response.map(r => {
                populateResult(result, r)
                result.build()
            })
        } else {
            SMono.error(new IllegalArgumentException("Unknown operation"))
        }
    }

    def populateResult(result: Result.Builder, response: com.couchbase.client.scala.manager.bucket.BucketSettings): Result.Builder = {
        val builder = BucketSettings.newBuilder()

        builder.setName(response.name)
                .setFlushEnabled(response.flushEnabled)
                .setRamQuotaMB(response.ramQuotaMB)
                .setNumReplicas(response.numReplicas)
                .setReplicaIndexes(response.replicaIndexes)
                .setMaxExpirySeconds(response.maxTTL.getOrElse(0))

        response.bucketType match {
            case Couchbase => builder.setBucketType(BucketType.COUCHBASE)
            case Memcached => builder.setBucketType(BucketType.MEMCACHED)
            case Ephemeral => builder.setBucketType(BucketType.EPHEMERAL)
        }

        response.ejectionMethod match {
            case EjectionMethod.FullEviction => builder.setEvictionPolicy(EvictionPolicyType.FULL)
            case EjectionMethod.NoEviction => builder.setEvictionPolicy(EvictionPolicyType.NO_EVICTION)
            case EjectionMethod.NotRecentlyUsed => builder.setEvictionPolicy(EvictionPolicyType.NOT_RECENTLY_USED)
            case EjectionMethod.ValueOnly => builder.setEvictionPolicy(EvictionPolicyType.VALUE_ONLY)
        }

        response.compressionMode.orNull match {
            case Off => builder.setCompressionMode(CompressionMode.OFF)
            case Active => builder.setCompressionMode(CompressionMode.ACTIVE)
            case _ => builder.setCompressionMode(CompressionMode.PASSIVE)
        }

        response.minimumDurabilityLevel match {
            case Disabled => builder.setMinimumDurabilityLevel(Durability.NONE)
            case ClientVerified(_, _) => builder.setMinimumDurabilityLevel(Durability.NONE)
            case Majority => builder.setMinimumDurabilityLevel(Durability.MAJORITY)
            case MajorityAndPersistToActive => builder.setMinimumDurabilityLevel(Durability.MAJORITY_AND_PERSIST_TO_ACTIVE)
            case PersistToMajority => builder.setMinimumDurabilityLevel(Durability.PERSIST_TO_MAJORITY)
        }

        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setBucketManagerResult(com.couchbase.client.protocol.sdk.cluster.bucketmanager.Result.newBuilder()
                        .setBucketSettings(builder)))
    }

}
