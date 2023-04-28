/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.performer.scala.query

import com.couchbase.client.core.retry.BestEffortRetryStrategy
import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.setSuccess
import com.couchbase.client.performer.scala.util.OptionsUtil.{DefaultManagementTimeout, DefaultRetryStrategy}
import com.couchbase.client.scala.manager.query.{QueryIndex, QueryIndexType}
import com.couchbase.client.scala.{Cluster, Collection}

import java.util.concurrent.TimeUnit
import scala.collection.convert.ImplicitConversions._
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

object QueryIndexManagerHelper {

  def handleClusterQueryIndexManager(
      cluster: Cluster,
      command: com.couchbase.client.protocol.sdk.Command
  ): com.couchbase.client.protocol.run.Result.Builder = {
    val qim = command.getClusterCommand.getQueryIndexManager
    if (!qim.hasShared) throw new UnsupportedOperationException
    val op = command.getClusterCommand.getQueryIndexManager.getShared
    handleQueryIndexManagerShared(Left((cluster, qim.getBucketName)), op)
  }

  def handleCollectionQueryIndexManager(
      collection: Collection,
      command: com.couchbase.client.protocol.sdk.Command
  ): com.couchbase.client.protocol.run.Result.Builder = {
    if (!command.getCollectionCommand.getQueryIndexManager.hasShared) {
      throw new UnsupportedOperationException
    }
    // [start:1.4.3]
    val op = command.getCollectionCommand.getQueryIndexManager.getShared
    handleQueryIndexManagerShared(Right(collection), op)
    // [end:1.4.3]
    // [start:<1.4.3]
        throw new UnsupportedOperationException();
    // [end:<1.4.3]
  }

  private def handleQueryIndexManagerShared(
      either: Either[(Cluster, String), Collection],
      op: com.couchbase.client.protocol.sdk.query.indexmanager.Command
  ): com.couchbase.client.protocol.run.Result.Builder = {

    val result = com.couchbase.client.protocol.run.Result.newBuilder

    if (op.hasCreatePrimaryIndex) {
      val req = op.getCreatePrimaryIndex
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      either match {
        case Left((cluster, bucketName)) =>
          cluster.queryIndexes
            .createPrimaryIndex(
              bucketName,
              if (req.hasOptions && req.getOptions.hasIndexName) Some(req.getOptions.getIndexName)
              else None,
              if (req.hasOptions && req.getOptions.hasIgnoreIfExists)
                req.getOptions.getIgnoreIfExists
              else false,
              if (req.hasOptions && req.getOptions.hasNumReplicas)
                Some(req.getOptions.getNumReplicas)
              else None,
              if (req.hasOptions && req.getOptions.hasDeferred) Some(req.getOptions.getDeferred)
              else None,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy,
              // [start:1.2.5]
              if (req.hasOptions && req.getOptions.hasScopeName) Some(req.getOptions.getScopeName)
              else None,
              if (req.hasOptions && req.getOptions.hasCollectionName)
                Some(req.getOptions.getCollectionName)
              else None
              // [end:1.2.5]
            )
            .get
        case Right(collection) =>
          // [start:1.4.4]
          collection.queryIndexes
            .createPrimaryIndex(
              if (req.hasOptions && req.getOptions.hasIndexName) Some(req.getOptions.getIndexName)
              else None,
              if (req.hasOptions && req.getOptions.hasIgnoreIfExists)
                req.getOptions.getIgnoreIfExists
              else false,
              if (req.hasOptions && req.getOptions.hasNumReplicas)
                Some(req.getOptions.getNumReplicas)
              else None,
              if (req.hasOptions && req.getOptions.hasDeferred) Some(req.getOptions.getDeferred)
              else None,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy
            )
            .get
        // [end:1.4.4]
      }
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (op.hasCreateIndex) {
      val req = op.getCreateIndex
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      either match {
        case Left((cluster, bucketName)) =>
          cluster.queryIndexes
            .createIndex(
              bucketName,
              req.getIndexName,
              req.getFieldsList.toSeq,
              if (req.hasOptions && req.getOptions.hasIgnoreIfExists)
                req.getOptions.getIgnoreIfExists
              else false,
              if (req.hasOptions && req.getOptions.hasNumReplicas)
                Some(req.getOptions.getNumReplicas)
              else None,
              if (req.hasOptions && req.getOptions.hasDeferred) Some(req.getOptions.getDeferred)
              else None,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy,
              // [start:1.2.5]
              if (req.hasOptions && req.getOptions.hasScopeName) Some(req.getOptions.getScopeName)
              else None,
              if (req.hasOptions && req.getOptions.hasCollectionName)
                Some(req.getOptions.getCollectionName)
              else None
              // [end:1.2.5]
            )
            .get
        case Right(collection) =>
          // [start:1.4.4]
          collection.queryIndexes
            .createIndex(
              req.getIndexName,
              req.getFieldsList.toSeq,
              if (req.hasOptions && req.getOptions.hasIgnoreIfExists)
                req.getOptions.getIgnoreIfExists
              else false,
              if (req.hasOptions && req.getOptions.hasNumReplicas)
                Some(req.getOptions.getNumReplicas)
              else None,
              if (req.hasOptions && req.getOptions.hasDeferred) Some(req.getOptions.getDeferred)
              else None,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy
            )
            .get
        // [end:1.4.4]
      }
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (op.hasGetAllIndexes) {
      val req = op.getGetAllIndexes
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      val indexes = either match {
        case Left((cluster, bucketName)) =>
          cluster.queryIndexes
            .getAllIndexes(
              bucketName,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy,
              // [start:1.2.5]
              if (req.hasOptions && req.getOptions.hasScopeName) Some(req.getOptions.getScopeName)
              else None,
              if (req.hasOptions && req.getOptions.hasCollectionName)
                Some(req.getOptions.getCollectionName)
              else None
              // [end:1.2.5]
            )
            .get
        case Right(collection) =>
          // [start:1.4.4]
          collection.queryIndexes
            .getAllIndexes(
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy
            )
            .get
        // [end:1.4.4]
        // [start:<1.4.4]
          throw new UnsupportedOperationException()
          // [end:<1.4.4]
      }
      result.setSdk(
        com.couchbase.client.protocol.sdk.Result.newBuilder
          .setQueryIndexes(
            com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndexes.newBuilder
              .addAllIndexes(mapQueryIndexes(indexes))
          )
      )
      result.setElapsedNanos(System.nanoTime - start)
    } else if (op.hasDropPrimaryIndex) {
      val req = op.getDropPrimaryIndex
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      either match {
        case Left((cluster, bucketName)) =>
          cluster.queryIndexes
            .dropPrimaryIndex(
              bucketName,
              if (req.hasOptions && req.getOptions.hasIgnoreIfNotExists)
                req.getOptions.getIgnoreIfNotExists
              else false,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy,
              // [start:1.2.5]
              if (req.hasOptions && req.getOptions.hasScopeName) Some(req.getOptions.getScopeName)
              else None,
              if (req.hasOptions && req.getOptions.hasCollectionName)
                Some(req.getOptions.getCollectionName)
              else None
              // [end:1.2.5]
            )
            .get
        case Right(collection) =>
          // [start:1.4.4]
          collection.queryIndexes
            .dropPrimaryIndex(
              if (req.hasOptions && req.getOptions.hasIgnoreIfNotExists)
                req.getOptions.getIgnoreIfNotExists
              else false,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy
            )
            .get
        // [end:1.4.4]
      }
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (op.hasDropIndex) {
      val req = op.getDropIndex
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      either match {
        case Left((cluster, bucketName)) =>
          cluster.queryIndexes
            .dropIndex(
              bucketName,
              req.getIndexName,
              if (req.hasOptions && req.getOptions.hasIgnoreIfNotExists)
                req.getOptions.getIgnoreIfNotExists
              else false,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy,
              // [start:1.2.5]
              if (req.hasOptions && req.getOptions.hasScopeName) Some(req.getOptions.getScopeName)
              else None,
              if (req.hasOptions && req.getOptions.hasCollectionName)
                Some(req.getOptions.getCollectionName)
              else None
              // [end:1.2.5]
            )
            .get
        case Right(collection) =>
          // [start:1.4.4]
          collection.queryIndexes
            .dropIndex(
              req.getIndexName,
              if (req.hasOptions && req.getOptions.hasIgnoreIfNotExists)
                req.getOptions.getIgnoreIfNotExists
              else false,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy
            )
            .get
        // [end:1.4.4]
      }
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (op.hasWatchIndexes) {
      val req = op.getWatchIndexes
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      either match {
        case Left((cluster, bucketName)) =>
          cluster.queryIndexes
            .watchIndexes(
              bucketName,
              req.getIndexNamesList.toSeq,
              Duration(req.getTimeoutMsecs, TimeUnit.MILLISECONDS),
              if (req.hasOptions && req.getOptions.hasWatchPrimary) req.getOptions.getWatchPrimary
              else false,
              DefaultRetryStrategy,
              // [start:1.2.5]
              if (req.hasOptions && req.getOptions.hasScopeName) Some(req.getOptions.getScopeName)
              else None,
              if (req.hasOptions && req.getOptions.hasCollectionName)
                Some(req.getOptions.getCollectionName)
              else None
              // [end:1.2.5]
            )
            .get
        case Right(collection) =>
          // [start:1.4.4]
          collection.queryIndexes
            .watchIndexes(
              req.getIndexNamesList.toSeq,
              Duration(req.getTimeoutMsecs, TimeUnit.MILLISECONDS),
              if (req.hasOptions && req.getOptions.hasWatchPrimary) req.getOptions.getWatchPrimary
              else false,
              DefaultRetryStrategy
            )
            .get
        // [end:1.4.4]
      }
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)
    } else if (op.hasBuildDeferredIndexes) {
      val req = op.getBuildDeferredIndexes
      result.setInitiated(getTimeNow)
      val start = System.nanoTime
      either match {
        case Left((cluster, bucketName)) =>
          cluster.queryIndexes
            .buildDeferredIndexes(
              bucketName,
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy,
              // [start:1.2.5]
              if (req.hasOptions && req.getOptions.hasScopeName) Some(req.getOptions.getScopeName)
              else None,
              if (req.hasOptions && req.getOptions.hasCollectionName)
                Some(req.getOptions.getCollectionName)
              else None
              // [end:1.2.5]
            )
            .get
        case Right(collection) =>
          // [start:1.4.4]
          collection.queryIndexes
            .buildDeferredIndexes(
              if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
                Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
              else DefaultManagementTimeout,
              DefaultRetryStrategy
            )
            .get
        // [end:1.4.4]
      }
      result.setElapsedNanos(System.nanoTime - start)
      setSuccess(result)

    }

    result
  }

  def mapQueryIndexes(
      indexes: Seq[QueryIndex]
  ): java.util.List[com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndex] = {
    indexes
      .map(i => {
        val builder = com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndex.newBuilder
          .setName(i.name)
          .setIsPrimary(i.isPrimary)
          .setType(i.typ match {
            case QueryIndexType.View =>
              com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndexType.VIEW
            case QueryIndexType.GSI =>
              com.couchbase.client.protocol.sdk.query.indexmanager.QueryIndexType.GSI
          })
          .setState(i.state)
          .setKeyspace(i.keyspaceId)
          .addAllIndexKey(i.indexKey.asJava)
          // [start:1.2.5]
          .setBucketName(i.bucketName)
        // [end:1.2.5]

        i.condition.map(v => builder.setCondition(v))
        // [start:1.2.5]
        i.partition.map(v => builder.setPartition(v))
        i.scopeName.map(v => builder.setScopeName(v))
        i.collectionName.map(v => builder.setCollectionName(v))
        // [end:1.2.5]

        builder.build
      })
      .asJava
  }
}
