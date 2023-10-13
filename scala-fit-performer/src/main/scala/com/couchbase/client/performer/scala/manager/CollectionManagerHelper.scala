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

// [skip:<1.4.11]

import com.couchbase.client.performer.scala.ScalaSdkCommandExecutor.setSuccess
import com.couchbase.client.performer.scala.util.OptionsUtil.{
  DefaultManagementTimeout,
  DefaultRetryStrategy
}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.scala.Cluster
import com.couchbase.client.scala.manager.collection.{
  CollectionSpec,
  CreateCollectionSettings,
  ScopeSpec,
  UpdateCollectionSettings
}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object CollectionManagerHelper {

  def handleCollectionManager(cluster: Cluster, command: Command): Result.Builder = {
    val bm     = command.getBucketCommand.getCollectionManager
    val bucket = cluster.bucket(command.getBucketCommand.getBucketName)

    val result = Result.newBuilder()

    if (bm.hasGetAllScopes) {
      val req = bm.getGetAllScopes

      val response = bucket.collections.getAllScopes(
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(scopes) => populateResult(result, scopes)
        case Failure(e)      => throw e
      }
    } else if (bm.hasCreateScope) {
      val req = bm.getCreateScope
      val response = bucket.collections.createScope(
        req.getName,
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else if (bm.hasDropScope) {
      val req = bm.getDropScope
      val response = bucket.collections.dropScope(
        req.getName,
        if (req.hasOptions && req.getOptions.hasTimeoutMsecs)
          scala.concurrent.duration
            .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
        else DefaultManagementTimeout
      )

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else if (bm.hasCreateCollection) {
      val req = bm.getCreateCollection
      val response = if (req.hasOptions) {
        bucket.collections.createCollection(
          req.getScopeName,
          req.getName,
          createCollectionSettings(req.getSettings),
          if (req.getOptions.hasTimeoutMsecs)
            scala.concurrent.duration
              .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
      } else {
        bucket.collections.createCollection(
          req.getScopeName,
          req.getName,
          createCollectionSettings(req.getSettings)
        )
      }

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else if (bm.hasDropCollection) {
      val req = bm.getDropCollection
      val response = if (req.hasOptions) {
        bucket.collections.dropCollection(
          req.getScopeName,
          req.getName,
          if (req.getOptions.hasTimeoutMsecs)
            scala.concurrent.duration
              .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
      } else {
        bucket.collections.dropCollection(
          req.getScopeName,
          req.getName
        )
      }

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else if (bm.hasUpdateCollection) {
      val req = bm.getUpdateCollection
      val response = if (req.hasOptions) {
        bucket.collections.updateCollection(
          req.getScopeName,
          req.getName,
          updateCollectionSettings(req.getSettings),
          if (req.getOptions.hasTimeoutMsecs)
            scala.concurrent.duration
              .Duration(req.getOptions.getTimeoutMsecs, TimeUnit.MILLISECONDS)
          else DefaultManagementTimeout,
          DefaultRetryStrategy
        )
      } else {
        bucket.collections.updateCollection(
          req.getScopeName,
          req.getName,
          updateCollectionSettings(req.getSettings)
        )
      }

      response match {
        case Success(_) => setSuccess(result)
        case Failure(e) => throw e
      }
    } else {
      throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))
    }

    result
  }

  private def createCollectionSettings(
      bs: com.couchbase.client.protocol.sdk.bucket.collectionmanager.CreateCollectionSettings
  ): CreateCollectionSettings = {
    var cs = CreateCollectionSettings()
    if (bs.hasExpirySecs) cs = cs.maxExpiry(duration.Duration(bs.getExpirySecs, TimeUnit.SECONDS))
    if (bs.hasHistory) cs = cs.history(bs.getHistory)
    cs
  }

  private def updateCollectionSettings(
      bs: com.couchbase.client.protocol.sdk.bucket.collectionmanager.UpdateCollectionSettings
  ): UpdateCollectionSettings = {
    var cs = UpdateCollectionSettings()
    if (bs.hasExpirySecs) cs = cs.maxExpiry(duration.Duration(bs.getExpirySecs, TimeUnit.SECONDS))
    if (bs.hasHistory) cs = cs.history(bs.getHistory)
    cs
  }

  private def populateResult(
      result: com.couchbase.client.protocol.run.Result.Builder,
      response: Seq[ScopeSpec]
  ): Result.Builder = {
    result.setSdk(
      com.couchbase.client.protocol.sdk.Result
        .newBuilder()
        .setCollectionManagerResult(
          com.couchbase.client.protocol.sdk.bucket.collectionmanager.Result
            .newBuilder()
            .setGetAllScopesResult(
              com.couchbase.client.protocol.sdk.bucket.collectionmanager.GetAllScopesResult.newBuilder
                .addAllResult(response.map(v => populateScopeSpec(v)).asJava)
            )
        )
    )
  }

  private def populateScopeSpec(
      in: ScopeSpec
  ): com.couchbase.client.protocol.sdk.bucket.collectionmanager.ScopeSpec = {
    com.couchbase.client.protocol.sdk.bucket.collectionmanager.ScopeSpec
      .newBuilder()
      .setName(in.name)
      .addAllCollections(in.collections.map(v => populateCollectionSpec(v)).asJava)
      .build
  }

  private def populateCollectionSpec(
      in: CollectionSpec
  ): com.couchbase.client.protocol.sdk.bucket.collectionmanager.CollectionSpec = {
    val builder =
      com.couchbase.client.protocol.sdk.bucket.collectionmanager.CollectionSpec.newBuilder()

    builder
      .setName(in.name)
      .setScopeName(in.scopeName)

    in.expiry.foreach(v => builder.setExpirySecs(v.toSeconds.toInt))
    in.history.foreach(v => builder.setHistory(v))

    builder.build
  }
}
