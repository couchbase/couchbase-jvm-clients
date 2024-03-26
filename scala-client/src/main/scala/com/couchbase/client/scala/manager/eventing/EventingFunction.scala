/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala.manager.eventing

import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.query.QueryScanConsistency

import scala.concurrent.duration.Duration

case class EventingFunction(
    name: String,
    code: String,
    sourceKeyspace: EventingFunctionKeyspace,
    metadataKeyspace: EventingFunctionKeyspace,
    settings: Option[EventingFunctionSettings] = None,
    version: Option[String] = None,
    enforceSchema: Option[Boolean] = None,
    handlerUuid: Option[Long] = None,
    functionInstanceId: Option[String] = None,
    bucketBindings: Option[Seq[EventingFunctionBucketBinding]] = None,
    urlBindings: Option[Seq[EventingFunctionUrlBinding]] = None,
    constantBindings: Option[Seq[EventingFunctionConstantBinding]] = None
) {}

object EventingFunction {
  def create(
      name: String,
      code: String,
      sourceKeyspace: EventingFunctionKeyspace,
      metadataKeyspace: EventingFunctionKeyspace
  ): EventingFunction = {
    EventingFunction(name, code, sourceKeyspace, metadataKeyspace)
  }
}

case class EventingFunctionBucketBinding(
    alias: String,
    name: EventingFunctionKeyspace,
    access: EventingFunctionBucketAccess
)

sealed trait EventingFunctionBucketAccess

object EventingFunctionBucketAccess {
  case object ReadOnly extends EventingFunctionBucketAccess

  case object ReadWrite extends EventingFunctionBucketAccess
}

case class EventingFunctionUrlBinding(
    hostname: String,
    alias: String,
    auth: EventingFunctionUrlAuth,
    allowCookies: Boolean,
    validateSslCertificate: Boolean
)

sealed trait EventingFunctionUrlAuth

object EventingFunctionUrlAuth {
  case object None extends EventingFunctionUrlAuth

  case class Basic(username: String, password: Option[String]) extends EventingFunctionUrlAuth

  case class Digest(username: String, password: Option[String]) extends EventingFunctionUrlAuth

  case class Bearer(key: String) extends EventingFunctionUrlAuth
}

case class EventingFunctionConstantBinding(alias: String, literal: String)

case class EventingFunctionSettings(
    cppWorkerThreadCount: Option[Long] = None,
    dcpStreamBoundary: Option[EventingFunctionDcpBoundary] = None,
    description: Option[String] = None,
    logLevel: Option[EventingFunctionLogLevel] = None,
    languageCompatibility: Option[EventingFunctionLanguageCompatibility] = None,
    executionTimeout: Option[Duration] = None,
    lcbInstCapacity: Option[Long] = None,
    lcbRetryCount: Option[Long] = None,
    lcbTimeout: Option[Duration] = None,
    queryConsistency: Option[QueryScanConsistency] = None,
    numTimerPartitions: Option[Long] = None,
    sockBatchSize: Option[Long] = None,
    tickDuration: Option[Duration] = None,
    timerContextSize: Option[Long] = None,
    userPrefix: Option[String] = None,
    bucketCacheSize: Option[Long] = None,
    bucketCacheAge: Option[Long] = None,
    curlMaxAllowedRespSize: Option[Long] = None,
    workerCount: Option[Long] = None,
    queryPrepareAll: Option[Boolean] = None,
    handlerHeaders: Option[Seq[String]] = None,
    handlerFooters: Option[Seq[String]] = None,
    enableAppLogRotation: Option[Boolean] = None,
    appLogDir: Option[String] = None,
    appLogMaxSize: Option[Long] = None,
    appLogMaxFiles: Option[Long] = None,
    checkpointInterval: Option[Duration] = None,
    processingStatus: Option[EventingFunctionProcessingStatus] = None,
    deploymentStatus: Option[EventingFunctionDeploymentStatus] = None
) {}

sealed trait EventingFunctionDcpBoundary {}

object EventingFunctionDcpBoundary {
  case object Everything extends EventingFunctionDcpBoundary

  case object FromNow extends EventingFunctionDcpBoundary
}

sealed trait EventingFunctionDeploymentStatus {}

object EventingFunctionDeploymentStatus {
  case object Deployed extends EventingFunctionDeploymentStatus

  case object Undeployed extends EventingFunctionDeploymentStatus
}

sealed trait EventingFunctionProcessingStatus

object EventingFunctionProcessingStatus {
  case object Running extends EventingFunctionProcessingStatus

  case object Paused extends EventingFunctionProcessingStatus
}

sealed trait EventingFunctionLogLevel

object EventingFunctionLogLevel {
  case object Info extends EventingFunctionLogLevel

  case object Error extends EventingFunctionLogLevel

  case object Warning extends EventingFunctionLogLevel

  case object Debug extends EventingFunctionLogLevel

  case object Trace extends EventingFunctionLogLevel
}

sealed trait EventingFunctionLanguageCompatibility {}

object EventingFunctionLanguageCompatibility {
  case object Version_6_0_0 extends EventingFunctionLanguageCompatibility

  case object Version_6_5_0 extends EventingFunctionLanguageCompatibility

  case object Version_6_6_2 extends EventingFunctionLanguageCompatibility

  case object Version_7_2_0 extends EventingFunctionLanguageCompatibility
}

case class EventingFunctionKeyspace(
    bucket: String,
    scope: Option[String] = None,
    collection: Option[String] = None
)

object EventingFunctionKeyspace {
  def createFrom(collection: Collection): EventingFunctionKeyspace = {
    EventingFunctionKeyspace(
      collection.bucketName,
      Some(collection.scopeName),
      Some(collection.name)
    )
  }
}

case class EventingStatus(numEventingNodes: Int, functions: Seq[EventingFunctionState])

case class EventingFunctionState(
    name: String,
    status: EventingFunctionStatus,
    numBootstrappingNodes: Int,
    numDeployedNodes: Int,
    deploymentStatus: EventingFunctionDeploymentStatus,
    processingStatus: EventingFunctionProcessingStatus
)

sealed trait EventingFunctionStatus

object EventingFunctionStatus {
  case object Undeployed extends EventingFunctionStatus

  case object Deploying extends EventingFunctionStatus

  case object Deployed extends EventingFunctionStatus

  case object Undeploying extends EventingFunctionStatus

  case object Paused extends EventingFunctionStatus

  case object Pausing extends EventingFunctionStatus
}
