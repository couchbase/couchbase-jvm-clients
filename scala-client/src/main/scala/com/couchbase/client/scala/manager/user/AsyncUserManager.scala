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

package com.couchbase.client.scala.manager.user

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

@Volatile
class AsyncUserManager(private val couchbaseOps: CoreCouchbaseOps)(
    implicit val ec: ExecutionContext
) {
  private[scala] val defaultManagerTimeout =
    couchbaseOps.environment.timeoutConfig.managementTimeout
  private[scala] val defaultRetryStrategy = couchbaseOps.environment.retryStrategy

  private def reactive: Future[ReactiveUserManager] = {
    couchbaseOps match {
      case core: Core => Future.successful(new ReactiveUserManager(core))
      case _          => Future.failed(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }

  }

  def getUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[UserAndMetadata] = {
    reactive.flatMap(_.getUser(username, domain, timeout, retryStrategy).toFuture)
  }

  def getAllUsers(
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[UserAndMetadata]] = {
    reactive.flatMap(_.getAllUsers(domain, timeout, retryStrategy).collectSeq().toFuture)
  }

  def upsertUser(
      user: User,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.flatMap(_.upsertUser(user, domain, timeout, retryStrategy).toFuture)
  }

  def dropUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.flatMap(_.dropUser(username, domain, timeout, retryStrategy).toFuture)
  }

  def changePassword(
      newPassword: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.flatMap(_.changePassword(newPassword, timeout, retryStrategy).toFuture)
  }

  def availableRoles(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[RoleAndDescription]] = {
    reactive.flatMap(_.availableRoles(timeout, retryStrategy).collectSeq().toFuture)
  }

  def getGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Group] = {
    reactive.flatMap(_.getGroup(groupName, timeout, retryStrategy).toFuture)
  }

  def getAllGroups(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[Group]] = {
    reactive.flatMap(_.getAllGroups(timeout, retryStrategy).collectSeq().toFuture)
  }

  def upsertGroup(
      group: Group,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.flatMap(_.upsertGroup(group, timeout, retryStrategy).toFuture)
  }

  def dropGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    reactive.flatMap(_.dropGroup(groupName, timeout, retryStrategy).toFuture)
  }
}
