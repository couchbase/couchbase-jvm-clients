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

import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.DurationConversions._
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

class ReactiveUserManager(private val async: AsyncUserManager)(implicit val ec: ExecutionContext) {
  private[scala] val defaultManagerTimeout = async.defaultManagerTimeout
  private[scala] val defaultRetryStrategy  = async.defaultRetryStrategy

  def getUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[UserAndMetadata] = {
    SMono.fromFuture(async.getUser(username, domain, timeout, retryStrategy))
  }

  def getAllUsers(
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[UserAndMetadata] = {
    SMono
      .fromFuture(async.getAllUsers(domain, timeout, retryStrategy))
      .flatMapMany(v => SFlux.fromIterable(v))
  }

  def upsertUser(
      user: User,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.upsertUser(user, domain, timeout, retryStrategy))
  }

  def dropUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.dropUser(username, domain, timeout, retryStrategy))
  }

  def changePassword(
      newPassword: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.changePassword(newPassword, timeout, retryStrategy))
  }

  def availableRoles(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[RoleAndDescription] = {
    SMono
      .fromFuture(async.availableRoles(timeout, retryStrategy))
      .flatMapMany(v => SFlux.fromIterable(v))
  }

  def getGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Group] = {
    SMono.fromFuture(async.getGroup(groupName, timeout, retryStrategy))
  }

  def getAllGroups(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[Group] = {
    SMono
      .fromFuture(async.getAllGroups(timeout, retryStrategy))
      .flatMapMany(v => SFlux.fromIterable(v))
  }

  def upsertGroup(
      group: Group,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.upsertGroup(group, timeout, retryStrategy))
  }

  def dropGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {
    SMono.fromFuture(async.dropGroup(groupName, timeout, retryStrategy))
  }
}
