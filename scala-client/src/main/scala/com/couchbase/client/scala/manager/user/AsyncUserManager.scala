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

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.Future
import scala.concurrent.duration.Duration


@Volatile
class AsyncUserManager(val reactive: ReactiveUserManager) {
  private[scala] val defaultManagerTimeout = reactive.defaultManagerTimeout
  private[scala] val defaultRetryStrategy = reactive.defaultRetryStrategy

  def getUser(username: String,
              domain: AuthDomain = AuthDomain.Local,
              timeout: Duration = defaultManagerTimeout,
              retryStrategy: RetryStrategy = defaultRetryStrategy): Future[UserAndMetadata] = {
    reactive.getUser(username, domain, timeout, retryStrategy).toFuture
  }

  def getAllUsers(domain: AuthDomain = AuthDomain.Local,
                  timeout: Duration = defaultManagerTimeout,
                  retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Seq[UserAndMetadata]] = {
    reactive.getAllUsers(domain, timeout, retryStrategy).collectSeq().toFuture
  }


  def upsertUser(user: User,
                 domain: AuthDomain = AuthDomain.Local,
                 timeout: Duration = defaultManagerTimeout,
                 retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Unit] = {
    reactive.upsertUser(user, domain, timeout, retryStrategy).toFuture
  }

  def dropUser(username: String,
               domain: AuthDomain = AuthDomain.Local,
               timeout: Duration = defaultManagerTimeout,
               retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Unit] = {
    reactive.dropUser(username, domain, timeout, retryStrategy).toFuture
  }


  def availableRoles(timeout: Duration = defaultManagerTimeout,
                     retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Seq[RoleAndDescription]] = {
    reactive.availableRoles(timeout, retryStrategy).collectSeq().toFuture
  }

  def getGroup(groupName: String,
               timeout: Duration = defaultManagerTimeout,
               retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Group] = {
    reactive.getGroup(groupName, timeout, retryStrategy).toFuture
  }

  def getAllGroups(timeout: Duration = defaultManagerTimeout,
                   retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Seq[Group]] = {
    reactive.getAllGroups(timeout, retryStrategy).collectSeq().toFuture
  }

  def upsertGroup(group: Group,
                  timeout: Duration = defaultManagerTimeout,
                  retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Unit] = {
    reactive.upsertGroup(group, timeout, retryStrategy).toFuture
  }

  def dropGroup(groupName: String,
                timeout: Duration = defaultManagerTimeout,
                retryStrategy: RetryStrategy = defaultRetryStrategy): Future[Unit] = {
    reactive.dropGroup(groupName, timeout, retryStrategy).toFuture
  }
}
