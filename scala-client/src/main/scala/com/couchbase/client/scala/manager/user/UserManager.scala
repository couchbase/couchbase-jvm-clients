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
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.util.DurationConversions._

import scala.concurrent.duration.Duration
import scala.util.Try


@Volatile
class UserManager(val async: AsyncUserManager,
                  val reactive: ReactiveUserManager) {
  private val defaultManagerTimeout = async.defaultManagerTimeout
  private val defaultRetryStrategy = async.defaultRetryStrategy

  def getUser(username: String,
              domain: AuthDomain = AuthDomain.Local,
              timeout: Duration = defaultManagerTimeout,
              retryStrategy: RetryStrategy = defaultRetryStrategy): Try[UserAndMetadata] = {
    Collection.block(async.getUser(username, domain, timeout, retryStrategy))
  }

  def getAllUsers(domain: AuthDomain = AuthDomain.Local,
                  timeout: Duration = defaultManagerTimeout,
                  retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Seq[UserAndMetadata]] = {
    Collection.block(async.getAllUsers(domain, timeout, retryStrategy))
  }


  def upsertUser(user: User,
                 domain: AuthDomain = AuthDomain.Local,
                 timeout: Duration = defaultManagerTimeout,
                 retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    Collection.block(async.upsertUser(user, domain, timeout, retryStrategy))
  }

  def dropUser(username: String,
               domain: AuthDomain = AuthDomain.Local,
               timeout: Duration = defaultManagerTimeout,
               retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    Collection.block(async.dropUser(username, domain, timeout, retryStrategy))
  }

  def availableRoles(timeout: Duration = defaultManagerTimeout,
                     retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Seq[RoleAndDescription]] = {
    Collection.block(async.availableRoles(timeout, retryStrategy))
  }

  def getGroup(groupName: String,
               timeout: Duration = defaultManagerTimeout,
               retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Group] = {
    Collection.block(async.getGroup(groupName, timeout, retryStrategy))
  }

  def getAllGroups(timeout: Duration = defaultManagerTimeout,
                   retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Seq[Group]] = {
    Collection.block(async.getAllGroups(timeout, retryStrategy))
  }

  def upsertGroup(group: Group,
                  timeout: Duration = defaultManagerTimeout,
                  retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    Collection.block(async.upsertGroup(group, timeout, retryStrategy))
  }

  def dropGroup(groupName: String,
                timeout: Duration = defaultManagerTimeout,
                retryStrategy: RetryStrategy = defaultRetryStrategy): Try[Unit] = {
    Collection.block(async.dropGroup(groupName, timeout, retryStrategy))
  }
}
