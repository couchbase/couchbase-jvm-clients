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
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET
import com.couchbase.client.core.error.{
  CouchbaseException,
  GroupNotFoundException,
  UserNotFoundException
}
import com.couchbase.client.core.logging.RedactableArgument.{redactMeta, redactSystem, redactUser}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.manager.{GenericManagerRequest, GenericManagerResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.manager.ManagerUtil
import com.couchbase.client.scala.util.CouchbasePickler
import com.couchbase.client.scala.util.DurationConversions._
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

object ReactiveUserManager {
  // Some roles have an odd \u0019 which causes upickle to die
  def convertRoles(in: Array[Byte]): Array[Byte] = {
    in.map {
      case 0x19 => '\''.toByte
      case byte => byte
    }
  }
}

@Volatile
class ReactiveUserManager(private val core: Core) {
  private[scala] val defaultManagerTimeout =
    core.context().environment().timeoutConfig().managementTimeout()
  private[scala] val defaultRetryStrategy = core.context().environment().retryStrategy()

  private def pathForUsers = "/settings/rbac/users"

  private def pathForRoles = "/settings/rbac/roles"

  private def pathForUser(domain: AuthDomain, username: String) = {
    pathForUsers + "/" + urlEncode(domain.alias) + "/" + urlEncode(username)
  }

  private def pathForGroups = "/settings/rbac/groups"

  private def pathForGroup(name: String) = pathForGroups + "/" + urlEncode(name)

  private def sendRequest(request: GenericManagerRequest): SMono[GenericManagerResponse] = {
    ManagerUtil.sendRequest(core, request)
  }

  private def sendRequest(
      method: HttpMethod,
      path: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[GenericManagerResponse] = {
    ManagerUtil.sendRequest(core, method, path, timeout, retryStrategy)
  }

  private def sendRequest(
      method: HttpMethod,
      path: String,
      body: UrlQueryStringBuilder,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): SMono[GenericManagerResponse] = {
    ManagerUtil.sendRequest(core, method, path, body, timeout, retryStrategy)
  }

  protected def checkStatus(response: GenericManagerResponse, action: String): Try[Unit] = {
    ManagerUtil.checkStatus(response, action)
  }

  // TODO check 'If the server response does not include an “origins” field for a role' logic
  def getUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[UserAndMetadata] = {
    sendRequest(GET, pathForUser(domain, username), timeout, retryStrategy)
      .flatMap((response: GenericManagerResponse) => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          SMono.raiseError(new UserNotFoundException(domain.alias, username))
        } else
          checkStatus(response, "get " + domain + " user [" + redactUser(username) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _ =>
              val value = CouchbasePickler.read[UserAndMetadata](response.content)
              SMono.just(value)
          }
      })
  }

  def getAllUsers(
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[UserAndMetadata] = {
    sendRequest(GET, pathForUsers, timeout, retryStrategy)
      .flatMapMany((response: GenericManagerResponse) => {

        checkStatus(response, "get all users") match {
          case Failure(err) => SFlux.raiseError(err)
          case _ =>
            val value = CouchbasePickler.read[Seq[UserAndMetadata]](response.content)
            SFlux.fromIterable(value)
        }
      })
  }

  def upsertUser(
      user: User,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {

    val params = UrlQueryStringBuilder.createForUrlSafeNames
      .add("name", user.displayName)
      .add("roles", user.roles.map(_.format).mkString(","))

    // Omit empty group list for compatibility with Couchbase Server versions < 6.5.
    // Versions >= 6.5 treat the absent parameter just like an empty list.
    if (user.groups.nonEmpty) {
      params.add("groups", user.groups.mkString(","))
    }

    // Password is required when creating user, but optional when updating existing user.
    user.password.foreach((pwd: String) => params.add("password", pwd))

    sendRequest(HttpMethod.PUT, pathForUser(domain, user.username), params, timeout, retryStrategy)
      .flatMap((response: GenericManagerResponse) => {

        checkStatus(response, "create user [" + redactUser(user.username) + "]") match {
          case Failure(err) => SMono.raiseError(err)
          case _            => SMono.just(())
        }
      })
  }

  def dropUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {

    sendRequest(HttpMethod.DELETE, pathForUser(domain, username), timeout, retryStrategy)
      .flatMap((response: GenericManagerResponse) => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          SMono.raiseError(new UserNotFoundException(domain.alias, username))
        } else {
          checkStatus(response, "drop user [" + redactUser(username) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _            => SMono.just(())
          }
        }
      })
  }

  def availableRoles(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[RoleAndDescription] = {
    sendRequest(GET, pathForRoles, timeout, retryStrategy)
      .flatMapMany((response: GenericManagerResponse) => {

        checkStatus(response, "get all roles") match {
          case Failure(err) => SFlux.raiseError(err)
          case _ =>
            val converted = ReactiveUserManager.convertRoles(response.content())
            val values    = CouchbasePickler.read[Seq[RoleAndDescription]](converted)
            SFlux.fromIterable(values)
        }
      })
  }

  def getGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Group] = {

    sendRequest(HttpMethod.GET, pathForGroup(groupName), timeout, retryStrategy)
      .flatMap((response: GenericManagerResponse) => {
        if (response.status == ResponseStatus.NOT_FOUND) {
          SMono.raiseError(new GroupNotFoundException(groupName))
        } else {
          checkStatus(response, "get group [" + redactMeta(groupName) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _ =>
              val value = CouchbasePickler.read[Group](response.content)
              SMono.just(value)
          }
        }
      })
  }

  def getAllGroups(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SFlux[Group] = {

    sendRequest(HttpMethod.GET, pathForGroups, timeout, retryStrategy)
      .flatMapMany((response: GenericManagerResponse) => {
        checkStatus(response, "get all groups") match {
          case Failure(err) => SFlux.raiseError(err)
          case _ =>
            val values = CouchbasePickler.read[Seq[Group]](response.content())
            SFlux.fromIterable(values)
        }
      })
  }

  def upsertGroup(
      group: Group,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {

    val params = UrlQueryStringBuilder.createForUrlSafeNames
      .add("description", group.description)
      .add("roles", group.roles.map(_.format).mkString(","))

    group.ldapGroupReference.foreach(lgr => params.add("ldap_group_ref", lgr))

    sendRequest(HttpMethod.PUT, pathForGroup(group.name), params, timeout, retryStrategy)
      .flatMap((response: GenericManagerResponse) => {

        checkStatus(response, "create group [" + redactSystem(group.name) + "]") match {
          case Failure(err) => SMono.raiseError(err)
          case _            => SMono.just(())
        }
      })
  }

  def dropGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): SMono[Unit] = {

    sendRequest(HttpMethod.DELETE, pathForGroup(groupName), timeout, retryStrategy)
      .flatMap((response: GenericManagerResponse) => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          SMono.raiseError(new GroupNotFoundException(groupName))
        } else {
          checkStatus(response, "drop group [" + redactUser(groupName) + "]") match {
            case Failure(err) => SMono.raiseError(err)
            case _            => SMono.just(())
          }
        }
      })
  }
}
