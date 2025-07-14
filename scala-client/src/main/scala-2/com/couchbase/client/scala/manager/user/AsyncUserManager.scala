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
import com.couchbase.client.core.api.CoreCouchbaseOps
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET
import com.couchbase.client.core.endpoint.http.{CoreHttpClient, CoreHttpPath, CoreHttpResponse}
import com.couchbase.client.core.error.{
  CouchbaseException,
  GroupNotFoundException,
  UserNotFoundException
}
import com.couchbase.client.core.logging.RedactableArgument.{redactMeta, redactSystem, redactUser}
import com.couchbase.client.core.msg.{RequestTarget, ResponseStatus}
import com.couchbase.client.core.protostellar.CoreProtostellarUtil
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.util.CoreCommonConverters.makeCommonOptions
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.{CouchbasePickler, FutureConversions}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AsyncUserManager(private val couchbaseOps: CoreCouchbaseOps)(
    implicit val ec: ExecutionContext
) {
  private[scala] val defaultManagerTimeout =
    couchbaseOps.environment.timeoutConfig.managementTimeout
  private[scala] val defaultRetryStrategy = couchbaseOps.environment.retryStrategy

  private def pathForUsers = "/settings/rbac/users"

  private def pathForRoles = "/settings/rbac/roles"

  private def pathForUser(domain: AuthDomain, username: String) = {
    pathForUsers + "/" + urlEncode(domain.alias) + "/" + urlEncode(username)
  }

  private def pathForGroups = "/settings/rbac/groups"

  private def pathForGroup(name: String) = pathForGroups + "/" + urlEncode(name)

  private def pathForPassword = "/controller/changePassword"

  private def coreTry: Try[Core] = {
    couchbaseOps match {
      case core: Core => Success(core)
      case _          => Failure(CoreProtostellarUtil.unsupportedCurrentlyInProtostellar())
    }
  }

  private val httpClient: Try[CoreHttpClient] =
    coreTry.map(core => new CoreHttpClient(core, RequestTarget.manager()))

  private def sendRequest(
      method: HttpMethod,
      path: String,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): Future[CoreHttpResponse] = {
    val attempt: Try[Future[CoreHttpResponse]] = for {
      core    <- coreTry
      client  <- httpClient
      builder <- method match {
        case HttpMethod.GET =>
          Success(client.get(CoreHttpPath.path(path), makeCommonOptions(timeout, retryStrategy)))
        case HttpMethod.DELETE =>
          Success(client.delete(CoreHttpPath.path(path), makeCommonOptions(timeout, retryStrategy)))
        case _ =>
          Failure(new CouchbaseException("Internal bug, please report: unknown method " + method))
      }
    } yield FutureConversions.javaCFToScalaFuture(builder.exec(core))

    attempt.fold(Future.failed, identity)
  }

  private def sendRequest(
      method: HttpMethod,
      path: String,
      body: UrlQueryStringBuilder,
      timeout: Duration,
      retryStrategy: RetryStrategy
  ): Future[CoreHttpResponse] = {
    val attempt: Try[Future[CoreHttpResponse]] = for {
      core    <- coreTry
      client  <- httpClient
      builder <- method match {
        case HttpMethod.PUT =>
          Success(client.put(CoreHttpPath.path(path), makeCommonOptions(timeout, retryStrategy)))
        case HttpMethod.POST =>
          Success(client.post(CoreHttpPath.path(path), makeCommonOptions(timeout, retryStrategy)))
        case _ =>
          Failure(new CouchbaseException("Internal bug, please report: unknown method " + method))
      }
    } yield FutureConversions.javaCFToScalaFuture(builder.exec(core))

    attempt.fold(Future.failed, identity)
  }

  def getUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[UserAndMetadata] = {
    sendRequest(GET, pathForUser(domain, username), timeout, retryStrategy)
      .flatMap(response => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          Future.failed(new UserNotFoundException(domain.alias, username))
        } else
          checkStatus(response, "get " + domain + " user [" + redactUser(username) + "]") match {
            case Failure(err) => Future.failed(err)
            case _            =>
              val value = CouchbasePickler.read[UserAndMetadata](response.content)
              Future.successful(value)
          }
      })
  }

  protected def checkStatus(response: CoreHttpResponse, action: String): Try[Unit] = {
    if (response.status != ResponseStatus.SUCCESS) {
      Failure(
        new CouchbaseException(
          "Failed to " + action + "; response status=" + response.status + "; response " +
            "body=" + new String(response.content, UTF_8)
        )
      )
    } else Success(())
  }

  def getAllUsers(
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[UserAndMetadata]] = {
    sendRequest(GET, pathForUsers, timeout, retryStrategy)
      .flatMap(response => {
        checkStatus(response, "get all users") match {
          case Failure(err) => Future.failed(err)
          case _            =>
            val value = CouchbasePickler.read[Seq[UserAndMetadata]](response.content)
            Future.successful(value)
        }
      })
  }

  def upsertUser(
      user: User,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
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
      .flatMap(response => {

        checkStatus(response, "create user [" + redactUser(user.username) + "]") match {
          case Failure(err) => Future.failed(err)
          case _            => Future.successful(())
        }
      })
  }

  def dropUser(
      username: String,
      domain: AuthDomain = AuthDomain.Local,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    sendRequest(HttpMethod.DELETE, pathForUser(domain, username), timeout, retryStrategy)
      .flatMap(response => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          Future.failed(new UserNotFoundException(domain.alias, username))
        } else {
          checkStatus(response, "drop user [" + redactUser(username) + "]") match {
            case Failure(err) => Future.failed(err)
            case _            => Future.successful(())
          }
        }
      })
  }

  def changePassword(
      newPassword: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    val params = UrlQueryStringBuilder.createForUrlSafeNames().add("password", newPassword)

    sendRequest(HttpMethod.POST, pathForPassword, params, timeout, retryStrategy)
      .flatMap(response => {
        checkStatus(response, "change user password") match {
          case Failure(err) => Future.failed(err)
          case _            => Future.successful(())
        }
      })
  }

  def availableRoles(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[RoleAndDescription]] = {
    sendRequest(GET, pathForRoles, timeout, retryStrategy)
      .flatMap(response => {

        checkStatus(response, "get all roles") match {
          case Failure(err) => Future.failed(err)
          case _            =>
            val converted = AsyncUserManager.convertRoles(response.content())
            val values    = CouchbasePickler.read[Seq[RoleAndDescription]](converted)
            Future.successful(values)
        }
      })
  }

  def getGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Group] = {
    sendRequest(HttpMethod.GET, pathForGroup(groupName), timeout, retryStrategy)
      .flatMap(response => {
        if (response.status == ResponseStatus.NOT_FOUND) {
          Future.failed(new GroupNotFoundException(groupName))
        } else {
          checkStatus(response, "get group [" + redactMeta(groupName) + "]") match {
            case Failure(err) => Future.failed(err)
            case _            =>
              val value = CouchbasePickler.read[Group](response.content)
              Future.successful(value)
          }
        }
      })
  }

  def getAllGroups(
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Seq[Group]] = {
    sendRequest(GET, pathForGroups, timeout, retryStrategy)
      .flatMap(response => {
        checkStatus(response, "get all groups") match {
          case Failure(err) => Future.failed(err)
          case _            =>
            val values = CouchbasePickler.read[Seq[Group]](response.content())
            Future.successful(values)
        }
      })
  }

  def upsertGroup(
      group: Group,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    val params = UrlQueryStringBuilder.createForUrlSafeNames
      .add("description", group.description)
      .add("roles", group.roles.map(_.format).mkString(","))

    group.ldapGroupReference.foreach(lgr => params.add("ldap_group_ref", lgr))

    sendRequest(HttpMethod.PUT, pathForGroup(group.name), params, timeout, retryStrategy)
      .flatMap(response => {

        checkStatus(response, "create group [" + redactSystem(group.name) + "]") match {
          case Failure(err) => Future.failed(err)
          case _            => Future.successful(())
        }
      })
  }

  def dropGroup(
      groupName: String,
      timeout: Duration = defaultManagerTimeout,
      retryStrategy: RetryStrategy = defaultRetryStrategy
  ): Future[Unit] = {
    sendRequest(HttpMethod.DELETE, pathForGroup(groupName), timeout, retryStrategy)
      .flatMap(response => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          Future.failed(new GroupNotFoundException(groupName))
        } else {
          checkStatus(response, "drop group [" + redactUser(groupName) + "]") match {
            case Failure(err) => Future.failed(err)
            case _            => Future.successful(())
          }
        }
      })
  }
}

object AsyncUserManager {
  // Some roles have an odd \u0019 which causes upickle to die
  def convertRoles(in: Array[Byte]): Array[Byte] = {
    in.map {
      case 0x19 => '\''.toByte
      case byte => byte
    }
  }
}
