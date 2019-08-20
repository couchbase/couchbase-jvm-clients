package com.couchbase.client.scala.manager.user

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET
import com.couchbase.client.core.deps.io.netty.handler.codec.http.{DefaultFullHttpRequest, HttpHeaderValues, HttpMethod, HttpVersion}
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.logging.RedactableArgument.{redactMeta, redactSystem, redactUser}
import com.couchbase.client.core.msg.ResponseStatus
import com.couchbase.client.core.msg.manager.{GenericManagerRequest, GenericManagerResponse}
import com.couchbase.client.core.retry.RetryStrategy
import com.couchbase.client.core.util.UrlQueryStringBuilder
import com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode
import com.couchbase.client.scala.util.DurationConversions._
import com.couchbase.client.scala.util.{CouchbasePickler, FutureConversions}
import reactor.core.scala.publisher.{Flux, Mono}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}


@Stability.Volatile
class UserNotFoundException(val domain: AuthDomain, val username: String)
  extends CouchbaseException("User [" + redactUser(username) + "] not found in " + domain + " domain.")

@Stability.Volatile
class GroupNotFoundException(val groupName: String)
  extends CouchbaseException("Group [" + redactSystem(groupName) + "] not found.")

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
  private val defaultManagerTimeout = core.context().environment().timeoutConfig().managementTimeout()
  private val defaultRetryStrategy = core.context().environment().retryStrategy()

  private def pathForUsers = "/settings/rbac/users"

  private def pathForRoles = "/settings/rbac/roles"

  private def pathForUser(domain: AuthDomain, username: String) = {
    pathForUsers + "/" + urlEncode(domain.alias) + "/" + urlEncode(username)
  }

  private def pathForGroups = "/settings/rbac/groups"

  private def pathForGroup(name: String) = pathForGroups + "/" + urlEncode(name)

  private def sendRequest(request: GenericManagerRequest): Mono[GenericManagerResponse] = {
    core.send(request)
    FutureConversions.javaCFToScalaMono(request, request.response, true)
  }

  private def sendRequest(method: HttpMethod,
                          path: String,
                          timeout: Duration,
                          retryStrategy: RetryStrategy): Mono[GenericManagerResponse] = {
    sendRequest(new GenericManagerRequest(
      timeout,
      core.context,
      retryStrategy,
      () => new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path), method == HttpMethod.GET))
  }

  private def sendRequest(method: HttpMethod,
                          path: String,
                          body: UrlQueryStringBuilder,
                          timeout: Duration,
                          retryStrategy: RetryStrategy): Mono[GenericManagerResponse] = {
    sendRequest(new GenericManagerRequest(timeout,
      core.context,
      retryStrategy,
      () => {
        val content = Unpooled.copiedBuffer(body.build, UTF_8)
        val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, content)
        req.headers.add("Content-Type", HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED)
        req.headers.add("Content-Length", content.readableBytes)
        req
      }, method == HttpMethod.GET))
  }

  protected def checkStatus(response: GenericManagerResponse, action: String): Try[Unit] = {
    if (response.status != ResponseStatus.SUCCESS) {
      Failure(new CouchbaseException("Failed to " + action + "; response status=" + response.status + "; response " +
        "body=" + new String(response.content, UTF_8)))
    }
    else Success()
  }

  // TODO check 'If the server response does not include an “origins” field for a role' logic
  def getUser(username: String,
              domain: AuthDomain = AuthDomain.Local,
              timeout: Duration = defaultManagerTimeout,
              retryStrategy: RetryStrategy = defaultRetryStrategy): Mono[UserAndMetadata] = {
    sendRequest(GET, pathForUser(domain, username), timeout, retryStrategy)

      .flatMap((response: GenericManagerResponse) => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          Mono.error(new UserNotFoundException(domain, username))
        }
        else checkStatus(response, "get " + domain + " user [" + redactUser(username) + "]") match {
          case Failure(err) => Mono.error(err)
          case _ =>
            val value = CouchbasePickler.read[UserAndMetadata](response.content)
            Mono.just(value)
        }
      })
  }

  def getAllUsers(domain: AuthDomain = AuthDomain.Local,
                  timeout: Duration = defaultManagerTimeout,
                  retryStrategy: RetryStrategy = defaultRetryStrategy): Flux[UserAndMetadata] = {
    sendRequest(GET, pathForUsers, timeout, retryStrategy)

      .flatMapMany((response: GenericManagerResponse) => {

        checkStatus(response, "get all users") match {
          case Failure(err) => Flux.error(err)
          case _ =>
            val value = CouchbasePickler.read[Seq[UserAndMetadata]](response.content)
            Flux.fromIterable(value)
        }
      })
  }


  def upsertUser(user: User,
                 domain: AuthDomain = AuthDomain.Local,
                 timeout: Duration = defaultManagerTimeout,
                 retryStrategy: RetryStrategy = defaultRetryStrategy): Mono[Unit] = {

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
          case Failure(err) => Mono.error(err)
          case _ => Mono.empty
        }
      })
  }

  def dropUser(username: String,
               domain: AuthDomain = AuthDomain.Local,
               timeout: Duration = defaultManagerTimeout,
               retryStrategy: RetryStrategy = defaultRetryStrategy): Mono[Unit] = {

    sendRequest(HttpMethod.DELETE, pathForUser(domain, username), timeout, retryStrategy)

      .flatMap((response: GenericManagerResponse) => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          Mono.error(new UserNotFoundException(domain, username))
        }
        else {
          checkStatus(response, "drop user [" + redactUser(username) + "]") match {
            case Failure(err) => Mono.error(err)
            case _ => Mono.empty
          }
        }
      })
  }


  def availableRoles(timeout: Duration = defaultManagerTimeout,
                     retryStrategy: RetryStrategy = defaultRetryStrategy): Flux[RoleAndDescription] = {
    sendRequest(GET, pathForRoles, timeout, retryStrategy)

      .flatMapMany((response: GenericManagerResponse) => {

        checkStatus(response, "get all roles") match {
          case Failure(err) => Flux.error(err)
          case _ =>
            val converted = ReactiveUserManager.convertRoles(response.content())
            val values = CouchbasePickler.read[Seq[RoleAndDescription]](converted)
            Flux.fromIterable(values)
        }
      })
  }

  def getGroup(groupName: String,
               timeout: Duration = defaultManagerTimeout,
               retryStrategy: RetryStrategy = defaultRetryStrategy): Mono[Group] = {

    sendRequest(HttpMethod.GET, pathForGroup(groupName), timeout, retryStrategy)

      .flatMap((response: GenericManagerResponse) => {
        if (response.status == ResponseStatus.NOT_FOUND) {
          Mono.error(new GroupNotFoundException(groupName))
        }
        else {
          checkStatus(response, "get group [" + redactMeta(groupName) + "]") match {
            case Failure(err) => Mono.error(err)
            case _ =>
              val value = CouchbasePickler.read[Group](response.content)
              Mono.just(value)
          }
        }
      })
  }

  def getAllGroups(timeout: Duration = defaultManagerTimeout,
                   retryStrategy: RetryStrategy = defaultRetryStrategy): Flux[Group] = {

    sendRequest(HttpMethod.GET, pathForGroups, timeout, retryStrategy)

      .flatMapMany((response: GenericManagerResponse) => {
        checkStatus(response, "get all groups") match {
          case Failure(err) => Flux.error(err)
          case _ =>
            val values = CouchbasePickler.read[Seq[Group]](response.content())
            Flux.fromIterable(values)
        }
      })
  }

  def upsertGroup(group: Group,
                  timeout: Duration = defaultManagerTimeout,
                  retryStrategy: RetryStrategy = defaultRetryStrategy): Mono[Unit] = {

    val params = UrlQueryStringBuilder.createForUrlSafeNames
      .add("description", group.description)
      .add("roles", group.roles.map(_.format).mkString(","))

    group.ldapGroupReference.foreach(lgr => params.add("ldap_group_ref", lgr))

    sendRequest(HttpMethod.PUT, pathForGroup(group.name), params, timeout, retryStrategy)

      .flatMap((response: GenericManagerResponse) => {

        checkStatus(response, "create group [" + redactSystem(group.name) + "]") match {
          case Failure(err) => Mono.error(err)
          case _ => Mono.empty
        }
      })
  }

  def dropGroup(groupName: String,
                timeout: Duration = defaultManagerTimeout,
                retryStrategy: RetryStrategy = defaultRetryStrategy): Mono[Unit] = {

    sendRequest(HttpMethod.DELETE, pathForGroup(groupName), timeout, retryStrategy)

      .flatMap((response: GenericManagerResponse) => {

        if (response.status == ResponseStatus.NOT_FOUND) {
          Mono.error(new GroupNotFoundException(groupName))
        }
        else {
          checkStatus(response, "drop group [" + redactUser(groupName) + "]") match {
            case Failure(err) => Mono.error(err)
            case _ => Mono.empty
          }
        }
      })
  }
}
