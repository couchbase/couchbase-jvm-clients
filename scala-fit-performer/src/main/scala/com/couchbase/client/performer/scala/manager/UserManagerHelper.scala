/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import com.couchbase.client.performer.core.util.TimeUtil.getTimeNow
import com.couchbase.client.performer.scala.util.OptionsUtil
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.cluster.usermanager.{
  GetAllGroupsResult,
  GetAllUsersResult,
  GetGroupResult,
  GetRolesResult,
  GetUserResult
}
import com.couchbase.client.scala.manager.user._
import com.couchbase.client.scala.{Cluster, ReactiveCluster}
import com.google.protobuf.Timestamp

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.Try

/** The Scala SDK takes a domain on getUser, getAllUsers, upsertUser and dropUser, so any domain the driver sends is
  * passed through (though getAllUsers ignores it and returns users from all domains).
  */
object UserManagerHelper {
  private type FitCommand = com.couchbase.client.protocol.sdk.cluster.usermanager.Command
  private type FitResult  = com.couchbase.client.protocol.sdk.cluster.usermanager.Result

  /** Abstracts over the blocking and reactive APIs.
    *
    * A domain or timeout of None calls the SDK without one, so it uses its default.
    */
  private case class Ops(
      getUser: (String, Option[AuthDomain], Option[Duration]) => Try[UserAndMetadata],
      getAllUsers: (Option[AuthDomain], Option[Duration]) => Try[Seq[UserAndMetadata]],
      upsertUser: (User, Option[AuthDomain], Option[Duration]) => Try[Unit],
      dropUser: (String, Option[AuthDomain], Option[Duration]) => Try[Unit],
      availableRoles: Option[Duration] => Try[Seq[RoleAndDescription]],
      getGroup: (String, Option[Duration]) => Try[Group],
      getAllGroups: Option[Duration] => Try[Seq[Group]],
      upsertGroup: (Group, Option[Duration]) => Try[Unit],
      dropGroup: (String, Option[Duration]) => Try[Unit],
      changePassword: (String, Option[Duration]) => Try[Unit]
  )

  private def ops(m: UserManager): Ops = Ops(
    (n, d, t) =>
      (d, t) match {
        case (Some(d), Some(t)) => m.getUser(n, d, t)
        case (Some(d), None)    => m.getUser(n, d)
        case (None, Some(t))    => m.getUser(n, timeout = t)
        case (None, None)       => m.getUser(n)
      },
    (d, t) =>
      (d, t) match {
        case (Some(d), Some(t)) => m.getAllUsers(d, t)
        case (Some(d), None)    => m.getAllUsers(d)
        case (None, Some(t))    => m.getAllUsers(timeout = t)
        case (None, None)       => m.getAllUsers()
      },
    (u, d, t) =>
      (d, t) match {
        case (Some(d), Some(t)) => m.upsertUser(u, d, t)
        case (Some(d), None)    => m.upsertUser(u, d)
        case (None, Some(t))    => m.upsertUser(u, timeout = t)
        case (None, None)       => m.upsertUser(u)
      },
    (n, d, t) =>
      (d, t) match {
        case (Some(d), Some(t)) => m.dropUser(n, d, t)
        case (Some(d), None)    => m.dropUser(n, d)
        case (None, Some(t))    => m.dropUser(n, timeout = t)
        case (None, None)       => m.dropUser(n)
      },
    t => t.fold(m.availableRoles())(m.availableRoles(_)),
    (n, t) => t.fold(m.getGroup(n))(m.getGroup(n, _)),
    t => t.fold(m.getAllGroups())(m.getAllGroups(_)),
    (g, t) => t.fold(m.upsertGroup(g))(m.upsertGroup(g, _)),
    (n, t) => t.fold(m.dropGroup(n))(m.dropGroup(n, _)),
    (p, t) => t.fold(m.changePassword(p))(m.changePassword(p, _))
  )

  private def ops(m: ReactiveUserManager): Ops = Ops(
    (n, d, t) =>
      Try((d, t) match {
        case (Some(d), Some(t)) => m.getUser(n, d, t).block()
        case (Some(d), None)    => m.getUser(n, d).block()
        case (None, Some(t))    => m.getUser(n, timeout = t).block()
        case (None, None)       => m.getUser(n).block()
      }),
    (d, t) =>
      Try((d, t) match {
        case (Some(d), Some(t)) => m.getAllUsers(d, t).collectSeq().block()
        case (Some(d), None)    => m.getAllUsers(d).collectSeq().block()
        case (None, Some(t))    => m.getAllUsers(timeout = t).collectSeq().block()
        case (None, None)       => m.getAllUsers().collectSeq().block()
      }),
    (u, d, t) =>
      Try((d, t) match {
        case (Some(d), Some(t)) => m.upsertUser(u, d, t).block()
        case (Some(d), None)    => m.upsertUser(u, d).block()
        case (None, Some(t))    => m.upsertUser(u, timeout = t).block()
        case (None, None)       => m.upsertUser(u).block()
      }),
    (n, d, t) =>
      Try((d, t) match {
        case (Some(d), Some(t)) => m.dropUser(n, d, t).block()
        case (Some(d), None)    => m.dropUser(n, d).block()
        case (None, Some(t))    => m.dropUser(n, timeout = t).block()
        case (None, None)       => m.dropUser(n).block()
      }),
    t => Try(t.fold(m.availableRoles())(m.availableRoles(_)).collectSeq().block()),
    (n, t) => Try(t.fold(m.getGroup(n))(m.getGroup(n, _)).block()),
    t => Try(t.fold(m.getAllGroups())(m.getAllGroups(_)).collectSeq().block()),
    (g, t) => Try(t.fold(m.upsertGroup(g))(m.upsertGroup(g, _)).block()),
    (n, t) => Try(t.fold(m.dropGroup(n))(m.dropGroup(n, _)).block()),
    (p, t) => Try(t.fold(m.changePassword(p))(m.changePassword(p, _)).block())
  )

  def handleUserManager(
      cluster: Cluster,
      command: com.couchbase.client.protocol.sdk.Command
  ): Result = {
    handle(ops(cluster.users), command.getClusterCommand.getUserManager)
  }

  def handleUserManagerReactive(
      cluster: ReactiveCluster,
      command: com.couchbase.client.protocol.sdk.Command
  ): Result = {
    handle(ops(cluster.users), command.getClusterCommand.getUserManager)
  }

  private def timeout(
      hasOptions: Boolean,
      hasTimeout: Boolean,
      timeout: com.google.protobuf.Duration,
      hasParentSpanId: Boolean
  ): Option[Duration] = {
    if (hasOptions && hasParentSpanId) {
      throw new UnsupportedOperationException(
        "The Scala SDK's UserManager does not support a parent span"
      )
    }
    if (hasOptions && hasTimeout) Some(OptionsUtil.convertDuration(timeout)) else None
  }

  private def domain(
      hasOptions: Boolean,
      hasDomain: Boolean,
      domain: com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain
  ): Option[AuthDomain] = {
    if (hasOptions && hasDomain) Some(convertToSdk(domain)) else None
  }

  private def handle(ops: Ops, um: FitCommand): Result = {
    import com.couchbase.client.protocol.sdk.cluster.usermanager.Command.CommandCase._

    val result = Result.newBuilder
    result.setInitiated(getTimeNow)
    val start = System.nanoTime

    val sdkResult: com.couchbase.client.protocol.sdk.Result = um.getCommandCase match {
      case GET_USER =>
        val req  = um.getGetUser
        val o    = req.getOptions
        val user = ops
          .getUser(
            req.getUsername,
            domain(req.hasOptions, o.hasDomainName, o.getDomainName),
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        userManagerResult(
          FitResultBuilder.setGetUserResult(GetUserResult.newBuilder.setUser(convertToFit(user)))
        )

      case GET_ALL_USERS =>
        val req   = um.getGetAllUsers
        val o     = req.getOptions
        val users = ops
          .getAllUsers(
            domain(req.hasOptions, o.hasDomainName, o.getDomainName),
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        userManagerResult(
          FitResultBuilder.setGetAllUsersResult(
            GetAllUsersResult.newBuilder.addAllUsers(users.map(convertToFit).asJava)
          )
        )

      case UPSERT_USER =>
        val req = um.getUpsertUser
        val o   = req.getOptions
        ops
          .upsertUser(
            convertToSdk(req.getUser),
            domain(req.hasOptions, o.hasDomainName, o.getDomainName),
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        success()

      case DROP_USER =>
        val req = um.getDropUser
        val o   = req.getOptions
        ops
          .dropUser(
            req.getUsername,
            domain(req.hasOptions, o.hasDomainName, o.getDomainName),
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        success()

      case GET_ROLES =>
        val req   = um.getGetRoles
        val o     = req.getOptions
        val roles = ops
          .availableRoles(timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId))
          .get
        userManagerResult(
          FitResultBuilder.setGetRolesResult(
            GetRolesResult.newBuilder.addAllRoles(roles.map(convertToFit).asJava)
          )
        )

      case GET_GROUP =>
        val req   = um.getGetGroup
        val o     = req.getOptions
        val group = ops
          .getGroup(
            req.getGroupName,
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        userManagerResult(
          FitResultBuilder.setGetGroupResult(
            GetGroupResult.newBuilder.setGroup(convertToFit(group))
          )
        )

      case GET_ALL_GROUPS =>
        val req    = um.getGetAllGroups
        val o      = req.getOptions
        val groups = ops
          .getAllGroups(timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId))
          .get
        userManagerResult(
          FitResultBuilder.setGetAllGroupsResult(
            GetAllGroupsResult.newBuilder.addAllGroups(groups.map(convertToFit).asJava)
          )
        )

      case UPSERT_GROUP =>
        val req = um.getUpsertGroup
        val o   = req.getOptions
        ops
          .upsertGroup(
            convertToSdk(req.getGroup),
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        success()

      case DROP_GROUP =>
        val req = um.getDropGroup
        val o   = req.getOptions
        ops
          .dropGroup(
            req.getGroupName,
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        success()

      case CHANGE_PASSWORD =>
        val req = um.getChangePassword
        val o   = req.getOptions
        ops
          .changePassword(
            req.getNewPassword,
            timeout(req.hasOptions, o.hasTimeout, o.getTimeout, o.hasParentSpanId)
          )
          .get
        success()

      case other =>
        throw new UnsupportedOperationException(s"Unknown user manager command $other")
    }

    result.setElapsedNanos(System.nanoTime - start)
    result.setSdk(sdkResult)
    result.build
  }

  private def FitResultBuilder
      : com.couchbase.client.protocol.sdk.cluster.usermanager.Result.Builder =
    com.couchbase.client.protocol.sdk.cluster.usermanager.Result.newBuilder

  private def success(): com.couchbase.client.protocol.sdk.Result =
    com.couchbase.client.protocol.sdk.Result.newBuilder.setSuccess(true).build

  private def userManagerResult(
      result: com.couchbase.client.protocol.sdk.cluster.usermanager.Result.Builder
  ): com.couchbase.client.protocol.sdk.Result =
    com.couchbase.client.protocol.sdk.Result.newBuilder.setUserManagerResult(result).build

  // FIT -> SDK

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain
  ): AuthDomain = {
    import com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain._
    in match {
      case LOCAL    => AuthDomain.Local
      case EXTERNAL => AuthDomain.External
      case other    => throw new UnsupportedOperationException(s"Unknown auth domain $other")
    }
  }

  private def convertToSdk(in: com.couchbase.client.protocol.sdk.cluster.usermanager.Role): Role =
    new Role(
      in.getName,
      if (in.hasBucket) Some(in.getBucket) else None,
      if (in.hasScope) Some(in.getScope) else None,
      if (in.hasCollection) Some(in.getCollection) else None
    )

  private def convertToSdk(in: com.couchbase.client.protocol.sdk.cluster.usermanager.User): User = {
    val out = User(in.getUsername)
      .displayName(in.getDisplayName)
      .groups(in.getGroupsList.asScala.toSeq: _*)
      .roles(in.getRolesList.asScala.toSeq.map(convertToSdk): _*)
    if (in.hasPassword) out.password(in.getPassword) else out
  }

  private def convertToSdk(
      in: com.couchbase.client.protocol.sdk.cluster.usermanager.Group
  ): Group = {
    var out = Group(in.getName).roles(in.getRolesList.asScala.toSeq.map(convertToSdk): _*)
    if (in.hasDescription) out = out.description(in.getDescription)
    if (in.hasLdapGroupReference) out = out.ldapGroupReference(in.getLdapGroupReference)
    out
  }

  // SDK -> FIT

  private def convertToFit(
      in: AuthDomain
  ): com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain = in match {
    case AuthDomain.Local => com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain.LOCAL
    case AuthDomain.External =>
      com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain.EXTERNAL
  }

  private def convertToFit(in: Role): com.couchbase.client.protocol.sdk.cluster.usermanager.Role = {
    val out = com.couchbase.client.protocol.sdk.cluster.usermanager.Role.newBuilder.setName(in.name)
    in.bucket.foreach(out.setBucket)
    in.scope.foreach(out.setScope)
    in.collection.foreach(out.setCollection)
    out.build
  }

  private def convertToFit(
      in: RoleAndDescription
  ): com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndDescription =
    com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndDescription.newBuilder
      .setRole(convertToFit(in.role))
      .setDisplayName(in.displayName)
      .setDescription(in.description)
      .build

  private def convertToFit(
      in: Origin
  ): com.couchbase.client.protocol.sdk.cluster.usermanager.Origin = {
    val out =
      com.couchbase.client.protocol.sdk.cluster.usermanager.Origin.newBuilder.setType(in.typ)
    in.name.foreach(out.setName)
    out.build
  }

  private def convertToFit(
      in: RoleAndOrigins
  ): com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndOrigins =
    com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndOrigins.newBuilder
      .setRole(convertToFit(in.role))
      .addAllOrigins(in.origins.map(convertToFit).asJava)
      .build

  private def convertToFit(in: User): com.couchbase.client.protocol.sdk.cluster.usermanager.User =
    // Password is write-only, and never returned.
    com.couchbase.client.protocol.sdk.cluster.usermanager.User.newBuilder
      .setUsername(in.username)
      .setDisplayName(in.displayName)
      .addAllGroups(in.groups.asJava)
      .addAllRoles(in.roles.map(convertToFit).asJava)
      .build

  private def convertToFit(
      in: UserAndMetadata
  ): com.couchbase.client.protocol.sdk.cluster.usermanager.UserAndMetadata = {
    val out = com.couchbase.client.protocol.sdk.cluster.usermanager.UserAndMetadata.newBuilder
      .setDomain(convertToFit(in.domain))
      .setUser(convertToFit(in.user))
      .addAllEffectiveRoles(in.effectiveRoles.map(convertToFit).asJava)
      .addAllExternalGroups(in.externalGroups.asJava)
    in.passwordChanged.foreach(i =>
      out.setPasswordChanged(Timestamp.newBuilder.setSeconds(i.getEpochSecond).setNanos(i.getNano))
    )
    out.build
  }

  private def convertToFit(
      in: Group
  ): com.couchbase.client.protocol.sdk.cluster.usermanager.Group = {
    val out = com.couchbase.client.protocol.sdk.cluster.usermanager.Group.newBuilder
      .setName(in.name)
      .setDescription(in.description)
      .addAllRoles(in.roles.map(convertToFit).asJava)
    in.ldapGroupReference.foreach(out.setLdapGroupReference)
    out.build
  }
}
