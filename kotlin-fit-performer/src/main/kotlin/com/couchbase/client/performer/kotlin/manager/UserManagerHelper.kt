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

package com.couchbase.client.performer.kotlin.manager

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.manager.user.AuthDomain
import com.couchbase.client.kotlin.manager.user.Group
import com.couchbase.client.kotlin.manager.user.Role
import com.couchbase.client.kotlin.manager.user.RoleAndDescription
import com.couchbase.client.kotlin.manager.user.RoleAndOrigins
import com.couchbase.client.kotlin.manager.user.User
import com.couchbase.client.kotlin.manager.user.UserAndMetadata
import com.couchbase.client.performer.core.util.TimeUtil
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetAllGroupsResult
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetAllUsersResult
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetGroupResult
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetRolesResult
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetUserResult
import com.couchbase.client.protocol.sdk.cluster.usermanager.Origin
import com.google.protobuf.Timestamp
import kotlin.time.toKotlinDuration
import com.couchbase.client.protocol.sdk.Result as FitSdkResult
import com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain as FitAuthDomain
import com.couchbase.client.protocol.sdk.cluster.usermanager.Command as FitUserManagerCommand
import com.couchbase.client.protocol.sdk.cluster.usermanager.Group as FitGroup
import com.couchbase.client.protocol.sdk.cluster.usermanager.Result as FitUserManagerResult
import com.couchbase.client.protocol.sdk.cluster.usermanager.Role as FitRole
import com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndDescription as FitRoleAndDescription
import com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndOrigins as FitRoleAndOrigins
import com.couchbase.client.protocol.sdk.cluster.usermanager.User as FitUser
import com.couchbase.client.protocol.sdk.cluster.usermanager.UserAndMetadata as FitUserAndMetadata

/*
 * Where the Kotlin SDK's API differs from the RFC on AuthDomain (relayed to the driver through the presence of
 * SDK_USER_MANAGER_GET_ALL_USERS_RETURNS_ALL_DOMAINS and lack of SDK_USER_MANAGER_EXTERNAL_DOMAIN caps):
 *  - getUser takes an optional domain, which is passed through if provided.
 *  - upsertUser and dropUser have no domain parameter and only operate on LOCAL users, so specifying EXTERNAL is
 *    unsupported.
 *  - getAllUsers has no domain parameter and always returns users from all domains, so any domain is ignored.
 */
suspend fun handleUserManager(cluster: Cluster, command: Command, result: Result.Builder) {
    val um = command.clusterCommand.userManager
    val users = cluster.users

    result.initiated = TimeUtil.getTimeNow()
    val start = System.nanoTime()

    val sdkResult: FitSdkResult = when (um.commandCase) {
        FitUserManagerCommand.CommandCase.GET_USER -> {
            val req = um.getUser
            val o = req.options
            val domain = if (req.hasOptions() && o.hasDomainName()) o.domainName.toKotlin() else null
            val common = if (req.hasOptions()) common(o.hasTimeout(), o.timeout, o.hasParentSpanId()) else null
            val user = when {
                domain != null && common != null -> users.getUser(req.username, domain, common)
                domain != null -> users.getUser(req.username, domain)
                common != null -> users.getUser(req.username, common = common)
                else -> users.getUser(req.username)
            }
            userManagerResult(FitUserManagerResult.newBuilder()
                .setGetUserResult(GetUserResult.newBuilder().setUser(user.toFit())))
        }

        FitUserManagerCommand.CommandCase.GET_ALL_USERS -> {
            val req = um.getAllUsers
            val o = req.options
            // The Kotlin SDK has no domain parameter for getAllUsers (it returns users from all domains), so domain_name is ignored.
            val all = if (req.hasOptions()) users.getAllUsers(common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            else users.getAllUsers()
            userManagerResult(FitUserManagerResult.newBuilder()
                .setGetAllUsersResult(GetAllUsersResult.newBuilder().addAllUsers(all.map { it.toFit() })))
        }

        FitUserManagerCommand.CommandCase.UPSERT_USER -> {
            val req = um.upsertUser
            val user = req.user.toKotlin()
            if (req.hasOptions()) {
                val o = req.options
                if (o.hasDomainName()) requireLocalDomain(o.domainName, "upsertUser")
                users.upsertUser(user, common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            } else users.upsertUser(user)
            success()
        }

        FitUserManagerCommand.CommandCase.DROP_USER -> {
            val req = um.dropUser
            if (req.hasOptions()) {
                val o = req.options
                if (o.hasDomainName()) requireLocalDomain(o.domainName, "dropUser")
                users.dropUser(req.username, common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            } else users.dropUser(req.username)
            success()
        }

        FitUserManagerCommand.CommandCase.GET_ROLES -> {
            val req = um.getRoles
            val o = req.options
            val roles = if (req.hasOptions()) users.getRoles(common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            else users.getRoles()
            userManagerResult(FitUserManagerResult.newBuilder()
                .setGetRolesResult(GetRolesResult.newBuilder().addAllRoles(roles.map { it.toFit() })))
        }

        FitUserManagerCommand.CommandCase.GET_GROUP -> {
            val req = um.getGroup
            val o = req.options
            val group = if (req.hasOptions()) users.getGroup(req.groupName, common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            else users.getGroup(req.groupName)
            userManagerResult(FitUserManagerResult.newBuilder()
                .setGetGroupResult(GetGroupResult.newBuilder().setGroup(group.toFit())))
        }

        FitUserManagerCommand.CommandCase.GET_ALL_GROUPS -> {
            val req = um.getAllGroups
            val o = req.options
            val groups = if (req.hasOptions()) users.getAllGroups(common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            else users.getAllGroups()
            userManagerResult(FitUserManagerResult.newBuilder()
                .setGetAllGroupsResult(GetAllGroupsResult.newBuilder().addAllGroups(groups.map { it.toFit() })))
        }

        FitUserManagerCommand.CommandCase.UPSERT_GROUP -> {
            val req = um.upsertGroup
            val group = req.group.toKotlin()
            if (req.hasOptions()) {
                val o = req.options
                users.upsertGroup(group, common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            } else users.upsertGroup(group)
            success()
        }

        FitUserManagerCommand.CommandCase.DROP_GROUP -> {
            val req = um.dropGroup
            if (req.hasOptions()) {
                val o = req.options
                users.dropGroup(req.groupName, common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            } else users.dropGroup(req.groupName)
            success()
        }

        FitUserManagerCommand.CommandCase.CHANGE_PASSWORD -> {
            val req = um.changePassword
            if (req.hasOptions()) {
                val o = req.options
                users.changePassword(req.newPassword, common(o.hasTimeout(), o.timeout, o.hasParentSpanId()))
            } else users.changePassword(req.newPassword)
            success()
        }

        else -> throw UnsupportedOperationException("Unknown user manager command ${um.commandCase}")
    }

    result.elapsedNanos = System.nanoTime() - start
    result.setSdk(sdkResult)
}

private fun common(hasTimeout: Boolean, timeout: com.google.protobuf.Duration, hasParentSpanId: Boolean): CommonOptions {
    if (hasParentSpanId) {
        throw UnsupportedOperationException("The Kotlin performer does not support parent_span_id")
    }
    return CommonOptions(
        timeout = if (hasTimeout) java.time.Duration.ofSeconds(timeout.seconds, timeout.nanos.toLong()).toKotlinDuration() else null,
    )
}

private fun requireLocalDomain(domain: FitAuthDomain, op: String) {
    if (domain != FitAuthDomain.LOCAL) {
        throw UnsupportedOperationException("The Kotlin SDK's $op only supports the LOCAL domain, but got $domain")
    }
}

private fun success(): FitSdkResult = FitSdkResult.newBuilder().setSuccess(true).build()

private fun userManagerResult(result: FitUserManagerResult.Builder): FitSdkResult =
    FitSdkResult.newBuilder().setUserManagerResult(result).build()

// FIT -> SDK

private fun FitAuthDomain.toKotlin(): AuthDomain = when (this) {
    FitAuthDomain.LOCAL -> AuthDomain.LOCAL
    FitAuthDomain.EXTERNAL -> AuthDomain.EXTERNAL
    else -> throw UnsupportedOperationException("Unknown auth domain $this")
}

private fun FitRole.toKotlin(): Role = Role(
    name = name,
    bucket = if (hasBucket()) bucket else null,
    scope = if (hasScope()) scope else null,
    collection = if (hasCollection()) collection else null,
)

private fun FitUser.toKotlin(): User {
    val out = User(
        username = username,
        displayName = displayName,
        groups = groupsList.toSet(),
        roles = rolesList.map { it.toKotlin() }.toSet(),
    )
    return if (hasPassword()) out.copy(password = password) else out
}

private fun FitGroup.toKotlin(): Group {
    var out = Group(
        name = name,
        roles = rolesList.map { it.toKotlin() }.toSet(),
    )
    if (hasDescription()) out = out.copy(description = description)
    if (hasLdapGroupReference()) out = out.copy(ldapGroupReference = ldapGroupReference)
    return out
}

// SDK -> FIT

private fun AuthDomain.toFit(): FitAuthDomain = when (this) {
    AuthDomain.LOCAL -> FitAuthDomain.LOCAL
    AuthDomain.EXTERNAL -> FitAuthDomain.EXTERNAL
    else -> throw UnsupportedOperationException("Unknown auth domain $this")
}

private fun Role.toFit(): FitRole {
    val out = FitRole.newBuilder().setName(name)
    bucket?.let { out.setBucket(it) }
    scope?.let { out.setScope(it) }
    collection?.let { out.setCollection(it) }
    return out.build()
}

private fun RoleAndDescription.toFit(): FitRoleAndDescription = FitRoleAndDescription.newBuilder()
    .setRole(role.toFit())
    .setDisplayName(displayName)
    .setDescription(description)
    .build()

private fun RoleAndOrigins.Origin.toFit(): Origin {
    val out = Origin.newBuilder().setType(type)
    name?.let { out.setName(it) }
    return out.build()
}

private fun RoleAndOrigins.toFit(): FitRoleAndOrigins = FitRoleAndOrigins.newBuilder()
    .setRole(role.toFit())
    .addAllOrigins(origins.map { it.toFit() })
    .build()

private fun User.toFit(): FitUser = FitUser.newBuilder()
    // Password is write-only, and never returned.
    .setUsername(username)
    .setDisplayName(displayName)
    .addAllGroups(groups)
    .addAllRoles(roles.map { it.toFit() })
    .build()

private fun UserAndMetadata.toFit(): FitUserAndMetadata {
    val out = FitUserAndMetadata.newBuilder()
        .setDomain(domain.toFit())
        .setUser(user.toFit())
        .addAllEffectiveRoles(effectiveRolesAndOrigins.map { it.toFit() })
        .addAllExternalGroups(externalGroups)
    passwordChanged?.let {
        out.setPasswordChanged(Timestamp.newBuilder().setSeconds(it.epochSecond).setNanos(it.nano))
    }
    return out.build()
}

private fun Group.toFit(): FitGroup {
    val out = FitGroup.newBuilder()
        .setName(name)
        .setDescription(description)
        .addAllRoles(roles.map { it.toFit() })
    ldapGroupReference?.let { out.setLdapGroupReference(it) }
    return out.build()
}
