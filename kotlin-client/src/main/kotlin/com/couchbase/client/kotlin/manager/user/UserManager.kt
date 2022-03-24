/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.manager.user

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.GroupNotFoundException
import com.couchbase.client.core.error.UserNotFoundException
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.http.CouchbaseHttpClient
import com.couchbase.client.kotlin.http.CouchbaseHttpResponse
import com.couchbase.client.kotlin.http.HttpBody
import com.couchbase.client.kotlin.http.HttpTarget
import com.couchbase.client.kotlin.http.formatPath

/**
 * @sample com.couchbase.client.kotlin.samples.createUser
 * @sample com.couchbase.client.kotlin.samples.addRoleToExistingUser
 * @sample com.couchbase.client.kotlin.samples.changePassword
 */
public class UserManager(
    internal val core: Core,
    internal val httpClient: CouchbaseHttpClient,
) {
    private fun CouchbaseHttpResponse.check(block: CouchbaseHttpResponse.() -> Unit = {}): CouchbaseHttpResponse {
        block()
        if (!success)
            throw CouchbaseException("Unexpected HTTP status code $statusCode ; $contentAsString")
        return this
    }

    /**
     * @sample com.couchbase.client.kotlin.samples.createUser
     * @sample com.couchbase.client.kotlin.samples.addRoleToExistingUser
     * @sample com.couchbase.client.kotlin.samples.changePassword
     */
    public suspend fun upsertUser(
        user: User,
        common: CommonOptions = CommonOptions.Default,
    ) {
        with(user) {
            val form = mutableMapOf<String, Any>()
            form["name"] = displayName
            form["roles"] = roles.joinToString(",") { it.format() }

            // Omit empty group list for compatibility with Couchbase Server versions < 6.5.
            // Versions >= 6.5 treat the absent parameter just like an empty list.
            groups.let { if (it.isNotEmpty()) form["groups"] = it.joinToString(",") }

            // Password is required if inserting, optional if updating
            password?.let { form["password"] = it }

            // can only create/modify local users
            val domain = AuthDomain.LOCAL

            httpClient.put(
                common = common,
                target = HttpTarget.manager(),
                path = pathForUser(domain, username),
                body = HttpBody.form(form),
            ).check()
        }
    }

    /**
     * @throws UserNotFoundException if user does not exist
     */
    public suspend fun getUser(
        username: String,
        domain: AuthDomain = AuthDomain.LOCAL,
        common: CommonOptions = CommonOptions.Default,
    ): UserAndMetadata {
        val response = httpClient.get(
            common = common,
            target = HttpTarget.manager(),
            path = pathForUser(domain, username),
        ).check {
            when (statusCode) {
                404 -> throw UserNotFoundException.forUser(domain.name, username)
            }
        }

        return UserAndMetadata.parse(Mapper.decodeIntoTree(response.content) as ObjectNode)
    }

    public suspend fun getAllUsers(
        common: CommonOptions = CommonOptions.Default,
    ): List<UserAndMetadata> {
        val json = httpClient.get(
            common = common,
            target = HttpTarget.manager(),
            path = "/settings/rbac/users",
        ).check().content

        val array = Mapper.decodeIntoTree(json) as ArrayNode
        return array.map { UserAndMetadata.parse(it as ObjectNode) }
    }

    /**
     * @throws UserNotFoundException if user does not exist
     */
    public suspend fun dropUser(
        username: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        val domain = AuthDomain.LOCAL
        httpClient.delete(
            common = common,
            target = HttpTarget.manager(),
            path = pathForUser(domain, username),
        ).check {
            when (statusCode) {
                404 -> throw UserNotFoundException.forUser(domain.name, username)
            }
        }
    }

    @SinceCouchbase("6.5")
    public suspend fun upsertGroup(
        group: Group,
        common: CommonOptions = CommonOptions.Default,
    ) {
        with(group) {
            val form = mutableMapOf<String, Any>()
            form["description"] = description
            form["ldap_group_ref"] = ldapGroupReference ?: ""
            form["roles"] = roles.joinToString(",") { it.format() }

            httpClient.put(
                common = common,
                target = HttpTarget.manager(),
                path = pathForGroup(name),
                body = HttpBody.form(form),
            ).check()
        }
    }

    /**
     * @throws GroupNotFoundException if group does not exist
     */
    @SinceCouchbase("6.5")
    public suspend fun getGroup(
        groupName: String,
        common: CommonOptions = CommonOptions.Default,
    ): Group {
        val response = httpClient.get(
            common = common,
            target = HttpTarget.manager(),
            path = pathForGroup(groupName),
        ).check {
            when (statusCode) {
                404 -> throw GroupNotFoundException.forGroup(groupName)
            }
        }
        return Group(Mapper.decodeIntoTree(response.content) as ObjectNode)
    }

    @SinceCouchbase("6.5")
    public suspend fun getAllGroups(
        common: CommonOptions = CommonOptions.Default,
    ): List<Group> {
        val response = httpClient.get(
            common = common,
            target = HttpTarget.manager(),
            path = "/settings/rbac/groups",
        ).check()
        val array = Mapper.decodeIntoTree(response.content) as ArrayNode
        return array.map { Group(it as ObjectNode) }
    }

    /**
     * @throws GroupNotFoundException if group does not exist
     */
    @SinceCouchbase("6.5")
    public suspend fun dropGroup(
        groupName: String,
        common: CommonOptions = CommonOptions.Default,
    ) {
        httpClient.delete(
            common = common,
            target = HttpTarget.manager(),
            path = pathForGroup(groupName),
        ).check {
            when (statusCode) {
                404 -> throw GroupNotFoundException.forGroup(groupName)
            }
        }
    }

    /**
     * Returns all roles supported by the Couchbase Server cluster.
     */
    public suspend fun getRoles(
        common: CommonOptions = CommonOptions.Default,
    ): List<RoleAndDescription> {
        val json = httpClient.get(
            common = common,
            target = HttpTarget.manager(),
            path = "/settings/rbac/roles",
        ).check().content

        val array = Mapper.decodeIntoTree(json) as ArrayNode
        return array.map { RoleAndDescription(it as ObjectNode) }
    }
}

private fun pathForUser(
    domain: AuthDomain = AuthDomain.LOCAL,
    username: String,
): String = formatPath("/settings/rbac/users/{}/{}", domain.name, username)


private fun pathForGroup(
    groupName: String,
): String = formatPath("/settings/rbac/groups/{}", groupName)
