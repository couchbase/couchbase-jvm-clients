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
import com.couchbase.client.core.error.GroupNotFoundException
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.kotlin.manager.user.RoleAndOrigins.Origin
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.util.waitUntil
import com.couchbase.client.test.Capabilities
import com.couchbase.client.test.Capabilities.ENTERPRISE_EDITION
import com.couchbase.client.test.Capabilities.USER_GROUPS
import com.couchbase.client.test.ClusterType.CAVES
import com.couchbase.client.test.ClusterType.MOCKED
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger(GroupManagerIntegrationTest::class.java)

@IgnoreWhen(
    clusterTypes = [MOCKED, CAVES],
    missesCapabilities = [USER_GROUPS, ENTERPRISE_EDITION],
)
internal class GroupManagerIntegrationTest : KotlinIntegrationTest() {
    private val users: UserManager by lazy { cluster.users }

    @AfterEach
    @BeforeEach
    fun dropTestUser(): Unit = runBlocking {
        users.dropUserQuietly(cluster.core, USERNAME)
        users.dropGroupQuietly(cluster.core, GROUP_A)
        users.dropGroupQuietly(cluster.core, GROUP_B)
        waitUntil { !users.userExists(USERNAME) }
        waitUntil { !users.groupExists(GROUP_A) }
        waitUntil { !users.groupExists(GROUP_B) }
    }

    @Test
    fun getAll(): Unit = runBlocking {
        users.upsertGroup(Group(GROUP_A))
        users.upsertGroup(Group(GROUP_B))
        ConsistencyUtil.waitUntilGroupPresent(cluster.core, GROUP_A)
        ConsistencyUtil.waitUntilGroupPresent(cluster.core, GROUP_B)
        waitUntil { users.groupExists(GROUP_A) }
        waitUntil { users.groupExists(GROUP_B) }
        val actualNames = users.getAllGroups().map { it.name }.toSet()
        assertTrue(actualNames.containsAll(setOf(GROUP_A, GROUP_B)))
        users.dropGroup(GROUP_B)
        ConsistencyUtil.waitUntilGroupDropped(cluster.core, GROUP_B)
        waitUntil { !users.groupExists(GROUP_B) }
        assertFalse(users.getAllGroups().any { it.name == GROUP_B })
    }

    @Test
    fun create(): Unit = runBlocking {
        // Pre-7.0 there's a single "security_admin" role. In 7.0 it was split into
        // "security_admin_local" and "security_admin_external".
        // Don't care about the specific role... just pick one.
        val securityAdmin = users.getRoles().map { it.role }.first { it.name.startsWith("security_admin") }

        val fakeLdapRef = "ou=Users"
        upsert(Group(GROUP_A, description = "a", roles = setOf(readOnlyAdmin), ldapGroupReference = fakeLdapRef))
        upsert(Group(GROUP_B, description = "b", roles = setOf(readOnlyAdmin, bucketFullAccessWildcard)))

        assertEquals("a", users.getGroup(GROUP_A).description)
        assertEquals("b", users.getGroup(GROUP_B).description)
        assertEquals(fakeLdapRef, users.getGroup(GROUP_A).ldapGroupReference)
        assertNull(users.getGroup(GROUP_B).ldapGroupReference)

        assertEquals(setOf(readOnlyAdmin), users.getGroup(GROUP_A).roles)
        assertEquals(setOf(readOnlyAdmin, bucketFullAccessWildcard), users.getGroup(GROUP_B).roles)

        upsert(testUser.copy(roles = setOf(securityAdmin, bucketFullAccessWildcard), groups = setOf(GROUP_A, GROUP_B)))

        var userMeta: UserAndMetadata = users.getUser(USERNAME)

        assertEquals(setOf(securityAdmin, bucketFullAccessWildcard), userMeta.user.roles)
        assertEquals(setOf(securityAdmin, bucketFullAccessWildcard, readOnlyAdmin), userMeta.effectiveRoles)

        checkRoleOrigins(
            userMeta,
            securityAdmin.withOrigins(Origin("user")),
            readOnlyAdmin.withOrigins(Origin("group", "group-a"), Origin("group", "group-b")),
            bucketFullAccessWildcard.withOrigins(Origin("group", "group-b"), Origin("user")),
        )
        users.upsertGroup(users.getGroup(GROUP_A).copy(roles = setOf(securityAdmin)))
        users.upsertGroup(users.getGroup(GROUP_B).copy(roles = setOf(securityAdmin)))
        waitUntil {
            val groupA = users.getGroup(GROUP_A)
            val groupB = users.getGroup(GROUP_B)
            log.info("Group A={} B={}", groupA, groupB)
            groupA.roles == setOf(securityAdmin) && groupB.roles == setOf(securityAdmin)
        }
        userMeta = users.getUser(USERNAME)
        assertEquals(setOf(securityAdmin, bucketFullAccessWildcard), userMeta.effectiveRoles)
    }

    @Test
    fun dropAbsentGroup(): Unit = runBlocking {
        val name = "doesnotexist"
        val e = assertThrows<GroupNotFoundException> { users.dropGroup(name) }
        assertEquals(name, e.groupName())
    }

    @Test
    @IgnoreWhen(missesCapabilities = [Capabilities.COLLECTIONS])
    fun userInheritsCollectionAwareRoles(): Unit = runBlocking {
        val bucket = config().bucketname()
        assertUserInheritsRole(Role("data_reader", bucket))
        assertUserInheritsRole(Role("data_reader", bucket, "_default"))
        assertUserInheritsRole(Role("data_reader", bucket, "_default", "_default"))
    }

    private suspend fun assertUserInheritsRole(role: Role) {
        val roles = setOf(role)
        upsert(Group(GROUP_A, roles = roles))
        assertEquals(roles, users.getGroup(GROUP_A).roles)
        upsert(testUser.copy(groups = setOf(GROUP_A)))
        assertEquals(roles, users.getUser(testUser.username).effectiveRoles)
        assertEquals(emptySet<Role>(), users.getUser(testUser.username).user.roles)
    }

    @Test
    fun removeUserFromAllGroups(): Unit = runBlocking {
        // exercise the special-case code for upserting an empty group list.
        upsert(Group(GROUP_A, roles = setOf(readOnlyAdmin)))
        upsert(testUser.copy(groups = setOf(GROUP_A)))
        var userMeta: UserAndMetadata = users.getUser(testUser.username)
        assertEquals(setOf(readOnlyAdmin), userMeta.effectiveRoles)
        users.upsertUser(userMeta.user.copy(groups = emptySet()))
        waitUntil {
            users.getUser(testUser.username).user.groups.isEmpty()
        }

        userMeta = users.getUser(testUser.username)
        assertEquals(emptySet<Role>(), userMeta.effectiveRoles)
    }

    private suspend fun upsert(user: User) {
        users.upsertUser(user)
        ConsistencyUtil.waitUntilUserPresent(cluster.core, AuthDomain.LOCAL.name, user.username)
        waitUntil { users.userExists(user.username) }
    }

    private suspend fun upsert(group: Group) {
        users.upsertGroup(group)
        ConsistencyUtil.waitUntilGroupPresent(cluster.core, group.name)
        waitUntil { users.groupExists(group.name) }
    }
}

private const val USERNAME = "integration-test-user"
private const val GROUP_A = "group-a"
private const val GROUP_B = "group-b"

private val testUser = User(
    username = USERNAME,
    password = "password",
    displayName = "Integration Test User",
)

private val readOnlyAdmin = Role("ro_admin")
private val bucketFullAccessWildcard = Role("bucket_full_access", "*")

private suspend fun UserManager.groupExists(groupName: String): Boolean {
    return try {
        getGroup(groupName)
        true
    } catch (err: GroupNotFoundException) {
        false
    }
}

private suspend fun UserManager.dropGroupQuietly(core: Core, groupName: String) {
    try {
        dropGroup(groupName)
    } catch (_: GroupNotFoundException) {
    }
    ConsistencyUtil.waitUntilGroupDropped(core, groupName)
}
