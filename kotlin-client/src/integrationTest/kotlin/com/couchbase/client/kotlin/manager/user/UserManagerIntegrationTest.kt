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
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.FeatureNotAvailableException
import com.couchbase.client.core.error.UserNotFoundException
import com.couchbase.client.core.util.ConsistencyUtil
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.manager.user.RoleAndOrigins.Origin
import com.couchbase.client.kotlin.retry
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.kotlin.util.use
import com.couchbase.client.kotlin.util.waitUntil
import com.couchbase.client.test.Capabilities.COLLECTIONS
import com.couchbase.client.test.Capabilities.ENTERPRISE_EDITION
import com.couchbase.client.test.ClusterType
import com.couchbase.client.test.IgnoreWhen
import com.couchbase.client.test.Services
import com.couchbase.mock.deps.org.apache.http.auth.AuthScope
import com.couchbase.mock.deps.org.apache.http.auth.UsernamePasswordCredentials
import com.couchbase.mock.deps.org.apache.http.client.CredentialsProvider
import com.couchbase.mock.deps.org.apache.http.client.methods.HttpGet
import com.couchbase.mock.deps.org.apache.http.impl.client.BasicCredentialsProvider
import com.couchbase.mock.deps.org.apache.http.impl.client.HttpClientBuilder
import com.couchbase.mock.deps.org.apache.http.util.EntityUtils
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.minutes

@IgnoreWhen(clusterTypes = [ClusterType.MOCKED])
internal class UserManagerIntegrationTest : KotlinIntegrationTest() {
    private val users: UserManager by lazy { cluster.users }

    @AfterEach
    @BeforeEach
    fun dropTestUser(): Unit = runBlocking {
        users.dropUserQuietly(cluster.core, USERNAME)
        waitUntil { !users.userExists(USERNAME) }
    }

    @Test
    fun getRoles(): Unit = runBlocking {
        // Full results vary by server version, but should at least contain the admin role.
        val roles = users.getRoles()
        assertTrue(roles.any { it.role == admin && it.description.isNotBlank() && it.displayName.isNotBlank() })
    }

    @Test
    fun getAllUsers(): Unit = runBlocking {
        upsert(User(USERNAME, password = "password"))
        assertTrue(users.getAllUsers().any { it.user.username == USERNAME })
    }

    @Test
    fun createWithBadRole(): Unit = runBlocking {
        assertThrows<CouchbaseException> {
            users.upsertUser(testUser.copy(roles = setOf(Role("bogus"))))
        }
    }

    @Test
    fun canAssignAllRoles(): Unit = runBlocking {
        val allRoles = users.getRoles().map { it.role }.toSet()
        upsert(testUser.copy(roles = allRoles))
        assertEquals(allRoles, users.getUser(testUser.username).effectiveRoles)
    }

    @Test
    @IgnoreWhen(missesCapabilities = [COLLECTIONS, ENTERPRISE_EDITION])  // No RBAC support for CE
    fun canAssignCollectionsAwareRoles(): Unit = runBlocking {
        val bucket = config().bucketname()

        assertCanCreateWithRole(Role("admin"))
        assertCanCreateWithRole(Role("data_reader", "*"))
        assertCanCreateWithRole(Role("data_reader", "*", "*", "*"))
        assertCanCreateWithRole(Role("data_reader", bucket))
        assertCanCreateWithRole(Role("data_reader", bucket, "*"))
        assertCanCreateWithRole(Role("data_reader", bucket, "_default"))
        assertCanCreateWithRole(Role("data_reader", bucket, "_default", "*"))
        assertCanCreateWithRole(Role("data_reader", bucket, "_default", "_default"))
    }

    private suspend fun assertCanCreateWithRole(role: Role) {
        upsert(testUser.copy(roles = setOf(role)))

        val meta = users.getUser(USERNAME)
        assertEquals(meta.effectiveRoles, meta.user.roles)
        assertEquals(setOf(role), meta.user.roles)

        // Roles from the server have wildcards ("*") for scope & collection.
        // The server rejects these wildcards when upserting the user (at least with Couchbase 7.1).
        // Upsert the user again to ensure we're properly handling the wildcards (by omitting them)
        upsert(meta.user)
        assertEquals(setOf(role), users.getUser(USERNAME).effectiveRoles)
    }

    @Test
    fun create(): Unit = runBlocking {
        val origPassword = testUser.password!!
        val newPassword = "newpassword"
        upsert(testUser.copy(roles = setOf(admin)))

        // must be a specific kind of admin for this to succeed (not exactly sure which)
        assertCanAuthenticate(USERNAME, origPassword)
        var userMeta: UserAndMetadata = users.getUser(USERNAME, AuthDomain.LOCAL)
        assertEquals(AuthDomain.LOCAL, userMeta.domain)
        assertEquals("Integration Test User", userMeta.user.displayName)
        val expectedRoles: Set<Role> = setOf(admin)
        assertEquals(expectedRoles, userMeta.effectiveRoles)
        assertEquals(expectedRoles, userMeta.user.roles)
        checkRoleOrigins(userMeta, admin.withOrigins(Origin("user")))
        upsert(User(USERNAME, displayName = "Renamed", roles = setOf(admin)))
        waitUntil { users.getUser(USERNAME).user.displayName == "Renamed" }

        assertCanAuthenticate(USERNAME, origPassword)
        users.upsertUser(
            User(
                username = USERNAME,
                displayName = "Renamed",
                roles = setOf(readOnlyAdmin, bucketFullAccessWildcard),
                password = newPassword,
            )
        )
        waitUntil { users.getUser(USERNAME, AuthDomain.LOCAL).user.roles.size == 2 }

        assertCanAuthenticate(USERNAME, newPassword)
        userMeta = users.getUser(USERNAME, AuthDomain.LOCAL)

        assertEquals("Renamed", userMeta.user.displayName)
        assertEquals(setOf(readOnlyAdmin, bucketFullAccessWildcard), userMeta.effectiveRoles)
        checkRoleOrigins(userMeta, readOnlyAdmin.withOrigins(Origin("user")), bucketFullAccessWildcard.withOrigins(Origin("user")))
    }

    @Test
    @IgnoreWhen(hasCapabilities = [ENTERPRISE_EDITION])
    fun `change password throws FeatureNotAvailable on community edition`(): Unit = runBlocking {
        val origPassword = "password"
        val newPassword = "newPassword"

        upsert(
            User(
                username = USERNAME,
                roles = setOf(Role("bucket_full_access", bucket.name)),
                password = origPassword,
            )
        )

        Cluster.connect(connectionString, USERNAME, origPassword).use {
            // Open a bucket for clusters that don't support global config
            it.bucket(bucket.name).waitUntilReady(1.minutes)

            assertThrows<FeatureNotAvailableException> { it.users.changePassword(newPassword) }
        }
    }

    @Test
    @IgnoreWhen(missesCapabilities = [ENTERPRISE_EDITION])
    fun `enterprise edition user can change their own password`(): Unit = runBlocking {
        val origPassword = "password"
        val newPassword = "newPassword"

        upsert(
            User(
                username = USERNAME,
                roles = setOf(Role("bucket_full_access", bucket.name)),
                password = origPassword,
            )
        )

        assertCanAuthenticate(USERNAME, origPassword)
        assertCannotAuthenticate(USERNAME, newPassword)

        Cluster.connect(connectionString, USERNAME, origPassword).use {
            // Open a bucket for clusters that don't support global config
            it.bucket(bucket.name).waitUntilReady(1.minutes)

            it.users.changePassword(newPassword)
        }

        retry {
            assertCanAuthenticate(USERNAME, newPassword)
            assertCannotAuthenticate(USERNAME, origPassword)
        }
    }

    private fun assertCannotAuthenticate(username: String, password: String) {
        assertTrue(runCatching { assertCanAuthenticate(username, password) }.isFailure)
    }

    // asserts the password is what we think it should be
    private fun assertCanAuthenticate(username: String, password: String) {
        val provider: CredentialsProvider = BasicCredentialsProvider()
        provider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(username, password))

        HttpClientBuilder.create()
            .setDefaultCredentialsProvider(provider)
            .build().use { client ->
                val node = config().nodes()[0]
                val hostAndPort = node.hostname() + ":" + node.ports()[Services.MANAGER]
                client.execute(HttpGet("http://$hostAndPort/pools"))
                    .use { response ->
                        val body = EntityUtils.toString(response.entity)
                        assertEquals(200, response.statusLine.statusCode, "Server response: $body")
                    }
            }
    }

    internal suspend fun upsert(user: User) {
        users.upsertUser(user)
        ConsistencyUtil.waitUntilUserPresent(cluster.core, AuthDomain.LOCAL.name, user.username)
        waitUntil { users.userExists(user.username) }
    }
}

private const val USERNAME = "integration-test-user"

private val testUser = User(
    username = USERNAME,
    password = "password",
    displayName = "Integration Test User",
)

private val admin = Role("admin")
private val readOnlyAdmin = Role("ro_admin")
private val bucketFullAccessWildcard = Role("bucket_full_access", "*")

internal fun checkRoleOrigins(userMeta: UserAndMetadata, vararg expected: RoleAndOrigins) {
    assertEquals(setOf(*expected), userMeta.effectiveRolesAndOrigins)
}

internal fun Role.withOrigins(vararg origins: Origin) = RoleAndOrigins(this, setOf(*origins))

internal suspend fun UserManager.userExists(username: String): Boolean {
    return try {
        getUser(username)
        true
    } catch (err: UserNotFoundException) {
        false
    }
}

internal suspend fun UserManager.dropUserQuietly(core: Core, username: String) {
    try {
        dropUser(username)
    } catch (_: UserNotFoundException) {
    }
    ConsistencyUtil.waitUntilUserDropped(core, AuthDomain.LOCAL.name, username)
}
