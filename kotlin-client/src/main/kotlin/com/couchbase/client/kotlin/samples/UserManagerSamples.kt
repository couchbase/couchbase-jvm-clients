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

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.manager.user.Role
import com.couchbase.client.kotlin.manager.user.User

@Suppress("UNUSED_VARIABLE")
internal suspend fun createUser(cluster: Cluster) {
    // Create a new user
    cluster.users.upsertUser(
        User(
            username = "alice",
            password = "swordfish",
            roles = setOf(Role("data_reader", bucket = "*"))
        )
    )
}

@Suppress("UNUSED_VARIABLE")
internal suspend fun addRoleToExistingUser(cluster: Cluster) {
    // Add a new role to an existing user
    // (must get existing user first, otherwise existing values are clobbered)
    val user = cluster.users.getUser("alice").user
    cluster.users.upsertUser(
        user.copy(roles = user.roles + Role("data_writer", bucket = "*"))
    )
}

@Suppress("UNUSED_VARIABLE")
internal suspend fun changePassword(cluster: Cluster) {
    // Change the password of an existing user
    // (must get existing user first, otherwise existing values are clobbered)
    val user = cluster.users.getUser("alice").user
    cluster.users.upsertUser(
        user.copy(password = "correct horse battery staple")
    )
}
