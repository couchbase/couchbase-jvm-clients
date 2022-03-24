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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import java.time.Instant

/**
 * Information about a user, as returned from [UserManager.getUser] or [UserManager.getAllUsers].
 */
public class UserAndMetadata(
    public val user: User,
    public val domain: AuthDomain,
    public val passwordChanged: Instant?,
    public val externalGroups: Set<String>,
    public val effectiveRolesAndOrigins: Set<RoleAndOrigins>,
) {
    public val effectiveRoles: Set<Role> = effectiveRolesAndOrigins.map { it.role }.toSet()

    internal companion object {
        fun parse(json: ObjectNode): UserAndMetadata {
            val effectiveRolesAndOrigins = json.path("roles").map { RoleAndOrigins(it as ObjectNode) }.toSet()
            return UserAndMetadata(
                user = User(
                    username = json.path("id").textValue(),
                    displayName = json.path("name").textValue() ?: "",
                    groups = json.path("groups").map { it.textValue() }.toSet(),
                    roles = effectiveRolesAndOrigins.filter { it.innate }.map { it.role }.toSet(),
                ),
                domain = AuthDomain.of(json.path("domain").textValue()),
                passwordChanged = json.get("password_change_date")?.let { Instant.parse(it.textValue()) },
                externalGroups = json.path("external_groups").map { it.textValue() }.toSet(),
                effectiveRolesAndOrigins = effectiveRolesAndOrigins,
            )
        }
    }

    override fun toString(): String {
        return "UserAndMetadata(user=$user, authDomain=$domain, passwordChanged=$passwordChanged, externalGroups=$externalGroups, effectiveRoles=$effectiveRoles)"
    }
}

