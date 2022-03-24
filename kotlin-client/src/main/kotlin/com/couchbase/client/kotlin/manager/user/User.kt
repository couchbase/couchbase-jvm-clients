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

import com.couchbase.client.core.annotation.SinceCouchbase

/**
 * A Couchbase Server user account.
 *
 * @property displayName An optional descriptive name.
 *
 * @property groups Names of the groups this user belongs to.
 * Adding a user to a group indirectly grants them the roles associated with the group.
 *
 * @property roles Roles assigned directly to the user. Does not include
 * roles inherited from groups. (If you're interested in inherited roles as well,
 * see [UserAndMetadata.effectiveRoles].)
 *
 * @property password The user's password. Always null when getting a user.
 * Must be non-null when creating a user. May be null when updating a user,
 * if the intent is to preserve the user's existing password.
 */
public class User(
    public val username: String,
    public val displayName: String = "",
    @SinceCouchbase("6.5") public val groups: Set<String> = emptySet(),
    public val roles: Set<Role> = emptySet(),
    public val password: String? = null,
) {

    public fun copy(
        username: String = this.username,
        displayName: String = this.displayName,
        groups: Set<String> = this.groups,
        roles: Set<Role> = this.roles,
        password: String? = this.password,
    ): User {
        return User(username, displayName, groups, roles, password)
    }

    override fun toString(): String {
        return "User(username='$username', displayName='$displayName', groups=$groups, roles=$roles, hasPassword=${password != null})"
    }
}
