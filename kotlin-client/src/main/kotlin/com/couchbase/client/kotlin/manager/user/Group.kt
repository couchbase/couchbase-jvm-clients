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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode

@SinceCouchbase("6.5")
public class Group(
    public val name: String,
    public val description: String = "",
    public val roles: Set<Role> = emptySet(),
    public val ldapGroupReference: String? = null,
) {
    internal constructor(json: ObjectNode) : this(
        name = json.path("id").textValue(),
        description = json.path("description").textValue() ?: "",
        roles = json.path("roles").map{ Role(it as ObjectNode) }.toSet(),
        ldapGroupReference = json.path("ldap_group_ref").textValue()?.ifEmpty { null }
    )

    public fun copy(
        name: String = this.name,
        description: String = this.description,
        roles: Set<Role> = this.roles,
        ldapGroupReference: String? = this.ldapGroupReference,
    ): Group {
        return Group(name, description, roles, ldapGroupReference)
    }

    override fun toString(): String {
        return "Group(name='$name', description='$description', roles=$roles, ldapGroupReference=$ldapGroupReference)"
    }
}
