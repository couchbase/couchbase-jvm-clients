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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode
import java.util.Objects.requireNonNull

public class RoleAndOrigins(
    public val role: Role,
    public val origins: Set<Origin>,
) {
    /**
     * Whether this role is assigned specifically to the user (has origin "user"
     * as opposed to being inherited from a group).
     */
    public val innate : Boolean = origins.any { it.type == "user"}

    internal constructor(json: ObjectNode) : this(
        Role(json),
        Origin.parseList(json.get("origins") as ArrayNode?).toSet()
    )

    /**
     * Indicates why a user has the role.
     *
     * An origin of type `user` means the role is assigned directly to the user (in which case the `name` field is null).
     * An origin of type `group` means the role is inherited from the group identified by the `name` field.
     */
    public class Origin(
        public val type: String,
        public val name: String? = null,
    ) {
        internal companion object {
            fun parseList(arrayNode: ArrayNode?): List<Origin> {
                if (arrayNode == null) return listOf(Origin("user")) // Server doesn't support groups, so...
                return arrayNode.map {
                    Origin(
                        requireNonNull(it.path("type").textValue(), "missing origin 'type'"),
                        it.path("name").textValue(),
                    )
                }
            }
        }

        override fun toString(): String = if (name == null) type else "$type:$name"

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Origin

            if (type != other.type) return false
            if (name != other.name) return false

            return true
        }

        override fun hashCode(): Int {
            var result = type.hashCode()
            result = 31 * result + (name?.hashCode() ?: 0)
            return result
        }
    }

    override fun toString(): String = "$role<-$origins"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RoleAndOrigins

        if (role != other.role) return false
        if (origins != other.origins) return false

        return true
    }

    override fun hashCode(): Int {
        var result = role.hashCode()
        result = 31 * result + origins.hashCode()
        return result
    }
}
