/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.api.kv.CoreReadPreference
import com.couchbase.client.kotlin.env.dsl.ClusterEnvironmentDslBuilder

/**
 * For operations that can fetch information from multiple nodes,
 * specifying a read preference influences which nodes the SDK talks to.
 *
 * @see ReadPreference.none
 * @see ReadPreference.preferredServerGroup
 */
public sealed class ReadPreference {

    internal abstract fun toCore(): CoreReadPreference

    override fun toString(): String = javaClass.simpleName

    internal object None : ReadPreference() {
        override fun toCore(): CoreReadPreference = CoreReadPreference.NO_PREFERENCE
    }

    internal object PreferredServerGroup : ReadPreference() {
        override fun toCore(): CoreReadPreference = CoreReadPreference.PREFERRED_SERVER_GROUP
    }

    public companion object {
        /**
         * Expresses no preference about which nodes to consult.
         */
        public fun none(): ReadPreference = None

        /**
         * Consult only nodes associated with the cluster environment's
         * preferred server group.
         *
         * See [ClusterEnvironmentDslBuilder.preferredServerGroup].
         */
        @SinceCouchbase("7.6.2") // First version that includes "serverGroup" in cluster topology.
        public fun preferredServerGroup(): ReadPreference = PreferredServerGroup
    }
}
