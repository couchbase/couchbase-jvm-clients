/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.msg.kv.MutationToken
import com.couchbase.client.kotlin.env.dsl.IoConfigDslBuilder

/**
 * Result returned from all kinds of Key-Value mutation operations.
 */
public class MutationResult internal constructor(
    /**
     * The Compare And Swap (CAS) value of the document after the mutation.
     */
    public val cas: Long,

    /**
     * Identifies the mutation within the cluster's history.
     *
     * Always null if mutation tokens are disabled in the cluster
     * environment's I/O configuration.
     *
     * @see IoConfigDslBuilder.enableMutationTokens
     * @see MutationState
     */
    public val mutationToken: MutationToken?,
) {
    override fun toString(): String = "MutationResult(cas=$cas, mutationToken=$mutationToken)"
}
