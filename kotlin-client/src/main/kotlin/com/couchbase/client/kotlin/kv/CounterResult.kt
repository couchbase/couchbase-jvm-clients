/*
 * Copyright 2021 Couchbase, Inc.
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

import com.couchbase.client.core.msg.kv.MutationToken

public class CounterResult internal constructor(
    cas: Long,
    mutationToken: MutationToken?,

    /**
     * The value of the counter after the mutation.
     */
    public val content: ULong,

    ) : MutationResult(cas, mutationToken) {
    override fun toString(): String = "CounterResult(content=$content, cas=$cas, mutationToken=$mutationToken)"
}

