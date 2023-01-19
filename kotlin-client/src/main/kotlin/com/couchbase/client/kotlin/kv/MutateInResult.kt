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

import com.couchbase.client.core.api.kv.CoreSubdocMutateResult
import com.couchbase.client.kotlin.internal.toStringUtf8

public class MutateInResult(
    private val core: CoreSubdocMutateResult,
    private val spec: MutateInSpec,
) : MutationResult(core.cas(), core.mutationToken().orElse(null)) {

    public val SubdocLong.value: Long get() = content(this).toStringUtf8().toLong()

    internal fun content(subdoc: SubdocLong): ByteArray {
        checkSpec(subdoc.spec)
        return core.field(subdoc.index).value()
    }

    private fun checkSpec(spec: MutateInSpec) {
        require(spec == this.spec) { "Subdoc was not created from the same MutateInSpec as this result." }
    }
}
