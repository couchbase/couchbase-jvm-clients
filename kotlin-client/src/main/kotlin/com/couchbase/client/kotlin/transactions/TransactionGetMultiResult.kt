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

package com.couchbase.client.kotlin.transactions

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext
import com.couchbase.client.core.transaction.CoreTransactionOptionalGetMultiResult
import com.couchbase.client.kotlin.codec.JsonSerializer
import kotlin.jvm.optionals.getOrNull

public class TransactionGetMultiResult internal constructor(
    private val specs: List<TransactionDocumentSpec>,
    private val coreResults: List<CoreTransactionOptionalGetMultiResult>,
    private val defaultJsonSerializer: JsonSerializer,
) {
    /**
     * Returns the document identified by [spec].
     *
     * @throws DocumentNotFoundException if the document does not exist
     * @throws IllegalArgumentException if the given [spec] was not used in the `getMulti` request that returned this result.
     */
    public operator fun get(spec: TransactionDocumentSpec): TransactionGetResult {
        return getOrNull(spec) ?: throw DocumentNotFoundException(ReducedKeyValueErrorContext.create(spec.documentId, spec.collection.collectionId))
    }

    /**
     * Returns the document identified by [spec], or null if not found.
     *
     * @throws IllegalArgumentException if the given [spec] was not used in the `getMulti` request that returned this result.
     */
    public fun getOrNull(spec: TransactionDocumentSpec): TransactionGetResult? {
        val index = specs.indexOf(spec)
        require(index >= 0) { "The spec '$spec' is not present in this result. Expected one of: $specs" }
        return coreResults[index].internal.getOrNull()?.let { TransactionGetResult(it, defaultJsonSerializer) }
    }
}
