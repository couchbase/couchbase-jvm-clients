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

import com.couchbase.client.core.logging.RedactableArgument.redactUser
import com.couchbase.client.kotlin.Collection

/**
 * A collection and the ID of a document in the collection.
 *
 * @see TransactionAttemptContext.plus
 */
public class TransactionDocumentSpec(
    public val collection: Collection,
    public val documentId: String,
) {
    override fun toString(): String = redactUser("${collection.keyspace.format()}/$documentId").toString()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TransactionDocumentSpec

        if (collection != other.collection) return false
        if (documentId != other.documentId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = collection.hashCode()
        result = 31 * result + documentId.hashCode()
        return result
    }
}
