/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.kotlin.transactions

import com.couchbase.client.core.transaction.CoreTransactionGetResult
import com.couchbase.client.kotlin.Keyspace
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.typeRef

/**
 * A document inside a transaction.
 */
public class TransactionGetResult internal constructor(
    internal val internal: CoreTransactionGetResult,
    @property:PublishedApi internal val defaultJsonSerializer: JsonSerializer,
) {
    /**
     * The document's ID.
     */
    public val id: String = internal.id()

    /**
     * The fully-qualified name of the collection containing the document.
     */
    public val keyspace: Keyspace = internal.collection().toKeyspace()

    /**
     * The retrieved content. Useful for accessing the raw bytes
     * of the document.
     */
    public val content: Content =
        if (internal.isBinary) Content.binary(internal.contentAsBytes())
        else Content.json(internal.contentAsBytes())

    /**
     * Returns the document content after deserializing it into the type
     * specified by the type parameter.
     *
     * @param jsonSerializer the serializer to use, or null to use the serializer
     * configured on the cluster environment.
     */
    public inline fun <reified T> contentAs(jsonSerializer: JsonSerializer? = null): T {
        return (jsonSerializer ?: defaultJsonSerializer).deserialize(content.bytes, typeRef())
    }

    override fun toString(): String = internal.toString()
}
