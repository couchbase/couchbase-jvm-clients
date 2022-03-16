/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.transactions;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;

import java.util.Objects;


/**
 * Represents a value fetched from Couchbase, along with additional transactional metadata.
 */
public class TransactionGetResult {
    private final CoreTransactionGetResult internal;
    private final JsonSerializer serializer;

    @Stability.Internal
    TransactionGetResult(CoreTransactionGetResult internal, JsonSerializer serializer) {
        this.internal = Objects.requireNonNull(internal);
        this.serializer = Objects.requireNonNull(serializer);
    }

    @Override
    public String toString() {
        return internal.toString();
    }

    CoreTransactionGetResult internal() {
        return internal;
    }

    /**
     * Returns the document's ID, which must be unique across the bucket.
     */
    public String id() {
        return internal.id();
    }

    /**
     * Decodes the content of the document into a {@link JsonObject} using the default decoder.
     */
    public JsonObject contentAsObject() {
        return contentAs(JsonObject.class);
    }

    /**
     * Decodes the content of the document into the target class.
     * <p>
     * The JsonSerializer configured on the underlying Java SDK is used.
     * <p>
     * @param target the target class to decode the encoded content into.
     */
    public <T> T contentAs(final Class<T> target) {
        return serializer.deserialize(target, internal.contentAsBytes());
    }

    /**
     * Decodes the content of the document into the target class.
     * <p>
     * The JsonSerializer configured on the underlying Java SDK is used.
     * <p>
     * @param target  the target class to decode the encoded content into.
     */
    public <T> T contentAs(final TypeRef<T> target) {
        return serializer.deserialize(target, internal.contentAsBytes());
    }

    /**
     * Returns the raw unconverted contents as a byte[].
     */
    public byte[] contentAsBytes() {
        return internal().contentAsBytes();
    }
}
