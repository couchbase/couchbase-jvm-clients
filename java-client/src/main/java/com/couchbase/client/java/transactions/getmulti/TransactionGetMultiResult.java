/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.java.transactions.getmulti;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The result of a getMulti operation, which contains the fetched documents.
 */
@Stability.Uncommitted
public class TransactionGetMultiResult {
    private final List<TransactionGetMultiSpecResult> results;

    @Stability.Internal
    public TransactionGetMultiResult(List<TransactionGetMultiSpecResult> results) {
        this.results = requireNonNull(results);
    }

    private void validateResultIndex(int specIndex) {
        if (specIndex < 0) {
            throw new InvalidArgumentException("Index must be greater or equal to 0.", null, null);
        }
        if (specIndex >= results.size()) {
            throw new InvalidArgumentException("Index must be less than the number of results.", null, null);
        }
    }

    /**
     * If the document matching this `specIndex` existed.
     * @param specIndex this is in the same order as the `specs` list provided to the getMulti() operation.
     */
    public boolean exists(int specIndex) {
        validateResultIndex(specIndex);
        return results.get(specIndex).exists();
    }

    private TransactionGetMultiSpecResult get(int specIndex) {
        if (!exists(specIndex)) {
            throw new DocumentNotFoundException(null);
        }
        return results.get(specIndex);
    }

    /**
     * Decodes the content of a document into a {@link JsonObject}.
     * <p>
     * The JsonSerializer configured on the underlying Java SDK is used, unless a Transcoder was specified in the {@link TransactionGetMultiSpec}, in which case that is used.
     * <p>
     * The document is at the specified `specIndex`.
     * <p>
     * If the document did not exist, a {@link DocumentNotFoundException} is thrown.  {@link #exists(int)} can be called initially to check if the document existed.
     *
     * @param specIndex this is in the same order as the `specs` list provided to the getMulti() operation.
     */
    public JsonObject contentAsObject(int specIndex) {
        return get(specIndex).get().contentAsObject();
    }

    /**
     * Decodes the content of a document into the target class.
     * <p>
     * The JsonSerializer configured on the underlying Java SDK is used, unless a Transcoder was specified in the {@link TransactionGetMultiSpec}, in which case that is used.
     * <p>
     * The document is at the specified `specIndex`.
     * <p>
     * If the document did not exist, a {@link DocumentNotFoundException} is thrown.  {@link #exists(int)} can be called initially to check if the document existed.
     *
     * @param specIndex this is in the same order as the `specs` list provided to the getMulti() operation.
     * @param target the target class to decode the encoded content into.
     */
    public <T> T contentAs(int specIndex, Class<T> target) {
        return get(specIndex).get().contentAs(target);
    }

    /**
     * Decodes the content of a document into the target class.
     * <p>
     * The JsonSerializer configured on the underlying Java SDK is used, unless a Transcoder was specified in the {@link TransactionGetMultiSpec}, in which case that is used.
     * <p>
     * The document is at the specified `specIndex`.
     * <p>
     * If the document did not exist, a {@link DocumentNotFoundException} is thrown.  {@link #exists(int)} can be called initially to check if the document existed.
     *
     * @param specIndex this is in the same order as the `specs` list provided to the getMulti() operation.
     * @param target the target class to decode the encoded content into.
     */
    public <T> T contentAs(int specIndex, TypeRef<T> target) {
        return get(specIndex).get().contentAs(target);
    }

    /**
     * Returns the raw unconverted contents as a byte[], bypassing any Serializer or Transcoder.
     */
    public byte[] contentAsBytes(int specIndex) {
        return get(specIndex).get().contentAsBytes();
    }

    public int size() {
        return results.size();
    }
}
