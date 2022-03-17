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

package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.logging.RedactableArgument;

import java.util.Objects;

import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_PER_DOC_BUCKET;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_PER_DOC_COLLECTION;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_PER_DOC_ID;
import static com.couchbase.client.core.transaction.support.TransactionFields.ATR_FIELD_PER_DOC_SCOPE;

@Stability.Internal
public class DocRecord {
    private final String bucketName;
    private final String scopeName;
    private final String collectionName;
    private final String id;

    public DocRecord(String bucketName,
                     String scopeName,
                     String collectionName,
                     String id) {
        this.bucketName = Objects.requireNonNull(bucketName);
        this.scopeName = Objects.requireNonNull(scopeName);
        this.collectionName = Objects.requireNonNull(collectionName);
        this.id = Objects.requireNonNull(id);
    }

    public String bucketName() {
        return bucketName;
    }

    public String scopeName() {
        return scopeName;
    }

    public String collectionName() {
        return collectionName;
    }

    public String id() {
        return id;
    }

    public static DocRecord createFrom(JsonNode o) {
        return new DocRecord(
                o.get(ATR_FIELD_PER_DOC_BUCKET).textValue(),
                o.get(ATR_FIELD_PER_DOC_SCOPE).textValue(),
                o.get(ATR_FIELD_PER_DOC_COLLECTION).textValue(),
                o.get(ATR_FIELD_PER_DOC_ID).textValue()
        );
    }

    @Override
    public String toString() {
        // All docs in a txn get logged on one line, so keep this very brief
        final StringBuilder sb = new StringBuilder();
        sb.append(RedactableArgument.redactUser(bucketName));
        if (!scopeName.equals("_default")) {
            sb.append('.');
            sb.append(RedactableArgument.redactUser(scopeName));
        }
        if (!collectionName.equals("_default")) {
            sb.append('.');
            sb.append(RedactableArgument.redactUser(collectionName));
        }
        sb.append('.');
        sb.append(RedactableArgument.redactUser(id));
        return sb.toString();

    }
}
