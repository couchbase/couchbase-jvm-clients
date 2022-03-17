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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.logging.RedactableArgument;

import java.util.Objects;

@Stability.Internal
public class LogDeferDocId {
    private final CollectionIdentifier collection;
    private final String docId;

    public LogDeferDocId(CollectionIdentifier collection, String docId) {
        this.collection = Objects.requireNonNull(collection);
        this.docId = Objects.requireNonNull(docId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(collection.bucket());
        sb.append('.');
        sb.append(collection.scope());
        sb.append('.');
        sb.append(collection.collection());
        sb.append('.');
        sb.append(docId);
        return RedactableArgument.redactUser(sb.toString()).toString();
    }
}
