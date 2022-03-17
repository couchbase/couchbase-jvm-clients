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
import com.couchbase.client.core.io.CollectionIdentifier;

import java.util.List;
import java.util.Objects;

@Stability.Internal
public class ActiveTransactionRecords {
    private final List<ActiveTransactionRecordEntry> entries;
    private final String id;
    private final CollectionIdentifier collection;
    private final long casInNanos;
    private final CasMode casMode;

    public ActiveTransactionRecords(String id, CollectionIdentifier collection, long casInNanos, List<ActiveTransactionRecordEntry> entries, CasMode casMode) {
        // No copy required, these are created only by streams
        this.entries = Objects.requireNonNull(entries);
        this.id = Objects.requireNonNull(id);
        this.collection = Objects.requireNonNull(collection);
        this.casInNanos = casInNanos;
        this.casMode = Objects.requireNonNull(casMode);
    }

    public CollectionIdentifier collection() {
        return collection;
    }

    public String bucketName() {
        return collection.bucket();
    }

    public List<ActiveTransactionRecordEntry> entries() {
        return entries;
    }

    public String id() {
        return id;
    }

    public long cas() {
        return casInNanos;
    }

    public long casInMillis() {
        return casInNanos / 1_000_000;
    }

    public CasMode casMode() {
        return casMode;
    }
}

