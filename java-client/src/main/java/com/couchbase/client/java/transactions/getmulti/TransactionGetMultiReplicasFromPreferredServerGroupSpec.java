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
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.Transcoder;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A request to fetch a particular document.
 */
@Stability.Uncommitted
public class TransactionGetMultiReplicasFromPreferredServerGroupSpec {
    private final Collection collection;
    private final String id;
    private @Nullable Transcoder transcoder;

    /**
     * Creates a request to fetch a document for the given `collection` and `id`.
     */
    public static TransactionGetMultiReplicasFromPreferredServerGroupSpec create(Collection collection, String id) {
        return new TransactionGetMultiReplicasFromPreferredServerGroupSpec(collection, id);
    }

    /**
     * Provide a transcoder so that e.g. binary documents can be handled.
     * <p>
     * (Most applications will not need to set this.)
     */
    public TransactionGetMultiReplicasFromPreferredServerGroupSpec transcoder(Transcoder transcoder) {
        this.transcoder = requireNonNull(transcoder);
        return this;
    }

    @Stability.Internal
    TransactionGetMultiReplicasFromPreferredServerGroupSpec(Collection collection, String id) {
        this.collection = requireNonNull(collection);
        this.id = requireNonNull(id);
    }

    public Collection collection() {
        return collection;
    }

    public String id() {
        return id;
    }

    public @Nullable Transcoder transcoder() {
        return transcoder;
    }

    @Override
    public String toString() {
        return "TransactionGetMultiReplicaFromPreferredServerGroupSpec{" +
                "doc=" + RedactableArgument.redactUser(id) +
                '}';
    }
}
