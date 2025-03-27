/*
 * Copyright (c) 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.couchbase.client.core.transaction.util.DebugUtil;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreTransactionGetMultiSpec {
    public final CollectionIdentifier collectionIdentifier;
    public final String id;
    public final int specIndex;

    public CoreTransactionGetMultiSpec(CollectionIdentifier collectionIdentifier, String id, int specIndex) {
        this.collectionIdentifier = requireNonNull(collectionIdentifier);
        this.id = requireNonNull(id);
        this.specIndex = specIndex;
    }

    @Override
    public String toString() {
        return "CoreTransactionGetMultiSpec{" +
                "doc=" + DebugUtil.docId(collectionIdentifier, id) +
                ", specIndex=" + specIndex +
                '}';
    }
}
