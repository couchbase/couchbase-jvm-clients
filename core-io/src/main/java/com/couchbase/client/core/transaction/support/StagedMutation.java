/*
 * Copyright 2021 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.support;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.components.DocumentMetadata;
import com.couchbase.client.core.transaction.util.DebugUtil;
import reactor.util.annotation.Nullable;

import java.util.Optional;

@Stability.Internal
public class StagedMutation {
    public final String operationId;
    public final String id;
    public final CollectionIdentifier collection;
    public final long cas;
    public final Optional<DocumentMetadata> documentMetadata;
    // The staged content.  Will be null iff cluster does not support ReplaceBodyWithXattr
    public final @Nullable byte[] content;
    public final StagedMutationType type;

    public StagedMutation(String operationId,
                          String id,
                          CollectionIdentifier collection,
                          long cas,
                          Optional<DocumentMetadata> documentMetadata,
                          byte[] content,
                          StagedMutationType type) {
        this.operationId = operationId;
        this.id = id;
        this.collection = collection;
        this.cas = cas;
        this.documentMetadata = documentMetadata;
        this.content = content;
        this.type = type;
    }

    @Override
    public String toString() {
        return type.toString() + " " + DebugUtil.docId(collection, id);
    }
}
