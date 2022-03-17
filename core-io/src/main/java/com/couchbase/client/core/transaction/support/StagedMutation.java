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
package com.couchbase.client.core.transaction.support;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.util.DebugUtil;
import reactor.util.annotation.Nullable;

import java.util.Objects;

@Stability.Internal
public class StagedMutation {
    public final String operationId;
    public final CoreTransactionGetResult doc;
    public @Nullable final byte[] content;
    public final StagedMutationType type;

    public StagedMutation(String operationId,
                          CoreTransactionGetResult doc,
                          @Nullable byte[] content,
                          StagedMutationType type) {
        this.operationId = Objects.requireNonNull(operationId);
        this.doc = Objects.requireNonNull(doc);
        this.content = content;
        this.type = Objects.requireNonNull(type);
    }

    @Override
    public String toString() {
        return type.toString() + " " + DebugUtil.docId(doc);
    }
}
