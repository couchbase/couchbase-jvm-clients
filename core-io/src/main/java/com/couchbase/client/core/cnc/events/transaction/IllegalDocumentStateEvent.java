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
package com.couchbase.client.core.cnc.events.transaction;

import java.util.Objects;

/**
 * Indicates that a document has been modified by a non-transactional write while it is in a transaction.
 * <p>
 * The application must protect against this.
 */
public class IllegalDocumentStateEvent extends TransactionEvent {
    private final String msg;
    private final String docId;

    public IllegalDocumentStateEvent(Severity severity, String msg, String docId) {
        super(Objects.requireNonNull(severity), Category.CORE.path());
        this.msg = Objects.requireNonNull(msg);
        this.docId = Objects.requireNonNull(docId);
    }

    @Override
    public String description() {
        return msg;
    }

    public String docId() {
        return docId;
    }
}
