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

package com.couchbase.client.core.error.transaction;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.util.DebugUtil;

public class DocumentAlreadyInTransactionException extends CouchbaseException {
    private final CoreTransactionGetResult doc;

    private DocumentAlreadyInTransactionException(CoreTransactionAttemptContext ctx, CoreTransactionGetResult doc, String msg) {
        super(msg);
        this.doc = doc;
    }

    static public DocumentAlreadyInTransactionException create(CoreTransactionAttemptContext ctx, CoreTransactionGetResult doc) {
        StringBuilder msg = new StringBuilder();
        msg.append("Document ");
        msg.append(DebugUtil.docId(doc));
        msg.append(" is already in a transaction, atr=");
        msg.append(doc.links().atrBucketName().orElse("-"));
        msg.append('/');
        msg.append(doc.links().atrId().orElse("-"));
        msg.append(" attemptId=");
        msg.append(doc.links().stagedAttemptId().orElse("-"));

        return new DocumentAlreadyInTransactionException(ctx, doc, msg.toString());
    }

    public String docId() {
        return doc.id();
    }
}
