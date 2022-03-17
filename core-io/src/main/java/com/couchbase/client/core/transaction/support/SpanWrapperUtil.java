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
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class SpanWrapperUtil {
    public static final String DB_COUCHBASE = "db.couchbase";
    public static final String DB_COUCHBASE_TRANSACTIONS = "db.couchbase.transactions.";

    private SpanWrapperUtil() {
    }

    public static SpanWrapper basic(SpanWrapper span, String op) {
        return span.attribute("db.system", "couchbase")
                .attribute("db.operation", op)
                .attribute(DB_COUCHBASE + ".service", "transactions");
    }

    public static SpanWrapper createOp(@Nullable CoreTransactionAttemptContext ctx,
                                       RequestTracer tracer,
                                       @Nullable CollectionIdentifier collection,
                                       @Nullable String id,
                                       String op,
                                       @Nullable SpanWrapper attemptSpan) {
        SpanWrapper out = SpanWrapper.create(tracer, op, attemptSpan);
        if (ctx != null) {
            out.attribute(DB_COUCHBASE + ".transactions.transaction_id", ctx.transactionId())
                .attribute(DB_COUCHBASE + ".transactions.attempt_id", ctx.attemptId());
        }
        basic(out, op);
        if (collection != null) {
            out.attribute("db.name", collection.bucket())
                .attribute(DB_COUCHBASE + ".scope", collection.scope())
                .attribute(DB_COUCHBASE + ".collection", collection.collection());
        }
        if (id != null ) {
            out.attribute(DB_COUCHBASE + ".document", id);
        }
        return out;
    }

}
