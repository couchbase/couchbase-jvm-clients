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
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.cnc.TracingIdentifiers.ATTR_OPERATION;
import static com.couchbase.client.core.cnc.TracingIdentifiers.ATTR_SYSTEM;
import static com.couchbase.client.core.cnc.TracingIdentifiers.ATTR_SYSTEM_COUCHBASE;
import static com.couchbase.client.core.cnc.TracingIdentifiers.ATTR_TRANSACTION_ATTEMPT_ID;
import static com.couchbase.client.core.cnc.TracingIdentifiers.ATTR_TRANSACTION_ID;
import static com.couchbase.client.core.cnc.TracingIdentifiers.SERVICE_TRANSACTIONS;

@Stability.Internal
public class SpanWrapperUtil {
    private SpanWrapperUtil() {
    }

    public static SpanWrapper createOp(@Nullable CoreTransactionAttemptContext ctx,
                                       RequestTracer tracer,
                                       @Nullable CollectionIdentifier collection,
                                       @Nullable String id,
                                       String op,
                                       @Nullable SpanWrapper attemptSpan) {
        SpanWrapper out = SpanWrapper.create(tracer, op, attemptSpan);
        if (!out.isInternal()) {
            out.attribute(ATTR_OPERATION, op);
            return setAttributes(out, ctx, collection, id);
        }
        return out;
    }

    public static SpanWrapper setAttributes(SpanWrapper out,
                                            @Nullable CoreTransactionAttemptContext ctx,
                                            @Nullable CollectionIdentifier collection,
                                            @Nullable String id) {
        if (!out.isInternal()) {
            out.attribute(ATTR_SYSTEM, ATTR_SYSTEM_COUCHBASE)
                    .attribute(TracingIdentifiers.ATTR_SERVICE, SERVICE_TRANSACTIONS);
            if (ctx != null) {
                out.attribute(ATTR_TRANSACTION_ID, ctx.transactionId())
                        .attribute(ATTR_TRANSACTION_ATTEMPT_ID, ctx.attemptId());
            }
            if (collection != null) {
                out.attribute(TracingIdentifiers.ATTR_NAME, collection.bucket())
                        .attribute(TracingIdentifiers.ATTR_SCOPE, collection.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE))
                        .attribute(TracingIdentifiers.ATTR_COLLECTION, collection.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION));
            }
            if (id != null) {
                out.attribute(TracingIdentifiers.ATTR_DOCUMENT_ID, id);
            }
        }
        return out;
    }

}
