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
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.tracing.RequestTracerAndDecorator;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.cnc.TracingIdentifiers.SERVICE_TRANSACTIONS;

@Stability.Internal
public class SpanWrapperUtil {
    private SpanWrapperUtil() {
    }

    public static SpanWrapper createOp(@Nullable CoreTransactionAttemptContext ctx,
                                       RequestTracerAndDecorator tracer,
                                       @Nullable CollectionIdentifier collection,
                                       @Nullable String id,
                                       String op,
                                       @Nullable SpanWrapper attemptSpan) {
        SpanWrapper out = SpanWrapper.create(tracer.requestTracer, op, attemptSpan);
        if (!out.isInternal()) {
            tracer.decorator.provideLowCardinalityAttr(TracingAttribute.OPERATION, out.span(), op);
            return setAttributes(out, tracer, ctx, collection, id);
        }
        return out;
    }

    public static SpanWrapper addOperationAttribute(RequestTracerAndDecorator tracer,
                                                    SpanWrapper span,
                                                    String op) {
        if (!span.isInternal()) {
            tracer.decorator.provideLowCardinalityAttr(TracingAttribute.OPERATION, span.span(), op);
        }
        return span;
    }

    public static SpanWrapper setAttributes(SpanWrapper out,
                                            RequestTracerAndDecorator tracer,
                                            @Nullable CoreTransactionAttemptContext ctx,
                                            @Nullable CollectionIdentifier collection,
                                            @Nullable String id) {
        if (!out.isInternal()) {
            tracer.decorator.provideLowCardinalityAttr(TracingAttribute.SERVICE, out.span(), SERVICE_TRANSACTIONS);
            if (ctx != null) {
                tracer.decorator.provideAttr(TracingAttribute.TRANSACTION_ID, out.span(), ctx.transactionId());
                tracer.decorator.provideAttr(TracingAttribute.TRANSACTION_ATTEMPT_ID, out.span(), ctx.attemptId());
            }
            if (collection != null) {
                tracer.decorator.provideLowCardinalityAttr(TracingAttribute.BUCKET_NAME, out.span(), collection.bucket());
                tracer.decorator.provideAttr(TracingAttribute.SCOPE_NAME, out.span(), collection.scope().orElse(CollectionIdentifier.DEFAULT_SCOPE));
                tracer.decorator.provideAttr(TracingAttribute.COLLECTION_NAME, out.span(), collection.collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION));
            }
            if (id != null) {
                tracer.decorator.provideAttr(TracingAttribute.DOCUMENT_ID, out.span(), id);
            }
        }
        return out;
    }

}
