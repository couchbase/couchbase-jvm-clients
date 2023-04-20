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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Wraps a RequestSpan, with the value-add that it automatically records how long the operation took for
 * transaction logging.
 */
@Stability.Internal
public class SpanWrapper {
    private final long startTime = System.nanoTime();
    private final RequestSpan span;
    private boolean finished = false;
    private final boolean isInternal;

    public long elapsedMicros() {
        return (System.nanoTime() - startTime) * 1_000;
    }

    public long finish(@Nullable Throwable err) {
        if (err != null) {
            span.recordException(err);
            span.status(RequestSpan.StatusCode.ERROR);
        }
        return finish();
    }

    public long finish() {
        if (!finished) {
            finished = true;
            long elapsed = System.nanoTime() - startTime;
            span.end();
            return TimeUnit.NANOSECONDS.toMicros(elapsed);
        }
        return 0;
    }

    public SpanWrapper(RequestSpan span) {
        this.span = Objects.requireNonNull(span);
         isInternal = CbTracing.isInternalSpan(span);
    }

    public static SpanWrapper create(RequestTracer tracer, String op, @Nullable SpanWrapper parent) {
        RequestSpan span = tracer.requestSpan(op, parent == null ? null : parent.span);
        return new SpanWrapper(span);
    }

    public <T> SpanWrapper attribute(String key, T value) {
        if (!isInternal) {
            span.attribute(key, String.valueOf(value));
        }
        return this;
    }

    public <T> SpanWrapper lowCardinalityAttribute(String key, T value) {
        if (!isInternal) {
            span.lowCardinalityAttribute(key, String.valueOf(value));
        }
        return this;
    }

    public RequestSpan span() {
        return span;
    }

    public long finishWithErrorStatus() {
        if (!isInternal) {
            span.status(RequestSpan.StatusCode.ERROR);
        }
        return finish();
    }

    public void setErrorStatus() {
        if (!isInternal) {
            span.status(RequestSpan.StatusCode.ERROR);
        }
    }

    public void recordExceptionAndSetErrorStatus(Throwable err) {
        if (!isInternal) {
            span.recordException(err);
            span.status(RequestSpan.StatusCode.ERROR);
        }
    }

    public void recordException(Throwable err) {
        if (!isInternal) {
            span.recordException(err);
        }
    }

    public boolean isInternal() {
        return isInternal;
    }
}

