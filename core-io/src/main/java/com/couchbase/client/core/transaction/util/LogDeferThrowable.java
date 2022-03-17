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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.logging.RedactableArgument;

import java.util.Objects;

@Stability.Internal
public class LogDeferThrowable {
    private final Throwable err;

    public LogDeferThrowable(Throwable err) {
        this.err = Objects.requireNonNull(err);
    }

    @Override
    public String toString() {
        // Query errors sometimes contain document ids, so must be redacted
        // Log toString rather than getMessage, as with Couchbase errors this will log the context also - verbose but
        // often essential for debugging
        // Have also found in the field that having some form of the stacktrace is often essential, e.g. on CBSE-10352.
        // Without it just get something like `got error in checkATREntryForBlockingDoc: {err=NullPointerException,ec=FAIL_OTHER,msg='null'}`
        StringBuilder out = new StringBuilder(RedactableArgument.redactUser(err.toString()).toString());
        if (err.getCause() == null) {
            out.append(" no cause");
        }
        else {
            out.append(" cause: ");
            out.append(RedactableArgument.redactUser(err.getCause().toString()));
        }
        out.append(" stacktrace: ");
        out.append(DebugUtil.createElidedStacktrace(err));
        return out.toString().trim();
    }
}
