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

import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;

import java.util.Objects;

/**
 * The cleanup attempt failed and was unable to cleanly return a CleanupResultEvent with !success
 *
 * @author Graham Pople
 */
public class CleanupFailedEvent extends TransactionEvent {
    private final CleanupRequest req;
    private final Throwable err;

    public CleanupFailedEvent(CleanupRequest req, Throwable err) {
        super(Severity.VERBOSE, CoreTransactionsCleanup.CATEGORY);
        this.req = Objects.requireNonNull(req);
        this.err = Objects.requireNonNull(err);
    }

    @Override
    public String description() {
        StringBuilder sb = new StringBuilder();
        sb.append("Transaction cleanup attempt threw");
        sb.append(", err='");
        sb.append(err.toString());
        sb.append("', ATR=");
        sb.append(req.atrId());
        sb.append(", attempt=");
        sb.append(req.attemptId());
        return sb.toString();
    }
}
