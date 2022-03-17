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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Emitted when the lost transactions cleanup process discovered a lost transaction and attempted to clean it up.
 * <p>
 * Contains whether this attempt was successful, and all logs related to the attempt.
 */
@Stability.Uncommitted
public class TransactionCleanupAttemptEvent extends TransactionEvent {
    private final boolean success;
    private final boolean isRegular;
    private final List<TransactionLogEvent> logs;
    private final String attemptId;
    private final String atrId;
    private final CollectionIdentifier atrCollection;
    private final CleanupRequest req;
    private final String addlDebug;

    public TransactionCleanupAttemptEvent(Severity severity, boolean success, boolean isRegular,
                                          List<TransactionLogEvent> logs, String attemptId, String atrId, CollectionIdentifier atrCollection,
                                          CleanupRequest req, String addlDebug) {
        super(Objects.requireNonNull(severity), CoreTransactionsCleanup.CATEGORY);
        this.success = success;
        this.isRegular = isRegular;
        this.logs = new ArrayList<>(Objects.requireNonNull(logs));

        this.attemptId = Objects.requireNonNull(attemptId);
        this.atrId = Objects.requireNonNull(atrId);
        this.atrCollection = Objects.requireNonNull(atrCollection);
        this.req = Objects.requireNonNull(req);
        this.addlDebug = Objects.requireNonNull(addlDebug);
    }

    public List<TransactionLogEvent> logs() {
        return logs; // Defensive copy overkill here
    }

    public boolean success() {
        return success;
    }

    public String attemptId() {
        return attemptId;
    }

    public String atrId() {
        return atrId;
    }

    @Deprecated // Kept only for FIT compile compatibility, use atrCollection now
    public String atrBucket() {
        return null;
    }

    public CollectionIdentifier atrCollection() {
        return atrCollection;
    }

    @Override
    public String description() {
        StringBuilder sb = new StringBuilder();
        sb.append("Transaction cleanup attempt ");
        sb.append(success ? "succeeded" : "failed");
        sb.append(", ");
        sb.append(addlDebug);
        sb.append("isRegular=");
        sb.append(isRegular);
        // TXNJ-208: Log diagnostics automatically so the issue can be debugged
        if (!success) {
            sb.append(", logs=[");
            for (int i = 0; i < logs.size(); ++ i) {
                sb.append(logs.get(i).toString());
                if (i != logs.size() - 1) {
                    sb.append("; ");
                }
            }
            sb.append("]");
        }
        else {
            // This info is duplicated in the logs too
            sb.append(", req=");
            sb.append(req);
        }
        return sb.toString();
    }
}
