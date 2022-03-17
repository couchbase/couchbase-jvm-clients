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
import com.couchbase.client.core.transaction.cleanup.ClientRecordDetails;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;

import java.time.Duration;
import java.util.Objects;

/**
 * Emitted periodically with a summary of what will be done soon for cleanup.
 * <p>
 * As this exposes implementation details of transactions, all methods are subject to change and marked
 * with @Stability.Volatile.
 */
@Stability.Uncommitted
public class TransactionCleanupStartRunEvent extends TransactionEvent {
    private final String bucketName;
    private final String scopeName;
    private final String collectionName;
    private final String clientId;
    private final ClientRecordDetails clientDetails;
    private final Duration cleanupWindow;
    private final int atrsToCheckInNextWindow;
    private final int totalAtrs;
    private final Duration checkAtrEvery;

    @Stability.Volatile
    public TransactionCleanupStartRunEvent(String bucketName,
                                           String scopeName,
                                           String collectionName,
                                           String clientUuid,
                                           ClientRecordDetails clientDetails,
                                           Duration cleanupWindow,
                                           int atrsToCheckInNextWindow,
                                           int totalAtrs,
                                           Duration checkAtrEvery) {
        // All this info is included in TransactionCleanupEndRunEvent too (published at INFO), so publish this at DEBUG
        super(Severity.DEBUG, CoreTransactionsCleanup.CATEGORY_STATS);
        this.bucketName = Objects.requireNonNull(bucketName);
        this.scopeName = Objects.requireNonNull(scopeName);
        this.collectionName = Objects.requireNonNull(collectionName);
        this.clientId = Objects.requireNonNull(clientUuid);
        this.clientDetails = Objects.requireNonNull(clientDetails);
        this.cleanupWindow = Objects.requireNonNull(cleanupWindow);
        this.atrsToCheckInNextWindow = atrsToCheckInNextWindow;
        this.totalAtrs = totalAtrs;
        this.checkAtrEvery = Objects.requireNonNull(checkAtrEvery);
    }

    @Override
    public String description() {
        StringBuilder sb = new StringBuilder(200);
        sb.append(bucketName);
        sb.append('.');
        sb.append(scopeName);
        sb.append('.');
        sb.append(collectionName);
        sb.append("/clientId=");
        sb.append(clientId.substring(0, TransactionLogEvent.CHARS_TO_LOG));
        sb.append(",index=");
        sb.append(clientDetails.indexOfThisClient());
        sb.append(",numClients=");
        sb.append(clientDetails.numActiveClients());
        sb.append(",ATRs={checking=");
        sb.append(atrsToCheckInNextWindow);
        // Note this is expected total, not how many actually exist
        sb.append(",total=");
        sb.append(totalAtrs);
        sb.append("},runLength=");
        sb.append(cleanupWindow.toMillis());
        sb.append("millis");
        return sb.toString();
    }
}
