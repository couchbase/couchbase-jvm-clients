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
import com.couchbase.client.core.transaction.cleanup.ActiveTransactionRecordStats;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Emitted periodically with a summary of cleanup data since the last event.
 * <p>
 * As this exposes implementation details of transactions, all methods are subject to change and marked
 * with @Stability.Volatile.
 */
@Stability.Uncommitted
public class TransactionCleanupEndRunEvent extends TransactionEvent {
    private final TransactionCleanupStartRunEvent start;
    private final Duration timeOfLastRun;
    private int numAtrsChecked;
    private int numEntries;
    private long numAtrsPresent;
    private long numAtrsMissing;
    private long numAtrsErrored;
    private final Map<String, ActiveTransactionRecordStats> atrStats;
    private Optional<ActiveTransactionRecordEntry> oldest = Optional.empty();
    private int numEntriesExpired;
    private int numEntriesAttemptsFailed;
    private int largestNumEntries;

    @Stability.Volatile
    public TransactionCleanupEndRunEvent(TransactionCleanupStartRunEvent start,
                                         // Map key is ATRId
                                         Map<String, ActiveTransactionRecordStats> atrStats,
                                         Duration timeOfLastRun
    ) {
        super(Severity.INFO, CoreTransactionsCleanup.CATEGORY_STATS);
        this.start = Objects.requireNonNull(start);
        // No defensive copy as this isn't expoed to user
        this.atrStats = Objects.requireNonNull(atrStats);
        Collection<ActiveTransactionRecordStats> atrs = atrStats.values();

        numAtrsChecked = atrs.size();

        atrs.forEach(stats -> {
            if (stats.errored.isPresent()) {
                numAtrsErrored += 1;
            }
            numEntries += stats.numEntries;
            if (stats.numEntries > largestNumEntries) {
                largestNumEntries = stats.numEntries;
            }
            if (stats.exists) {
                numAtrsPresent += 1;
            } else {
                numAtrsMissing += 1;
            }
            numEntriesExpired += stats.expired.size();
            numEntriesAttemptsFailed += stats.expiredEntryCleanupFailedAttempts.get();
            if (stats.oldest().isPresent()) {
                if (!oldest.isPresent() || stats.oldest().get().ageMillis() > oldest.get().ageMillis()) {
                    oldest = stats.oldest();
                }
            }
        });

        this.timeOfLastRun = Objects.requireNonNull(timeOfLastRun);
    }

    @Override
    public String description() {
        StringBuilder sb = new StringBuilder(300);
        sb.append("start={");
        sb.append(start.description());
        sb.append("},end={taken=");
        sb.append(timeOfLastRun.toMillis());
        sb.append("millis,ATRs={tried=");
        sb.append(numAtrsChecked);
        sb.append(",present=");
        sb.append(numAtrsPresent);
        sb.append(",errored=");
        sb.append(numAtrsErrored);
        sb.append("},entries={expired=");
        sb.append(numEntriesExpired);
        sb.append(",failed=");
        sb.append(numEntriesAttemptsFailed);
        sb.append(",total=");
        sb.append(numEntries);
        sb.append("},largestNumEntries=");
        sb.append(largestNumEntries);
        if (oldest.isPresent()) {
            sb.append(",oldest=");
            ActiveTransactionRecordEntry o = oldest.get();
            sb.append(o.ageMillis());
            sb.append("millis,id=");
            sb.append(o.atrId());
            sb.append("/");
            sb.append(o.attemptId().substring(0, TransactionLogEvent.CHARS_TO_LOG));
            sb.append('}');
        }
        sb.append('}');

        return sb.toString();
    }
}
