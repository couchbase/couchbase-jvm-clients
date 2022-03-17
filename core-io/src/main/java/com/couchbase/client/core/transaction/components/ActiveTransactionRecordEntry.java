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

package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.support.AttemptState;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Stability.Internal
public class ActiveTransactionRecordEntry {
    private final String atrBucket;
    private final String atrId;
    private final String attemptId;
    private final Optional<String> transactionId;
    private final AttemptState state;
    private final Optional<Long> timestampStartMillis;
    private final Optional<Long> timestampCommitMillis;
    private final Optional<Long> timestampCompleteMillis;
    private final Optional<Long> timestampRollBackMillis;
    private final Optional<Long> timestampRolledBackMillis;
    private final Optional<Integer> expiresAfterMillis;
    private final long cas;
    private final Optional<List<DocRecord>> insertedIds;
    private final Optional<List<DocRecord>> replacedIds;
    private final Optional<List<DocRecord>> removedIds;
    private final Optional<ForwardCompatibility> forwardCompatibility;
    // Added in ExtStoreDurability
    private final Optional<DurabilityLevel> durabilityLevel;

    public ActiveTransactionRecordEntry(String atrBucket,
                                        String atrId,
                                        String attemptId,
                                        Optional<String> transactionId,
                                        AttemptState state,
                                        Optional<Long> timestampStartMillis,
                                        Optional<Long> timestampCommitMillis,
                                        Optional<Long> timestampCompleteMillis,
                                        Optional<Long> timestampRollBackMillis,
                                        Optional<Long> timestampRolledBackMillis,
                                        Optional<Integer> expiresAfterMillis,
                                        // Optional.empty() indicates the fields were not present
                                        Optional<List<DocRecord>> insertedIds,
                                        Optional<List<DocRecord>> replacedIds,
                                        Optional<List<DocRecord>> removedIds,
                                        long cas,
                                        Optional<ForwardCompatibility> forwardCompatibility,
                                        Optional<DurabilityLevel> durabilityLevel
    ) {
        this.atrId = Objects.requireNonNull(atrId);
        this.atrBucket = Objects.requireNonNull(atrBucket);
        this.attemptId = Objects.requireNonNull(attemptId);
        this.transactionId = Objects.requireNonNull(transactionId);
        this.state = Objects.requireNonNull(state);
        this.timestampStartMillis = Objects.requireNonNull(timestampStartMillis);
        this.timestampCommitMillis = Objects.requireNonNull(timestampCommitMillis);
        this.timestampCompleteMillis = Objects.requireNonNull(timestampCompleteMillis);
        this.timestampRollBackMillis = Objects.requireNonNull(timestampRollBackMillis);
        this.timestampRolledBackMillis = Objects.requireNonNull(timestampRolledBackMillis);
        this.expiresAfterMillis = Objects.requireNonNull(expiresAfterMillis);
        this.cas = cas;
        this.insertedIds = Objects.requireNonNull(insertedIds);
        this.replacedIds = Objects.requireNonNull(replacedIds);
        this.removedIds = Objects.requireNonNull(removedIds);
        this.forwardCompatibility = Objects.requireNonNull(forwardCompatibility);
        this.durabilityLevel = Objects.requireNonNull(durabilityLevel);
    }


    public boolean hasExpired() {
        return hasExpired(0);
    }

    public boolean hasExpired(long safetyMarginMillis) {
        long casInMillis = cas / 1_000_000;

        if (!expiresAfterMillis.isPresent()) {
            // Should always be set, protocol bug if not
            return false;
        }

        return hasExpired(casInMillis,
                expiresAfterMillis.get() + safetyMarginMillis);
    }

    public boolean hasExpired(long casInMillis, long txnExpiresAfterMillis) {
        if (timestampStartMillis.isPresent()) {
            return (casInMillis - timestampStartMillis.get()) > txnExpiresAfterMillis;
        } else {
            return false;
        }
    }

    public long ageMillis() {
        return (cas / 1_000_000) - timestampStartMillis().orElse(0l);
    }

    public String atrId() {
        return atrId;
    }

    public String attemptId() {
        return attemptId;
    }


    /**
     * This was added with {ExtTransactionId}
     */
    public Optional<String> transactionId() {
        return transactionId;
    }

    public AttemptState state() {
        return state;
    }

    public Optional<Long> timestampStartMillis() {
        return timestampStartMillis;
    }

    /**
     * Returns the CAS of the ATR documenting containing this entry
     */
    public long cas() {
        return cas;
    }

    public Optional<List<DocRecord>> insertedIds() {
        return insertedIds;
    }

    public Optional<List<DocRecord>> replacedIds() {
        return replacedIds;
    }

    public Optional<List<DocRecord>> removedIds() {
        return removedIds;
    }

    public Optional<Integer> expiresAfterMillis() {
        return expiresAfterMillis;
    }

    public Optional<ForwardCompatibility> forwardCompatibility() {
        return forwardCompatibility;
    }

    public Optional<DurabilityLevel> durabilityLevel() {
        return durabilityLevel;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ATREntry{");
        sb.append("atr=").append(atrBucket).append('/').append(atrId);
        sb.append(",attemptId=").append(attemptId);
        sb.append(",state=").append(state);
        sb.append(",expires=").append(expiresAfterMillis).append("ms");
        sb.append(",[age=").append(ageMillis()).append("ms");
        long casInMillis = cas / 1_000_000;
        sb.append(",cas=").append(cas);
        sb.append("ns/");
        sb.append(casInMillis);
        sb.append("ms],inserted=").append(insertedIds);
        sb.append(",replaced=").append(replacedIds);
        sb.append(",removed=").append(removedIds);
        sb.append(",start=").append(timestampStartMillis).append("ms");
        sb.append(",fc=").append(forwardCompatibility.map(Object::toString).orElse("none"));
        sb.append(",dl=").append(durabilityLevel.map(Object::toString).orElse("n/a"));
        sb.append('}');
        return sb.toString();
    }
}