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

package com.couchbase.client.core.transaction.cleanup;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.DocRecord;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.support.AttemptState;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@Stability.Internal
public class CleanupRequest implements Delayed {
    private final String attemptId;
    private final String atrId;
    private final CollectionIdentifier atrCollection;
    private final Duration startTime;
    private final AttemptState state;
    private final List<DocRecord> stagedReplaces;
    private final List<DocRecord> stagedRemoves;
    private final List<DocRecord> stagedInserts;
    private final Optional<ForwardCompatibility> forwardCompatibility;
    private final long ageMillis;
    private final long createdAt;
    private final Optional<DurabilityLevel> durabilityLevel;

    /**
     *
     * @param attemptId Of the attempt making this request
     * @param atrId The ATR document's ID
     * @param delay When this request will be handled
     */
    public CleanupRequest(String attemptId,
                          String atrId,
                          CollectionIdentifier atrCollection,
                          AttemptState state,
                          List<DocRecord> stagedReplaces,
                          List<DocRecord> stagedRemoves,
                          List<DocRecord> stagedInserts,
                          Duration delay,
                          Optional<ForwardCompatibility> forwardCompatibility,
                          long ageMillis,
                          Optional<DurabilityLevel> durabilityLevel) {
        Objects.requireNonNull(attemptId);
        Objects.requireNonNull(atrId);
        Objects.requireNonNull(atrCollection);

        this.attemptId = Objects.requireNonNull(attemptId);
        this.atrId = Objects.requireNonNull(atrId);
        this.atrCollection = Objects.requireNonNull(atrCollection);
        this.startTime = Duration.ofNanos(System.nanoTime()).plus(delay);
        this.stagedReplaces = new ArrayList<>(Objects.requireNonNull(stagedReplaces));
        this.stagedRemoves = new ArrayList<>(Objects.requireNonNull(stagedRemoves));
        this.stagedInserts = new ArrayList<>(Objects.requireNonNull(stagedInserts));
        this.state = Objects.requireNonNull(state);
        this.forwardCompatibility = Objects.requireNonNull(forwardCompatibility);
        this.ageMillis = ageMillis;
        this.createdAt = System.nanoTime();
        this.durabilityLevel = Objects.requireNonNull(durabilityLevel);
    }


    public static CleanupRequest fromAtrEntry(CollectionIdentifier atrCollection, ActiveTransactionRecordEntry atrEntry) {
        return new CleanupRequest(atrEntry.attemptId(),
                atrEntry.atrId(),
                atrCollection,
                atrEntry.state(),
                atrEntry.replacedIds().orElse(new ArrayList<>()),
                atrEntry.removedIds().orElse(new ArrayList<>()),
                atrEntry.insertedIds().orElse(new ArrayList<>()),
                Duration.ZERO,
                atrEntry.forwardCompatibility(),
                atrEntry.ageMillis(),
                atrEntry.durabilityLevel());
    }

    public long createdAt() {
        return createdAt;
    }

    public String attemptId() {
        return attemptId;
    }

    public String atrId() {
        return atrId;
    }

    public CollectionIdentifier atrCollection() {
        return atrCollection;
    }

    public List<DocRecord> stagedReplaces() {
        return stagedReplaces;
    }

    public List<DocRecord> stagedRemoves() {
        return stagedRemoves;
    }

    public List<DocRecord> stagedInserts() {
        return stagedInserts;
    }

    public AttemptState state() {
        return state;
    }

    public Optional<ForwardCompatibility> forwardCompatibility() {
        return forwardCompatibility;
    }

    public Optional<DurabilityLevel> durabilityLevel() {
        return durabilityLevel;
    }

    public long ageMillis() {
        return ageMillis;
    }

    // Will start when current time == startTime
    @Override
    public long getDelay(TimeUnit unit) {
        Duration diff = startTime.minus(Duration.ofNanos(System.nanoTime()));
        return diff.toNanos();
    }

    @Override
    public int compareTo(Delayed o) {
        if (o instanceof CleanupRequest) {
            return Long.compare(startTime.toNanos(), ((CleanupRequest) o).startTime.toNanos());
        }
        throw new IllegalStateException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CleanupRequest(");
        sb.append("atrId=");
        sb.append(atrId);
        sb.append(",attemptId=");
        sb.append(attemptId);
        sb.append(",state=");
        sb.append(state);
        sb.append(",inserts=");
        sb.append(stagedInserts);
        sb.append(",replaces=");
        sb.append(stagedReplaces);
        sb.append(",removes=");
        sb.append(stagedRemoves);
        sb.append(",ageMillis=");
        sb.append(ageMillis);
        sb.append(",fc=");
        sb.append(forwardCompatibility);
        sb.append(",durability=");
        sb.append(durabilityLevel);
        sb.append(")");
        return sb.toString();
    }
}
