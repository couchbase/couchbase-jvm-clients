/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.cleanup;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple data class, so all fields are public.
 *
 * The stats for a particular ATR.
 */
@Stability.Internal
public class ActiveTransactionRecordStats {
    // How many entries the ATR contained.
    public int numEntries;

    // Whether the ATR existed.
    public boolean exists;

    // Any error that caused handling the ATR to fail.  ATR-not-found is not counted.
    public Optional<Throwable> errored = Optional.empty();

    // Any expired entries
    public Collection<ActiveTransactionRecordEntry> expired = Collections.EMPTY_LIST;

    public AtomicInteger expiredEntryCleanupTotalAttempts = new AtomicInteger(0);

    public AtomicInteger expiredEntryCleanupFailedAttempts = new AtomicInteger(0);

    // The oldest expired entry
    public Optional<ActiveTransactionRecordEntry> oldest() {
        return expired.stream()
                .sorted((f1, f2) -> Long.compare(f2.ageMillis(), f1.ageMillis()))
                .findFirst();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (exists) {
            sb.append("stats={entries=");
            sb.append(numEntries);
            errored.ifPresent(v -> {
                sb.append("error=");
                sb.append(v.getMessage());
            });
            sb.append(",expired=");
            sb.append(expired.size());
            sb.append('}');
        } else {
            sb.append("exists=false");
        }
        return sb.toString();
    }
}
