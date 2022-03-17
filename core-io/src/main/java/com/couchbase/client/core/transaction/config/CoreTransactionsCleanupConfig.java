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

package com.couchbase.client.core.transaction.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Stability.Internal
public class CoreTransactionsCleanupConfig {
    public static final Duration DEFAULT_TRANSACTION_CLEANUP_WINDOW = Duration.ofSeconds(60);

    private final boolean runLostAttemptsCleanupThread;
    private final boolean runRegularAttemptsCleanupThread;
    private final Duration cleanupWindow;
    private final Set<CollectionIdentifier> cleanupSet;

    public CoreTransactionsCleanupConfig(boolean runLostAttemptsCleanupThread,
                                         boolean runRegularAttemptsCleanupThread,
                                         Duration cleanupWindow,
                                         Set<CollectionIdentifier> cleanupSet) {
        this.runLostAttemptsCleanupThread = runLostAttemptsCleanupThread;
        this.runRegularAttemptsCleanupThread = runRegularAttemptsCleanupThread;
        this.cleanupWindow = Objects.requireNonNull(cleanupWindow);
        this.cleanupSet = new HashSet<>(Objects.requireNonNull(cleanupSet));
    }

    @Stability.Internal
    public static CoreTransactionsCleanupConfig createDefault() {
        return new CoreTransactionsCleanupConfig(
                true,
                true,
                DEFAULT_TRANSACTION_CLEANUP_WINDOW,
                new HashSet<>());
    }

    public static CoreTransactionsCleanupConfig createForSingleQueryTransactions() {
        return new CoreTransactionsCleanupConfig(false,
                false,
                DEFAULT_TRANSACTION_CLEANUP_WINDOW,
                new HashSet<>());
    }

    public boolean runLostAttemptsCleanupThread() {
        return runLostAttemptsCleanupThread;
    }

    public boolean runRegularAttemptsCleanupThread() {
        return runRegularAttemptsCleanupThread;
    }

    public Duration cleanupWindow() {
        return cleanupWindow;
    }

    public Set<CollectionIdentifier> cleanupSet() {
        return cleanupSet;
    }

    Map<String, Object> exportAsMap() {
        Map<String, Object> export = new LinkedHashMap<>();
        export.put("runLostAttemptsCleanupThread", runLostAttemptsCleanupThread);
        export.put("runRegularAttemptsCleanupThread", runRegularAttemptsCleanupThread);
        export.put("cleanupWindowMs", cleanupWindow.toMillis());
        export.put("cleanupSet", cleanupSet.stream().map(CollectionIdentifier::toString).collect(Collectors.joining(",")));
        return export;
    }
}

