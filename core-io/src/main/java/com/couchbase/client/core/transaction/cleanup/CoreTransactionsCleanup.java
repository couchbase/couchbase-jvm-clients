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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.events.transaction.CleanupFailedEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.error.transaction.internal.ThreadStopRequestedException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.util.BlockingRetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.cnc.events.transaction.TransactionEvent.DEFAULT_CATEGORY;

/**
 * Owns cleanup threads.
 *
 * @author Graham Pople
 */
@Stability.Internal
public class CoreTransactionsCleanup {
    public static final String CATEGORY = DEFAULT_CATEGORY + ".cleanup";
    public static final String CATEGORY_STATS = DEFAULT_CATEGORY + ".cleanup.stats";
    public static final String CATEGORY_CLIENT_RECORD = DEFAULT_CATEGORY + ".clientrecord";
    public static final String LOST_CATEGORY = DEFAULT_CATEGORY + ".cleanup.lost";
    public static final String REGULAR_CATEGORY = DEFAULT_CATEGORY + ".cleanup.regular";

    private static final Logger LOGGER = LoggerFactory.getLogger(CATEGORY);
    private static final Logger LOGGER_REGULAR = LoggerFactory.getLogger(REGULAR_CATEGORY);

    private final Core core;
    private final CoreTransactionsConfig config;
    private final DelayQueue<CleanupRequest> cleanupQueue = new DelayQueue<>();
    private volatile boolean stop = false;
    private final CountDownLatch stopLatch;
    private @Nullable final LostCleanupDistributed lostCleanup;
    private final CleanerFactory cleanerFactory;
    private Thread regularCleanupThread;

    public CoreTransactionsCleanup(Core core, CoreTransactionsConfig config) {
        this.core = Objects.requireNonNull(core);
        this.config = Objects.requireNonNull(config);
        this.lostCleanup = config.cleanupConfig().runLostAttemptsCleanupThread() ? new LostCleanupDistributed(core, config, this::getCleaner) : null;
        int countdown = config.cleanupConfig().runRegularAttemptsCleanupThread() ? 1 : 0;
        stopLatch = new CountDownLatch(countdown);
        cleanerFactory = config.cleanerFactory();

        if (config.cleanupConfig().runRegularAttemptsCleanupThread()) {
            runRegularAttemptsCleanupThread();
        }

        config.metadataCollection().ifPresent(mc -> {
            // JVMCBC-1084: this can potentially open buckets that the user has also opened.
            core.openBucket(mc.bucket());
            addToCleanupSet(mc);
        });
        config.cleanupConfig().cleanupSet().forEach(coll -> {
            core.openBucket(coll.bucket());
            addToCleanupSet(coll);
        });
    }

    public void addToCleanupSet(CollectionIdentifier coll) {
        if (lostCleanup != null) {
            lostCleanup.addToCleanupSet(coll);
        }
    }

    public Set<CollectionIdentifier> cleanupSet() {
        return lostCleanup != null ? lostCleanup.cleanupSet() : new HashSet<>();
    }

    void stopBackgroundProcesses(Duration timeout) {
        stop = true;
        if (regularCleanupThread != null) {
            regularCleanupThread.interrupt();
        }

        LOGGER.info("Waiting for {} regular background threads to exit", stopLatch.getCount());
        try {
            if (!stopLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                LOGGER.info("Background threads did not stop in expected time {}", timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for background threads {}", e.toString());
        }

        if (lostCleanup != null) {
            lostCleanup.shutdown(timeout);
        }

        LOGGER.info("Background threads have exitted");
    }

    private void runRegularAttemptsCleanupThread() {
        Objects.requireNonNull(LOGGER);

        LOGGER_REGULAR.debug("Starting background cleanup thread to find transactions from this client");
        regularCleanupThread = new Thread(() -> {
            try {
                while (!stop) {
                    List<CleanupRequest> requests = new ArrayList<>();
                    // Periodically check and drain the cleanupQueue
                    cleanupQueue.drainTo(requests);

                    for (CleanupRequest req : requests) {
                        TransactionsCleaner cleaner = getCleaner();
                        try {
                            TransactionCleanupAttemptEvent result = cleaner.performCleanup(req, true, null);
                            LOGGER_REGULAR.debug("result of cleanup request {}: success={}", req, result.success());
                        } catch (Throwable err) {
                            CleanupFailedEvent ev = new CleanupFailedEvent(req, err);
                            core.context().environment().eventBus().publish(ev);
                            // [REGULAR-CLEANUP-FAILURES] - retry it later
                            LOGGER_REGULAR.debug("error while handling cleanup request {}, leaving for lost cleanup: '{}'",
                                    req, err);
                            if (err instanceof ThreadStopRequestedException) {
                                stop = true;
                            }
                        }
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        if (!stop) {
                            LOGGER_REGULAR.warn("Unexpected: got InterruptedException but stop is not set");
                        }
                    }
                }
            } finally {
                stopLatch.countDown();
            }
        }, "cbtxn-reg-cleanup");

        regularCleanupThread.setDaemon(true);
        regularCleanupThread.start();
    }

    public TransactionsCleaner getCleaner() {
        return cleanerFactory.create(core, config.supported());
    }

    public Optional<Integer> cleanupQueueLength() {
        if (config.cleanupConfig().runRegularAttemptsCleanupThread()) {
            return Optional.of(cleanupQueue.size());
        }
        else {
            return Optional.empty();
        }
    }

    public void add(CleanupRequest cleanupRequest) {
        cleanupQueue.add(cleanupRequest);
    }

    public void shutdown(Duration timeout) {
        stopBackgroundProcesses(timeout);
        // Note we don't shutdown the schedulers here - those are part of the CoreEnvironment, which may be
        // shared by multiple Clusters.
    }
}
