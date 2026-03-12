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
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupEndRunEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupStartRunEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.cnc.tracing.RequestTracerAndDecorator;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.transaction.internal.ThreadStopRequestedException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecords;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordUtil;
import com.couchbase.client.core.transaction.components.CasMode;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.BlockingRetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

// Indication that the thread has failed due to a access error and will silently stop
@Stability.Internal
class AccessErrorException extends CouchbaseException { }

// The collection was deleted and should be removed from the cleanup set
@Stability.Internal
class CollectionDeletedException extends CouchbaseException { }

/**
 * Runs the algorithm to find 'lost' transactions, distributing the work between clients.
 * <p>
 * The user specifies that lost transactions should be found inside a certain cleanup window of X seconds.
 * <p>
 * As this algo is distributed and tries to require minimal co-operation between clients (they simply read and write a
 * Client Record doc periodically, there are flaws including:
 * <p>
 * Two clients can read the same client record at the same time and miss each other's updates
 * A client can read the record just before a new client removes itself
 * <p>
 * The idea is that these conflicts will sort themselves out on the next iteration, and that in the vast majority of cases
 * each ATR will be checked in X seconds.
 *
 * @author Graham Pople
 */
@Stability.Internal
public class LostCleanupDistributed {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreTransactionsCleanup.LOST_CATEGORY);

    private final Core core;
    private final ClientRecord clientRecord;
    private final CoreTransactionsConfig config;
    /**
     * This abstraction is purely to enable testing to inject errors during cleanup.
     */
    private final Supplier<TransactionsCleaner> cleanerSupplier;
    private volatile boolean stop = false;
    private final ScheduledExecutorService cleanupThreadLauncher = Executors.newSingleThreadScheduledExecutor();
    private final Duration actualCleanupWindow;
    private static final Duration BACKOFF_START = Duration.ofMillis(10);
    private final String clientUuid = UUID.randomUUID().toString();
    private final String bp;
    // The collections we want to cleanup.
    private final Set<CollectionIdentifier> cleanupSet = ConcurrentHashMap.newKeySet();
    // The collections we have cleanup threads for.
    private final Map<CollectionIdentifier, Thread> actuallyBeingCleaned = new ConcurrentHashMap<>();

    // A client can have both the lost and regular cleanup threads running.  Try to avoid them stepping on each
    // other's toes by making the lost thread require a little extra safety time before regarding an attempt as
    // ready to cleanup
    private static final Duration DEFAULT_SAFETY_MARGIN = Duration.ofMillis(1500);

    public LostCleanupDistributed(Core core,
                                  CoreTransactionsConfig config,
                                  Supplier<TransactionsCleaner> cleanerSupplier) {
        this.core = Objects.requireNonNull(core);
        this.clientRecord = config.clientRecordFactory().create(core);
        this.config = Objects.requireNonNull(config);
        this.cleanerSupplier = Objects.requireNonNull(cleanerSupplier);
        this.actualCleanupWindow = config.cleanupConfig().cleanupWindow();
        bp = "Client " + clientUuid.substring(0, TransactionLogEvent.CHARS_TO_LOG);
        start();
    }

    public void addToCleanupSet(CollectionIdentifier coll) {
        cleanupSet.add(coll);
    }

    public Set<CollectionIdentifier> cleanupSet() {
        return new HashSet<>(cleanupSet);
    }

    public void shutdown(Duration timeout) {
        synchronized (actuallyBeingCleaned) {
            try {
                // Prevent in-flight threads from adding themselves.
                // It's tempting to dispose of cleanupThreadLauncher here, but that seems to cause unpredictable
                // results with threads that were ultimately spawned by it then disappearing.
                Set<CollectionIdentifier> removeFromClientRecords = new HashSet<>(actuallyBeingCleaned.keySet());
                this.stop = true;

                // Now we're here (stop=true, synchronized), it's not possible for anything to be added to actuallyBeingCleaned.
                // Things can still be added to `cleanupSet`, but that's safe.

                LOGGER.info("{} stopping lost cleanup process, {} threads running", bp, actuallyBeingCleaned.keySet().size());

                // Wait for all threads to finish.
                // Don't call dispose() on them.  That appears to just end them, so checkIfThreadStopped() doesn't happen
                // and they don't remove themselves from actuallyBeingCleaned.
                long start = System.nanoTime();

                while (true) {
                    if (Duration.ofNanos(System.nanoTime() - start).compareTo(timeout) > 0) {
                        LOGGER.warn("Exceeded timeout of {}ms while waiting for transactions cleanup thread to finish", timeout.toMillis());
                        break;
                    }

                    if (actuallyBeingCleaned.isEmpty()) {
                        break;
                    }

                    for (Thread t : new ArrayList<>(actuallyBeingCleaned.values())) {
                        try {
                            t.join(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                }

                cleanupThreadLauncher.shutdownNow();

                // JVMCBC-1110: all threads are safely finished and won't be adding themselves to the client record.
                // Can now leave synchronized and remove everything from client record.
                clientRecord.removeClientFromClientRecord(clientUuid, removeFromClientRecords);

                // TXNJ-96
            } catch (Throwable err) {
                LOGGER.warn("{} failed to remove from cleanup set with err: {}", bp, err);
            }
        }

        LOGGER.info("{} stopped lost cleanup process and removed client from client records", bp);
    }

    private static List<String> atrsToHandle(int indexOfThisClient, int numActiveClients, int numAtrs) {
        List<String> allAtrs = ActiveTransactionRecordIds.allAtrs(numAtrs);
        ArrayList<String> out = new ArrayList<>();
        for (int i = indexOfThisClient; i < allAtrs.size(); i += numActiveClients) {
            out.add(allAtrs.get(i));
        }
        return out;
    }

    private RequestTracerAndDecorator tracer() {
        return core.context().coreResources().requestTracerAndDecorator();
    }

    private TracingDecorator tip() {
        return core.context().coreResources().tracingDecorator();
    }

    /**
     * Looks at an ATR, finds any expired entries, and cleans them up.
     *
     * Called only from lost.
     */
    public void handleATRCleanup(String bp,
                                 CollectionIdentifier atrCollection,
                                 String atrId,
                                 ActiveTransactionRecordStats stats,
                                 Duration safetyMargin,
                                 SpanWrapper pspan) {
        long start = System.nanoTime();
        AtomicLong timeToFetchAtr = new AtomicLong(0);
        AtomicReference<CasMode> casMode = new AtomicReference<>(CasMode.UNKNOWN);
        SpanWrapper span = SpanWrapperUtil.createOp(null, tracer(), atrCollection, atrId, TracingIdentifiers.TRANSACTION_CLEANUP_ATR, pspan);

        TransactionsCleaner cleaner = cleanerSupplier.get();

        try {
            cleaner.hooks().beforeAtrGet.apply(atrId);

            Optional<ActiveTransactionRecords> atr = ActiveTransactionRecord.getAtr(core,
                    atrCollection,
                    atrId,
                    core.context().environment().timeoutConfig().kvTimeout(),
                    span);

            timeToFetchAtr.set(System.nanoTime());

            if (!atr.isPresent()) {
                // Just ignore ATRs we can't find
                return;
            }

            casMode.set(atr.get().casMode());
            stats.numEntries = atr.get().entries().size();
            stats.exists = true;
            stats.errored = Optional.empty();

            Collection<ActiveTransactionRecordEntry> expired = atr.get().entries().stream()
                    .filter(v -> v.hasExpired(safetyMargin.toMillis()))
                    .collect(Collectors.toList());

            stats.expired = expired;

            tip().provideAttr(TracingAttribute.TRANSACTION_ATR_ENTRIES_COUNT, span.span(), stats.numEntries);
            tip().provideAttr(TracingAttribute.TRANSACTION_ATR_ENTRIES_EXPIRED, span.span(), stats.expired.size());

            for (ActiveTransactionRecordEntry atrEntry : expired) {
                LOGGER.trace("{} Found expired attempt {}, expires after {}, age {} (started {}, now {})",
                        bp, atrEntry.attemptId(), atrEntry.expiresAfterMillis().orElse(-1), atrEntry.ageMillis(),
                        atrEntry.timestampStartMillis().orElse(0L),
                        atrEntry.cas() / 1000000);

                stats.expiredEntryCleanupTotalAttempts.incrementAndGet();

                CleanupRequest req = CleanupRequest.fromAtrEntry(atrCollection, atrEntry);

                try {
                    cleaner.performCleanup(req, false, span);
                } catch (Throwable err) {
                    // Swallow errors.  It will be retried again on subsequent runs.
                    stats.expiredEntryCleanupFailedAttempts.incrementAndGet();
                }
            }

        } catch (Throwable err) {
            // Any errors from the main cleanupATREntry are already swallowed, so the only errors
            // reaching here are transient ones from fetching the ATR, and will already be reflected in
            // `stats.errored`.  We don't want transient problems with one ATR to affect the others, so
            // just continue.
            LOGGER.debug("{} Got error '{}' while getting ATR {}/",
                    bp, err, ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId));
            stats.errored = Optional.of(err);
            span.recordException(err);
        } finally {
            if (LOGGER.isTraceEnabled()) {
                long now = System.nanoTime();
                LOGGER.trace("{} processed ATR {} after {}µs ({} fetching ATR), CAS={}: {}", bp,
                        ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId),
                        TimeUnit.NANOSECONDS.toMicros(now - start), TimeUnit.NANOSECONDS.toMicros(timeToFetchAtr.get() - start), casMode.get(), stats);
            }

            span.finish();
        }
    }

    private void start() {
        periodicallyCheckCleanupSet();
    }

    void createThreadForCollectionIfNeeded(CollectionIdentifier coll) {
        synchronized (actuallyBeingCleaned) {
            if (stop) {
                return;
            }

            if (!actuallyBeingCleaned.containsKey(coll)) {
                String collDebug = redactMeta(coll.bucket() + "." + coll.scope().orElse("-") + "." + coll.collection().orElse("-")).toString();
                LOGGER.info("{} will start cleaning lost transactions on collection {}", bp, collDebug);

                // Spawn as a background thread, since perCollectionThread is blocking
                Thread thread = new Thread(() -> {
                    try {
                        perCollectionThread(coll);
                    } catch (ThreadStopRequestedException err) {
                        // If ThreadStopRequestedException then don't log here, as already logged in checkIfThreadStopped
                    } catch (Throwable err) {
                        LOGGER.warn("{} {} lost transactions thread has ended on error {} (will be retried)", bp, collDebug, DebugUtil.dbg(err));
                    } finally {
                        LOGGER.debug("{} {} lost transactions thread has ended", bp, collDebug);

                        // If we're not shutting down, periodicallyCheckCleanupSet will soon add it back again.
                        // This is why we don't also remove from the client record here.
                        // This part is intentionally not synchronized, since it's most likely to be
                        // happening during `shutdown`, which is fully synchronized.  It's fine for elements
                        // to be removed from `actuallyBeingCleaned` - just don't want them added, as then
                        // we could not remove them from the client record in some race situations (which is
                        // harmless, but trips up FIT).
                        actuallyBeingCleaned.remove(coll);
                    }
                });

                thread.setDaemon(true);
                actuallyBeingCleaned.put(coll, thread);
                thread.start();
            }
        }
    }

    private void periodicallyCheckCleanupSet() {
        cleanupThreadLauncher.scheduleWithFixedDelay(() -> {
            try {
                if (stop) {
                    return;
                }
                for (CollectionIdentifier coll : new HashSet<>(cleanupSet)) {
                    createThreadForCollectionIfNeeded(coll);
                }
            } catch (Throwable err) {
                LOGGER.warn("{} lost transactions cleanup ended with exception {}", bp, err);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void perCollectionThread(CollectionIdentifier collection) {

        String bp = "lost/" + redactMeta(collection.bucket() + "." + collection.scope().orElse("-") + "." + collection.collection().orElse("-")) + "/clientId=" + clientUuid.substring(0, TransactionLogEvent.CHARS_TO_LOG);

        AtomicReference<SpanWrapper> span = new AtomicReference<>();

        core.openBucket(collection.bucket());

        BlockingRetryHandler retry = BlockingRetryHandler.builder(BACKOFF_START, actualCleanupWindow).build();
        do {
            retry.shouldRetry(true);
            checkIfThreadStopped(collection);

            // Every X seconds (X = config.cleanupWindow), start off by reading & updating the client record
            // processClient can propagate errors
            SpanWrapper created = SpanWrapperUtil.createOp(null, tracer(), collection, null, TracingIdentifiers.TRANSACTION_CLEANUP_WINDOW, null);
            tip().provideAttr(TracingAttribute.TRANSACTION_CLEANUP_CLIENT_ID, created.span(), clientUuid);
            tip().provideAttr(TracingAttribute.TRANSACTION_CLEANUP_WINDOW, created.span(), config.cleanupConfig().cleanupWindow().toMillis());
            span.set(created);

            try {
                ClientRecordDetails clientDetails = clientRecord.processClient(clientUuid, collection, config, span.get());

                long startOfRun = System.nanoTime();
                List<String> atrsHandledByThisClient;
                Map<String, ActiveTransactionRecordStats> atrStats = new HashMap<>();

                // Now we have the client record, work out how to divvy up the work for this client
                atrsHandledByThisClient = atrsToHandle(clientDetails.indexOfThisClient(),
                        clientDetails.numActiveClients(),
                        config.numAtrs());

                tip().provideAttr(TracingAttribute.TRANSACTION_CLEANUP_NUM_ATRS, span.get().span(), atrsHandledByThisClient.size());
                tip().provideAttr(TracingAttribute.TRANSACTION_CLEANUP_NUM_ACTIVE, span.get().span(), clientDetails.numActiveClients());
                tip().provideAttr(TracingAttribute.TRANSACTION_CLEANUP_NUM_EXPIRED, span.get().span(), clientDetails.numExpiredClients());

                long checkAtrEveryNNanos = Math.max(1, actualCleanupWindow.toNanos() / atrsHandledByThisClient.size());

                if (atrsHandledByThisClient.size() < config.numAtrs()) {
                    atrsHandledByThisClient.forEach(id -> {
                        // Enable this only when explicitly testing LostTxnsCleanupCrucibleTest
                        // LOGGER.trace("{} owns ATR {} (of {}) and will check it over next {}millis, checking an ATR every {}millis",
                        //        bp, id, config.numAtrs(), actualCleanupWindow.toMillis(), checkAtrEveryNMillis);
                    });
                    }
                    else {
                    LOGGER.trace("{} owns all {} ATRs and will check them over next {}mills, checking an ATR every {}nanos", bp,
                            config.numAtrs(), actualCleanupWindow.toMillis(), checkAtrEveryNNanos);
                }

                TransactionCleanupStartRunEvent ev = new TransactionCleanupStartRunEvent(collection.bucket(),
                        collection.scope().orElse(DEFAULT_SCOPE),
                        collection.collection().orElse(DEFAULT_COLLECTION),
                        clientUuid,
                        clientDetails,
                        actualCleanupWindow,
                        atrsHandledByThisClient.size(),
                        config.numAtrs(),
                        Duration.ofMillis(checkAtrEveryNNanos));

                core.context().environment().eventBus().publish(ev);

                // This client knows what ATRs it's handling over the next X seconds now
                // TXNJ-351: Rate limit the ATR polling.  If we're checking an ATR every 1 second, and
                // handling the first ATR takes 0.3 seconds, the next should execute 0.7 seconds later.
                // Similar if the first ATR takes > 1 second, the next should execute instantly.
                int atrIndex = 0;
                for (String atrId : atrsHandledByThisClient) {
                    long target = startOfRun + (checkAtrEveryNNanos * atrIndex);
                    atrIndex += 1;
                    long delay = target - System.nanoTime();
                    if (delay > 0) {
                        TimeUnit.NANOSECONDS.sleep(delay);
                    }

                    // Where the ATR cleanup magic happens
                    LOGGER.trace("{} checking for lost txns in atr {} after delay {}", bp,
                            ActiveTransactionRecordUtil.getAtrDebug(collection, atrId), delay);

                    ActiveTransactionRecordStats stats = new ActiveTransactionRecordStats();

                    // Perform this checkIfThreadStopped inside the concatMap - see TXNJ-170.
                    checkIfThreadStopped(collection);

                    try {
                        // handleATRCleanup does not propagate errors
                        handleATRCleanup(bp, collection, atrId, stats, DEFAULT_SAFETY_MARGIN, span.get());
                        atrStats.put(atrId, stats);
                    } catch (TimeoutException err) {
                        Set<RetryReason> compare = new HashSet<>();
                        compare.add(RetryReason.KV_COLLECTION_OUTDATED);
                        boolean collectionDeleted = err.context().requestContext().retryReasons().equals(compare);

                        if (collectionDeleted) {
                            cleanupSet.remove(collection);
                            LOGGER.info("{} stopping cleanup on collection {} as it seems to be deleted", bp, collection);
                            // This will end this thread and remove from actuallyBeingCleaned
                            throw err;
                        }

                        throw err;
                    } catch (ThreadStopRequestedException err) {
                        throw err;
                    } catch (Throwable err) {
                        // Ignore individual ATR errors, press on
                        LOGGER.info("{} lost cleanup thread got error '{}', continuing", bp, err);
                    }
                }

                Duration timeForRun = Duration.ofNanos(System.nanoTime() - startOfRun);

                TransactionCleanupEndRunEvent evEnd = new TransactionCleanupEndRunEvent(
                        ev,
                        atrStats,
                        timeForRun);

                core.context().environment().eventBus().publish(evEnd);

                // Comments on error handling strategy:
                // TXNJ-90 - make sure the thread does not drop out
                // Update: With TXNJ-301 this is less mission-critical, as the thread will be restarted after a period
                // Note that problems with cleanup itself, or fetching ATRs, will not propagate here, so we're only handling
                // rare errors like transiently failing to write the client record.  Let's backoff for cleanupWindow.
            } catch (AccessErrorException | ThreadStopRequestedException err) {
                span.get().recordException(err);
                retry.shouldRetry(false);
                throw err;
            } catch (Throwable err) {
                span.get().recordException(err);
                LOGGER.debug("{} retrying lost cleanup on error {} after {}",
                        bp, DebugUtil.dbg(err), retry.peekNextBackoff());
                retry.sleepForNextBackoffAndSetShouldRetry();
            } finally {
                span.get().finish();
            }

            // Start all over again with the ClientRecord check
        } while (retry.shouldRetry());
    }

    private void checkIfThreadStopped(CollectionIdentifier collection) {
        if (stop) {
            LOGGER.info("{} Stopping background cleanup thread for lost transactions on {}", bp, collection);
            throw new ThreadStopRequestedException();
        }
    }
}
