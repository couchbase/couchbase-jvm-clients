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
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordUtil;
import com.couchbase.client.core.transaction.components.CasMode;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.error.transaction.internal.ThreadStopRequestedException;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.transaction.log.SimpleEventBusLogger;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupEndRunEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupStartRunEvent;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.MonoBridge;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

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
class AccessErrorException extends CouchbaseException {}

// The collection was deleted and should be removed from the cleanup set
@Stability.Internal
class CollectionDeletedException extends CouchbaseException {}

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
    private final Core core;
    private final ClientRecord clientRecord;
    private final SimpleEventBusLogger LOGGER;
    private final CoreTransactionsConfig config;
    /**
     * This abstraction is purely to enable testing to inject errors during cleanup.
     */
    private final Supplier<TransactionsCleaner> cleanerSupplier;
    private volatile boolean stop = false;
    private Disposable cleanupThreadLauncher;
    private final Duration actualCleanupWindow;
    private final String clientUuid = UUID.randomUUID().toString();
    private final String bp;
    // The collections we want to cleanup.
    private final Set<CollectionIdentifier> cleanupSet = ConcurrentHashMap.newKeySet();
    // The collections we have cleanup threads for.
    private final Map<CollectionIdentifier, Disposable> actuallyBeingCleaned = new ConcurrentHashMap<>();

    // A client can have both the lost and regular cleanup threads running.  Try to avoid them stepping on each
    // other's toes by making the lost thread require a little extra safety time before regarding an attempt as
    // ready to cleanup
    private static final Duration DEFAULT_SAFETY_MARGIN = Duration.ofMillis(1500);

    public LostCleanupDistributed(Core core,
                                  CoreTransactionsConfig config,
                                  Supplier<TransactionsCleaner> cleanerSupplier) {
        this.core = Objects.requireNonNull(core);
        this.LOGGER = new SimpleEventBusLogger(core.context().environment().eventBus(), CoreTransactionsCleanup.LOST_CATEGORY);
        this.clientRecord = config.clientRecordFactory().create(core);
        this.config = Objects.requireNonNull(config);
        this.cleanerSupplier = Objects.requireNonNull(cleanerSupplier);
        this.actualCleanupWindow = config.cleanupConfig().cleanupWindow();
        bp = String.format("Client %s", clientUuid.substring(0, TransactionLogEvent.CHARS_TO_LOG));
        start();
    }

    public void addToCleanupSet(CollectionIdentifier coll) {
        cleanupSet.add(coll);
    }

    public Set<CollectionIdentifier> cleanupSet() {
        return new HashSet<>(cleanupSet);
    }

    public Mono<Void> shutdown(Duration timeout) {
        return Mono.fromCallable(() -> {
                    synchronized (actuallyBeingCleaned) {
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

                            // LOGGER.verbose("Waiting for zero cleanup threads, currently {}", actuallyBeingCleaned.size());

                            if (actuallyBeingCleaned.isEmpty()) {
                                break;
                            }

                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        // JVMCBC-1110: all threads are safely finished and won't be adding themselves to the client record.
                        // Can now leave synchronized and remove everything from client record.
                        return removeFromClientRecords;
                    }
                }).flatMap(removeFromClientRecords -> clientRecord.removeClientFromClientRecord(clientUuid, removeFromClientRecords).then()

                        // TXNJ-96
                        .onErrorResume(err -> {
                            LOGGER.warn(String.format("%s failed to remove from cleanup set with err: %s", bp, err));
                            return Mono.empty();
                        }))

                .doOnTerminate(() -> LOGGER.info(String.format("%s stopped lost cleanup process and removed client from client records", bp)));
    }

    private static List<String> atrsToHandle(int indexOfThisClient, int numActiveClients, int numAtrs) {
        List<String> allAtrs = ActiveTransactionRecordIds.allAtrs(numAtrs);
        ArrayList<String> out = new ArrayList<>();
        for (int i = indexOfThisClient; i < allAtrs.size(); i += numActiveClients) {
            out.add(allAtrs.get(i));
        }
        return out;
    }

    private RequestTracer tracer() {
        return core.context().environment().requestTracer();
    }

    /**
     * Looks at an ATR, finds any expired entries, and cleans them up.
     *
     * Called only from lost.
     */
    public Flux<TransactionCleanupAttemptEvent> handleATRCleanup(String bp,
                                                                 CollectionIdentifier atrCollection,
                                                                 String atrId,
                                                                 ActiveTransactionRecordStats stats,
                                                                 Duration safetyMargin,
                                                                 SpanWrapper pspan) {
        return Flux.defer(() -> {
            long start = System.nanoTime();
            AtomicLong timeToFetchAtr = new AtomicLong(0);
            AtomicReference<CasMode> casMode = new AtomicReference<>(CasMode.UNKNOWN);
            SpanWrapper span = SpanWrapperUtil.createOp(null, tracer(), atrCollection, atrId, "cleanup.atr", pspan);

            TransactionsCleaner cleaner = cleanerSupplier.get();

            return cleaner.hooks().beforeAtrGet.apply(atrId)

                .then(ActiveTransactionRecord.getAtr(core,
                        atrCollection,
                        atrId,
                        core.context().environment().timeoutConfig().kvTimeout(),
                        span))

                .flatMap(atr -> {
                    timeToFetchAtr.set(System.nanoTime());

                    if (atr.isPresent()) {
                        casMode.set(atr.get().casMode());
                        return Mono.just(atr.get());
                    }
                        // Just ignore ATRs we can't find
                    else return Mono.empty();
                })

                .doOnError(err -> {
                    LOGGER.debug(String.format("%s Got error '%s' while getting ATR %s/",
                        bp, err, ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId)));
                    stats.errored = Optional.of(err);
                })

                .flatMapMany(atr -> {
                    stats.numEntries = atr.entries().size();
                    stats.exists = true;
                    stats.errored = Optional.empty();

                    Collection<ActiveTransactionRecordEntry> expired = atr.entries().stream()
                        .filter(v -> v.hasExpired(safetyMargin.toMillis()))
                        .collect(Collectors.toList());

                    stats.expired = expired;

                    span.attribute("db.couchbase.transactions.cleanup.atr.num_entries", stats.numEntries);
                    span.attribute("db.couchbase.transactions.cleanup.atr.num_expired", stats.expired);

                    return Flux.fromIterable(expired)
                            .publishOn(core.context().environment().transactionsSchedulers().schedulerCleanup());
                })

                .concatMap(atrEntry -> {
                    LOGGER.verbose(String.format("%s Found expired attempt %s, expires after %d, age %d (started %d, now %d)",
                        bp, atrEntry.attemptId(), atrEntry.expiresAfterMillis().orElse(-1), atrEntry.ageMillis(),
                        atrEntry.timestampStartMillis().orElse(0l),
                        atrEntry.cas() / 1000000));

                    stats.expiredEntryCleanupTotalAttempts.incrementAndGet();

                    CleanupRequest req = CleanupRequest.fromAtrEntry(atrCollection, atrEntry);

                    return cleaner.performCleanup(req, false, span)

                        .onErrorResume(err -> {
                            // Swallow errors.  It will be retried again on subsequent runs.
                            stats.expiredEntryCleanupFailedAttempts.incrementAndGet();
                            return Mono.empty();
                        });
                })

                .doOnTerminate(() -> {
                    long now = System.nanoTime();
                    LOGGER.verbose(String.format("%s processed ATR %s after %sµs (%d fetching ATR), CAS=%s: %s", bp,
                        ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId),
                        TimeUnit.NANOSECONDS.toMicros(now - start), TimeUnit.NANOSECONDS.toMicros(timeToFetchAtr.get() - start), casMode.get(), stats));
                    span.finish();
                })

                    .onErrorResume(err -> {
                        // Any errors from the main cleanupATREntry are already swallowed, so the only errors
                        // reaching here are transient ones from fetching the ATR, and will already be reflected in
                        // `stats.errored`.  We don't want transient problems with one ATR to affect the others, so
                        // just continue.
                        return Mono.empty();
                    });
        });
    }

    private void start() {
        periodicallyCheckCleanupSet();
    }

    Mono<Void> createThreadForCollectionIfNeeded(CollectionIdentifier coll) {
        return Mono.defer(() -> {
            synchronized (actuallyBeingCleaned) {
                if (stop) {
                    return Mono.empty();
                }

                if (!actuallyBeingCleaned.containsKey(coll)) {
                    String collDebug = redactMeta(coll.bucket() + "." + coll.scope().orElse("-") + "." + coll.collection().orElse("-")).toString();
                    LOGGER.info("{} will start cleaning lost transactions on collection {}", bp, collDebug);

                    // Spawn as a background thread, since perCollectionThread is blocking
                    Disposable thread = perCollectionThread(coll)

                            .onErrorResume(err -> {
                                // If ThreadStopRequestedException then don't log here, as already logged in checkIfThreadStopped
                                if (!(err instanceof ThreadStopRequestedException)) {
                                    LOGGER.warn("{} {} lost transactions thread has ended on error {} (will be retried)", bp, collDebug, DebugUtil.dbg(err));
                                    return Mono.empty();
                                }

                                return Mono.empty();
                            })

                            .doOnTerminate(() -> {
                                LOGGER.debug("{} {} lost transactions thread has ended", bp, collDebug);

                                // If we're not shutting down, periodicallyCheckCleanupSet will soon add it back again.
                                // This is why we don't also remove from the client record here.
                                // This part is intentionally not synchronized, since it's most likely to be
                                // happening during `shutdown`, which is fully synchronized.  It's fine for elements
                                // to be removed from `actuallyBeingCleaned` - just don't want them added, as then
                                // we could not remove them from the client record in some race situations (which is
                                // harmless, but trips up FIT).
                                actuallyBeingCleaned.remove(coll);
                            })

                            .subscribe();

                    actuallyBeingCleaned.put(coll, thread);
                }
            }

            return Mono.empty();
        });
    }

    private void periodicallyCheckCleanupSet() {
        cleanupThreadLauncher = Flux.interval(Duration.ZERO, Duration.ofSeconds(1), core.context().environment().transactionsSchedulers().schedulerCleanup())
                .concatMap(v -> Flux.fromIterable(cleanupSet))
                .publishOn(core.context().environment().transactionsSchedulers().schedulerCleanup())
                .concatMap(this::createThreadForCollectionIfNeeded)
                .doOnCancel(() -> {
                    LOGGER.info(String.format("%s has been told to cancel", bp));
                })
                .subscribe(v -> LOGGER.warn(String.format("%s lost transactions cleanup thread(s) ending", bp)),
                        (err) -> {
                            if (err instanceof ThreadStopRequestedException) {
                                LOGGER.info(String.format("%s lost transactions cleanup told to stop", bp));
                            } else {
                                LOGGER.warn(String.format("%s lost transactions cleanup ended with exception " + err, bp));
                            }
                        });
    }

    private Mono<Void> perCollectionThread(CollectionIdentifier collection) {
        return Mono.defer(() -> {

        String bp = "lost/" + redactMeta(collection.bucket() + "." + collection.scope().orElse("-") + "." + collection.collection().orElse("-")) + "/clientId=" + clientUuid.substring(0, TransactionLogEvent.CHARS_TO_LOG);

        AtomicReference<SpanWrapper> span = new AtomicReference<>();

        core.openBucket(collection.bucket());

        // Every X seconds (X = config.cleanupWindow), start off by reading & updating the client record
        // processClient can propagate errors
        return Mono.fromRunnable(() ->
                        span.set(SpanWrapperUtil.createOp(null, tracer(), collection, null, "cleanup.run", null)
                                .attribute("db.couchbase.transactions.cleanup.client_uuid", clientUuid)))

                .publishOn(core.context().environment().transactionsSchedulers().schedulerCleanup())

                .then(clientRecord.processClient(clientUuid, collection, config, span.get()))

                .flatMap(clientDetails -> {
                    long startOfRun = System.nanoTime();
                    List<String> atrsHandledByThisClient;
                    Map<String, ActiveTransactionRecordStats> atrStats = new HashMap<>();

                    // Now we have the client record, work out how to divvy up the work for this client
                    atrsHandledByThisClient = atrsToHandle(clientDetails.indexOfThisClient(),
                            clientDetails.numActiveClients(),
                            config.numAtrs());

                    long checkAtrEveryNNanos = Math.max(1, actualCleanupWindow.toNanos() / atrsHandledByThisClient.size());

                    if (atrsHandledByThisClient.size() < config.numAtrs()) {
                        atrsHandledByThisClient.forEach(id -> {
                            // Enable this only when explicitly testing LostTxnsCleanupCrucibleTest
                            // LOGGER.verbose(String.format("%s owns ATR %s (of %d) and will check it over next %dmillis, checking an ATR every %dmillis",
                            //        bp, id, config.numAtrs(), actualCleanupWindow.toMillis(), checkAtrEveryNMillis));
                        });
                    }
                    else {
                        LOGGER.verbose(String.format("%s owns all %d ATRs and will check them over next %dmills, checking an ATR every %dnanos", bp,
                                config.numAtrs(), actualCleanupWindow.toMillis(), checkAtrEveryNNanos));
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
                    return Flux.zip(Flux.fromIterable(atrsHandledByThisClient),
                                    Flux.interval(Duration.ofNanos(checkAtrEveryNNanos)))
                            .publishOn(core.context().environment().transactionsSchedulers().schedulerCleanup())

                        // Where the ATR cleanup magic happens
                        // TXNJ-402: Use flatMap rather than concatMap to partially avoid issues with dropped ticks
                        .flatMap(v -> {
                            String atrId = v.getT1();

                            LOGGER.verbose(String.format("%s checking for lost txns in atr %s", bp,
                                ActiveTransactionRecordUtil.getAtrDebug(collection, atrId)));

                            ActiveTransactionRecordStats stats = new ActiveTransactionRecordStats();

                            // Perform this checkIfThreadStopped inside the concatMap - see TXNJ-170.
                            Mono<String> out = checkIfThreadStopped(collection)

                                    // handleATRCleanup does not propagate errors
                                    .thenMany(handleATRCleanup(bp, collection, atrId, stats, DEFAULT_SAFETY_MARGIN, span.get()))

                                    .then(Mono.fromRunnable(() -> atrStats.put(atrId, stats)))

                                    .thenReturn(atrId);

                            MonoBridge<String> mb = new MonoBridge<>(out, "", this, null);

                            return mb.external();
                        })

                        // Ignore individual ATR errors, press on
                        .onErrorResume(err -> {
                            if (err instanceof TimeoutException) {
                                Set<RetryReason> compare = new HashSet<>();
                                compare.add(RetryReason.KV_COLLECTION_OUTDATED);
                                boolean collectionDeleted = ((TimeoutException) err).context().requestContext().retryReasons().equals(compare);

                                if (collectionDeleted) {
                                    cleanupSet.remove(collection);
                                    LOGGER.info(String.format("%s stopping cleanup on collection %s as it seems to be deleted", bp, collection));
                                    // This will end this thread and remove from actuallyBeingCleaned
                                    return Mono.error(err);
                                }
                            }

                            // See this happening in tests when the bucket is closed.  Raise error to get past the .repeat().
                            if (err instanceof ThreadStopRequestedException) {
                                return Mono.error(err);
                            } else {
                                LOGGER.info(String.format("%s lost cleanup thread got error '%s', continuing", bp, err));

                                return Mono.empty();
                            }
                        })

                        .then().thenReturn(Tuples.of(atrStats, ev, startOfRun));

                })

                .doOnNext(stats -> {
                    Duration timeForRun = Duration.ofNanos(System.nanoTime() - stats.getT3());

                    TransactionCleanupEndRunEvent ev = new TransactionCleanupEndRunEvent(
                            stats.getT2(),
                            stats.getT1(),
                            timeForRun);

                    core.context().environment().eventBus().publish(ev);
                })

                // TXNJ-90 - make sure the thread does not drop out
                // Update: With TXNJ-301 this is less mission-critical, as the thread will be restarted after a period
                // Note that problems with cleanup itself, or fetching ATRs, will not propagate here, so we're only handling
                // rare errors like transiently failing to write the client record.  Let's backoff for cleanupWindow.
                .retryWhen(Retry.allBut(ThreadStopRequestedException.class, AccessErrorException.class)
                        .exponentialBackoff(Duration.ofMillis(Math.min(1000, config.cleanupConfig().cleanupWindow().toMillis())),
                                config.cleanupConfig().cleanupWindow())
                        .doOnRetry(v -> {
                            LOGGER.debug(String.format("%s retrying lost cleanup on error %s after %s",
                                    bp, DebugUtil.dbg(v.exception()), v.backoff()));
                        })
                        .toReactorRetry())

                .doOnNext(v -> span.get().finish())
                .doOnError(err -> span.get().failWith(err))

                // Start all over again with the ClientRecord check
                .repeat()

                .then();
        });
    }

    private Mono<Void> checkIfThreadStopped(CollectionIdentifier collection) {
        return Mono.defer(() -> {
            if (stop) {
                LOGGER.info(String.format("%s Stopping background cleanup thread for lost transactions on %s", bp, collection));
                return Mono.error(new ThreadStopRequestedException());
            } else {
                return Mono.empty();
            }
        });
    }
}