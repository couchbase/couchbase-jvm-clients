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
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordUtil;
import com.couchbase.client.core.transaction.components.DocRecord;
import com.couchbase.client.core.transaction.components.DocumentGetter;
import com.couchbase.client.core.transaction.components.DurabilityLevelUtil;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibilityStage;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions;
import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.core.transaction.support.OptionsUtil;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.support.TransactionFields;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.error.internal.ErrorClass;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.TransactionKVHandler;
import com.couchbase.client.core.transaction.util.TriFunction;
import com.couchbase.client.core.transaction.util.MeteringUnits;
import com.couchbase.client.core.util.Bytes;
import com.couchbase.client.core.util.CbPreconditions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Handles cleaning up expired/lost transactions.
 *
 * @author Graham Pople
 */
@Stability.Internal
public class TransactionsCleaner {
    private final Core core;
    private final CleanerHooks hooks;
    private final CoreTransactionsSupportedExtensions supported;

    private final static int BEING_LOGGING_FAILED_CLEANUPS_AT_WARN_AFTER_X_MINUTES = 60 * 2;

    public TransactionsCleaner(Core core, CleanerHooks hooks, CoreTransactionsSupportedExtensions supported) {
        this.core = Objects.requireNonNull(core);
        this.hooks = Objects.requireNonNull(hooks);
        this.supported = Objects.requireNonNull(supported);
    }

    private Duration kvDurableTimeout() {
        return core.context().environment().timeoutConfig().kvDurableTimeout();
    }

    private Duration kvNonMutatingTimeout() {
        return core.context().environment().timeoutConfig().kvTimeout();
    }

    Mono<Void> cleanupDocs(CoreTransactionLogger perEntryLog, CleanupRequest req, SpanWrapper pspan) {
        String attemptId = req.attemptId();

        switch (req.state()) {

            case COMMITTED: {
                // Half-finished commit to complete
                Mono<Void> inserts = commitDocs(perEntryLog, attemptId, req.stagedInserts(), req, pspan);
                Mono<Void> replaces = commitDocs(perEntryLog, attemptId, req.stagedReplaces(), req, pspan);
                Mono<Void> removes = removeDocsStagedForRemoval(perEntryLog, attemptId, req.stagedRemoves(), req, pspan);

                return inserts
                        .then(replaces)
                        .then(removes);
            }

            case ABORTED: {
                // Half-finished rollback to complete
                Mono<Void> inserts = removeDocs(perEntryLog, attemptId, req.stagedInserts(), req, pspan);

                // This will just remove the xattrs, which is exactly what we want
                Mono<Void> replaces = removeTxnLinks(perEntryLog, attemptId, req.stagedReplaces(), req, pspan);
                Mono<Void> removes = removeTxnLinks(perEntryLog, attemptId, req.stagedRemoves(), req, pspan);

                return inserts
                        .then(replaces)
                        .then(removes);
            }

            case PENDING:
                // Not much to do here, as don't have the ids of the docs involved in the txn.  Leave it.  All reads
                // will ignore it.
                perEntryLog.logDefer(req.attemptId(), "No docs cleanup possible as txn in state {}, just removing",
                        Event.Severity.DEBUG, req.state());

                return Mono.empty();

            case COMPLETED:
            case ROLLED_BACK:
            case NOT_STARTED:
            default:
                perEntryLog.logDefer(req.attemptId(), "No docs cleanup to do as txn in state {}, just removing",
                        Event.Severity.DEBUG, req.state());

                return Mono.empty();
        }
    }


    private Mono<Void> commitDocs(CoreTransactionLogger perEntryLog,
                                  String attemptId,
                                  List<DocRecord> docs,
                                  CleanupRequest req,
                                  SpanWrapper pspan) {
        return doPerDoc(perEntryLog, attemptId, docs, pspan, true, (collection, doc, lir) -> {
            CbPreconditions.check(doc.links() != null);
            CbPreconditions.check(doc.links().isDocumentInTransaction());
            CbPreconditions.check(doc.links().stagedContentJsonOrBinary().isPresent());

            byte[] content = doc.links().stagedContentJsonOrBinary().get();

            return hooks.beforeCommitDoc.apply(doc.id()) // Testing hook

                    .then(Mono.defer(() -> {
                        if (lir.tombstone()) {
                            return TransactionKVHandler.insert(core, collection, doc.id(), content, doc.links().stagedUserFlags().orElse(CodecFlags.JSON_COMMON_FLAGS), kvDurableTimeout(),
                                    req.durabilityLevel(), OptionsUtil.createClientContext("Cleaner::commitDocsInsert"), pspan);
                        } else {
                            List<SubdocMutateRequest.Command> commands = Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, null, false, true, false, 0),
                                    // No need to set binary flag here even if document is binary - that's just for xattrs.
                                    new SubdocMutateRequest.Command(SubdocCommandType.SET_DOC, "", content, false, false, false, 1)
                            );
                            return TransactionKVHandler.mutateIn(core, collection, doc.id(), kvDurableTimeout(),
                                    false, false, false,
                                    lir.tombstone(), false, doc.cas(), doc.links().stagedUserFlags().orElse(CodecFlags.JSON_COMMON_FLAGS),
                                    req.durabilityLevel(), OptionsUtil.createClientContext("Cleaner::commitDocs"), pspan,
                                    commands);
                        }
                    }))

                    .doOnSubscribe(v -> {
                        perEntryLog.logDefer(attemptId, "removing txn links and writing content to doc {}",
                                Event.Severity.DEBUG, DebugUtil.docId(doc));
                    })

                    .then();

        });
    }

    /**
     * Transaction is ABORTED, rolling back a replace or remove by removing its links
     */
    private Mono<Void> removeTxnLinks(CoreTransactionLogger perEntryLog,
                                      String attemptId,
                                      List<DocRecord> docs,
                                      CleanupRequest req,
                                      SpanWrapper pspan) {
        return doPerDoc(perEntryLog, attemptId, docs, pspan, false, (collectionIdentifier, doc, lir) -> {
            return hooks.beforeRemoveLinks.apply(doc.id())

                    .then(TransactionKVHandler.mutateIn(core, collectionIdentifier, doc.id(), kvDurableTimeout(),
                                false, false, false,
                                lir.tombstone(), false, doc.cas(), doc.userFlags(),
                                req.durabilityLevel(), OptionsUtil.createClientContext("Cleaner::removeTxnLinks"), pspan, Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, Bytes.EMPTY_BYTE_ARRAY, false, true, false, 0)
                            )))

                    .doOnSubscribe(v -> perEntryLog.logDefer(attemptId, "removing txn links from doc {}",
                            Event.Severity.DEBUG, DebugUtil.docId(doc)))

                    .then();
        });
    }

    private Mono<Void> removeDocsStagedForRemoval(CoreTransactionLogger perEntryLog,
                                                  String attemptId,
                                                  List<DocRecord> docs,
                                                  CleanupRequest req,
                                                  SpanWrapper pspan) {
        return doPerDoc(perEntryLog, attemptId, docs, pspan, true, (collection, doc, lir) -> {
            if (doc.links().isDocumentBeingRemoved()) {
                return hooks.beforeRemoveDocStagedForRemoval.apply(doc.id()) // testing hook

                        .then(TransactionKVHandler.remove(core, collection, doc.id(), kvDurableTimeout(),
                                        doc.cas(), req.durabilityLevel(), OptionsUtil.createClientContext("Cleaner::removeDoc"), pspan))

                        .doOnSubscribe(v -> perEntryLog.debug(attemptId, "removing doc {}", doc.id()))
                        .then();
            } else {
                return Mono.create(v -> {
                    perEntryLog.debug(attemptId, "doc {} does not have expected remove indication, skipping",
                            DebugUtil.docId(doc));
                    v.success();
                });
            }
        });
    }

    /**
     * Transaction is ABORTED, rolling back a staged insert.
     */
    private Mono<Void> removeDocs(CoreTransactionLogger perEntryLog,
                                  String attemptId,
                                  List<DocRecord> docs,
                                  CleanupRequest req,
                                  SpanWrapper pspan) {
        return doPerDoc(perEntryLog, attemptId, docs, pspan, false, (collection, doc, lir) -> {
            return hooks.beforeRemoveDoc.apply(doc.id())

                    .then(Mono.defer(() -> {
                        if (lir.tombstone()) {
                            return TransactionKVHandler.mutateIn(core, collection, doc.id(), kvDurableTimeout(),
                                    false, false, false,
                                    true, false, doc.cas(), doc.userFlags(),
                                    req.durabilityLevel(), OptionsUtil.createClientContext("Cleaner::commitDocs"), pspan,
                                    Collections.singletonList(
                                            new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, Bytes.EMPTY_BYTE_ARRAY, false, true, false, 0)
                                    ));
                        } else {
                            return TransactionKVHandler.remove(core, collection, doc.id(), kvDurableTimeout(),
                                    doc.cas(), req.durabilityLevel(), OptionsUtil.createClientContext("Cleaner::removeDocs"), pspan);
                        }
                    }))

                    .doOnSubscribe(v -> perEntryLog.debug(attemptId, "removing doc {}",
                            DebugUtil.docId(doc)))

                    .then();
        });
    }

    private Mono<Void> doPerDoc(CoreTransactionLogger perEntryLog,
                                String attemptId,
                                List<DocRecord> docs,
                                SpanWrapper pspan,
                                boolean requireCrc32ToMatchStaging,
                                TriFunction<CollectionIdentifier, CoreTransactionGetResult, CoreSubdocGetResult, Mono<Void>> perDoc) {
        return Flux.fromIterable(docs)
                .publishOn(core.context().environment().transactionsSchedulers().schedulerCleanup())
                .concatMap(docRecord -> {
                    CollectionIdentifier collection = new CollectionIdentifier(docRecord.bucketName(),
                            Optional.of(docRecord.scopeName()), Optional.of(docRecord.collectionName()));
                    MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

                    return hooks.beforeDocGet.apply(docRecord.id())

                            .then(doPerDocGotDoc(perEntryLog,
                                    attemptId,
                                    pspan,
                                    requireCrc32ToMatchStaging,
                                    perDoc,
                                    docRecord,
                                    collection,
                                    units));
                })
                .then();
    }

    private Mono<Void> doPerDocGotDoc(CoreTransactionLogger perEntryLog,
                                      String attemptId,
                                      SpanWrapper pspan,
                                      boolean requireCrc32ToMatchStaging,
                                      TriFunction<CollectionIdentifier, CoreTransactionGetResult, CoreSubdocGetResult, Mono<Void>> perDoc,
                                      DocRecord docRecord,
                                      CollectionIdentifier collection,
                                      MeteringUnits.MeteringUnitsBuilder units) {
        return DocumentGetter.justGetDoc(core, collection, docRecord.id(), kvNonMutatingTimeout(), pspan, true, perEntryLog, units, false)

                .flatMap(docOpt -> {
                    if (docOpt.isPresent()) {
                        CoreTransactionGetResult doc = docOpt.get().getT1();
                        CoreSubdocGetResult lir = docOpt.get().getT2();
                        MeteringUnits built = units.build();

                        perEntryLog.debug(attemptId, "handling doc {} with cas {} " +
                                        "and links {}, isTombstone={}{}",
                                DebugUtil.docId(doc), doc.cas(), doc.links(), lir.tombstone(), DebugUtil.dbg(built));

                        if (!doc.links().isDocumentInTransaction()) {
                            // The txn probably committed this doc then crashed.  This is fine, can skip.
                            perEntryLog.debug(attemptId, "no staged content for doc {}, assuming it was committed and skipping",
                                    DebugUtil.docId(doc));
                            return Mono.empty();
                        } else if (!doc.links().stagedAttemptId().get().equals(attemptId)) {
                            perEntryLog.debug(attemptId, "for doc {}, staged version is for a " +
                                            "different attempt {}, skipping",
                                    DebugUtil.docId(doc),
                                    doc.links().stagedAttemptId().get());
                            return Mono.empty();
                        } else {
                            // This field is only present if cleaning up a protocol 2 transaction.
                            if (requireCrc32ToMatchStaging && doc.links().crc32OfStaging().isPresent()) {
                                String crc32WhenStaging = doc.links().crc32OfStaging().get();
                                // This field must always be present since the doc has just been fetched.
                                String crc32Now = doc.crc32OfGet().get();

                                perEntryLog.debug(attemptId, "checking whether document {} has changed since staging, crc32 then {} now {}",
                                        DebugUtil.docId(doc), crc32WhenStaging, crc32Now);

                                if (!crc32Now.equals(crc32WhenStaging)) {
                                    perEntryLog.warn(attemptId, "document {} has changed since staging, ignoring it to avoid data loss",
                                            DebugUtil.docId(doc));
                                    return Mono.empty();
                                }
                            }

                            return perDoc.apply(collection, doc, lir);
                        }
                    } else {
                        perEntryLog.debug(attemptId, "could not get doc {}, skipping",
                                DebugUtil.docId(collection, docRecord.id()));
                        return Mono.empty();
                    }
                })

                .onErrorResume(err -> {
                    ErrorClass ec = ErrorClass.classify(err);

                    perEntryLog.debug(attemptId, "got exception while handling doc {}: {}",
                            DebugUtil.docId(collection, docRecord.id()), DebugUtil.dbg(err));

                    if (ec == ErrorClass.FAIL_CAS_MISMATCH) {
                        // Cleanup is conservative.  It could be running hours, even days after the
                        // transaction originally committed.  If the document has changed since it
                        // was staged, fail this cleanup attempt.  It will be tried again later.
                        perEntryLog.debug(attemptId, "got CAS mismatch while cleaning up doc {}, " +
                                        "failing this cleanup attempt (it will be retried)",
                                DebugUtil.docId(collection, docRecord.id()));

                        return Mono.error(err);
                    } else {
                        return Mono.error(err);
                    }
                });
    }

    private RequestTracer tracer() {
        return core.context().coreResources().requestTracer();
    }


    /**
     * Kept purely for backwards compatibility with FIT performer.
     */
    public Mono<TransactionCleanupAttemptEvent> cleanupATREntry(CollectionIdentifier atrCollection,
                                                                String atrId,
                                                                String attemptId,
                                                                ActiveTransactionRecordEntry atrEntry,
                                                                boolean isRegularCleanup) {
        CleanupRequest req = CleanupRequest.fromAtrEntry(atrCollection, atrEntry);
        return performCleanup(req, isRegularCleanup, null);
    }

    /*
     * Cleans up an expired attempt.
     *
     * Called from lost and regular.
     *
     * Pre-condition: This is an expired attempt that should be cleaned up.  This code will not check that.
     */
    public Mono<TransactionCleanupAttemptEvent> performCleanup(CleanupRequest req,
                                                               boolean isRegularCleanup,
                                                               @Nullable SpanWrapper pspan) {
        SpanWrapper span = SpanWrapperUtil.createOp(null, tracer(), req.atrCollection(), req.atrId(), TracingIdentifiers.TRANSACTION_CLEANUP, pspan)
                .attribute(TracingIdentifiers.ATTR_TRANSACTION_ATTEMPT_ID, req.attemptId())
                .attribute(TracingIdentifiers.ATTR_TRANSACTION_AGE, req.ageMillis())
                .attribute(TracingIdentifiers.ATTR_TRANSACTION_STATE, req.state());

        req.durabilityLevel().ifPresent(v -> {
            span.lowCardinalityAttribute(TracingIdentifiers.ATTR_DURABILITY, DurabilityLevelUtil.convertDurabilityLevel(v));
        });

        return Mono.defer(() -> {
            CollectionIdentifier atrCollection = req.atrCollection();
            String atrId = req.atrId();
            String attemptId = req.attemptId();

            CoreTransactionLogger perEntryLog = new CoreTransactionLogger(core.context().environment().eventBus(),
                    ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId).toString());

            Event.Severity logLevel = Event.Severity.DEBUG;
            //            if (!isRegularCleanup) {
            // Cleanup of lost txns should be rare enough to make them log worthy
            // Update: nope.  If client crashes could have thousands of these to remove.
            //                logLevel = Event.Severity.INFO;
            //            }
            perEntryLog.logDefer(attemptId, "Cleaning up ATR entry (isRegular={}) {}", logLevel, isRegularCleanup,
                    req);

            Mono<Void> cleanupDocs = cleanupDocs(perEntryLog, req, span);

            Mono<Object> cleanupEntry = removeATREntry(req.state(), atrCollection, atrId, attemptId, perEntryLog, span, req);

            return ForwardCompatibility.check(core, ForwardCompatibilityStage.CLEANUP_ENTRY, req.forwardCompatibility(), perEntryLog, supported)

                    .then(cleanupDocs)
                    .then(cleanupEntry)

                    .then(Mono.fromCallable(() -> {

                        TransactionCleanupAttemptEvent event = new TransactionCleanupAttemptEvent(Event.Severity.DEBUG,
                                true, isRegularCleanup, perEntryLog.logs(), attemptId, atrId, atrCollection,
                                req, "");

                        core.context().environment().eventBus().publish(event);

                        return event;
                    }))

                    .onErrorResume(err -> {
                        // If cleanup fails, assume it's for a temporary reason.  The lost txns process will have
                        // another go at cleaning it up in a while.

                        long ageInMinutes = TimeUnit.MILLISECONDS.toMinutes(req.ageMillis());

                        perEntryLog.logDefer(attemptId, "error while attempting to cleanup ATR entry {}, entry is {} mins old, " +
                                        "cleanup will retry later: {}",
                                Event.Severity.WARN, ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId), ageInMinutes, DebugUtil.dbg(err));

                        Event.Severity level = Event.Severity.DEBUG;
                        String addlDebug = "";

                        // TXNJ-208: If the transaction is very old and it's still failing to be cleaned up, it's
                        // indicative of a bug.  Log it at a level the application is likely to automatically see, so
                        // it can be caught and investigated.  TransactionCleanupAttempt will automatically write its
                        // logs too.
                        if (ageInMinutes >= BEING_LOGGING_FAILED_CLEANUPS_AT_WARN_AFTER_X_MINUTES) {
                            level = Event.Severity.WARN;
                            addlDebug = "despite being " + ageInMinutes + " mins old which could indicate a serious error - please raise with support.  Diagnostics: ";
                        }

                        TransactionCleanupAttemptEvent event = new TransactionCleanupAttemptEvent(level,
                                false, isRegularCleanup, perEntryLog.logs(), attemptId, atrId, atrCollection,
                                req, addlDebug);

                        core.context().environment().eventBus().publish(event);

                        return Mono.just(event);
                    })

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }


    Mono<Object> removeATREntry(AttemptState state, CollectionIdentifier atrCollection, String atrId, String attemptId,
                                CoreTransactionLogger perEntryLog, SpanWrapper pspan, CleanupRequest req) {
        List<SubdocMutateRequest.Command> specs = new ArrayList<>();

        if (state == AttemptState.PENDING) {
            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "attempts." + attemptId + "." + TransactionFields.ATR_FIELD_COMMIT_ONLY_IF_NOT_ABORTED, new byte[] { '0' }, false, true, false, 0));
        }
        specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "attempts." + attemptId, Bytes.EMPTY_BYTE_ARRAY, false, true, false, specs.size()));

        return hooks.beforeAtrRemove.get() // testing hook

                .then(TransactionKVHandler.mutateIn(core, atrCollection, atrId, kvDurableTimeout(),
                                false, false, false,
                                false, false, 0, CodecFlags.BINARY_COMMON_FLAGS,
                                req.durabilityLevel(), OptionsUtil.createClientContext("Cleaner::removeATREntry"), pspan, specs))

                .doOnNext(v -> perEntryLog.debug(attemptId, "successfully removed ATR entry"))
                .onErrorResume(err -> {
                    ErrorClass ec = ErrorClass.classify(err);

                    perEntryLog.debug(attemptId, "got exception while removing ATR entry {}: {}",
                            atrId, DebugUtil.dbg(err));

                    if (ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                        perEntryLog.logDefer(attemptId, "failed to remove {} as entry isn't there, likely" +
                                        " due to concurrent cleanup",
                                Event.Severity.DEBUG, ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId));
                        return Mono.empty();
                    } else if (ec == ErrorClass.FAIL_PATH_ALREADY_EXISTS) {
                        perEntryLog.logDefer(attemptId, "not removing {} as it has changed from PENDING to COMMITTED",
                                Event.Severity.DEBUG, ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId));
                        return Mono.error(err);
                    } else {
                        return Mono.error(err);
                    }
                })
                // Return Mono<Object> so we can track how many lost attempts were cleaned up
                .map(v -> v);
    }

    public CleanerHooks hooks() {
        return hooks;
    }
}
