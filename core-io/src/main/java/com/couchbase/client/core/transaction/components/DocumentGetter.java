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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.transaction.error.internal.ErrorClass;
import com.couchbase.client.core.transaction.util.MeteringUnits;
import com.couchbase.client.core.transaction.util.TransactionKVHandler;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordEntryNotFoundException;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordNotFoundException;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibilityStage;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.util.CbPreconditions;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

import static com.couchbase.client.core.transaction.components.OperationTypes.INSERT;
import static com.couchbase.client.core.transaction.support.OptionsUtil.createClientContext;
import static com.couchbase.client.core.transaction.support.OptionsUtil.kvTimeoutNonMutating;

/**
 * Responsible for doing transaction-aware get()s.
 */
@Stability.Internal
public class DocumentGetter {
    private DocumentGetter() {}

    public static Mono<Optional<CoreTransactionGetResult>> getAsync(Core core,
                                                                    CoreTransactionLogger LOGGER,
                                                                    CollectionIdentifier collection,
                                                                    CoreMergedTransactionConfig config,
                                                                    String docId,
                                                                    String byAttemptId,
                                                                    boolean justReturn,
                                                                    @Nullable SpanWrapper span,
                                                                    Optional<String> resolvingMissingATREntry,
                                                                    MeteringUnits.MeteringUnitsBuilder units,
                                                                    CoreTransactionsSupportedExtensions supported) {
        return justGetDoc(core, collection, docId, kvTimeoutNonMutating(core), span, true, LOGGER, units)
                .flatMap(origTrans -> {
                    if (justReturn) {
                        return Mono.just(origTrans.map(v -> v.getT1()));
                    } else if (origTrans.isPresent()) {
                        CoreTransactionGetResult r = origTrans.get().getT1();
                        CoreSubdocGetResult lir = origTrans.get().getT2();

                        if (!r.links().isDocumentInTransaction()) {
                            if (lir.tombstone()) {
                                return Mono.just(Optional.empty());
                            }
                            else {
                                return Mono.just(Optional.of(r));
                            }
                        }
                        else if (r.links().stagedAttemptId().get().equals(byAttemptId)) {
                            LOGGER.info(byAttemptId, "doc {} is in our own transaction attempt - RYOW", DebugUtil.docId(collection, docId));
                            if (r.links().op().get().equals(OperationTypes.REMOVE)) {
                                return Mono.just(Optional.empty());
                            }
                            else {
                                return Mono.just(Optional.of(CoreTransactionGetResult.createFrom(r, r.links().stagedContentJsonOrBinary().get())));
                            }
                        }
                        else if (resolvingMissingATREntry.equals(r.links().stagedAttemptId())) {

                            if (r.links().op().isPresent() && r.links().op().get().equals(INSERT)) {
                                LOGGER.info(byAttemptId,
                                        "doc {} is in the same transaction as last time indicating it's part of a lost PENDING transaction, it's a staged insert so returning empty",
                                        DebugUtil.docId(collection, docId));

                                return Mono.just(Optional.empty());
                            }
                            else {
                                LOGGER.info(byAttemptId,
                                        "doc {} is in the same transaction as last time indicating it's part of a lost PENDING transaction, returning body",
                                        DebugUtil.docId(collection, docId));

                                return Mono.just(Optional.of(r));
                            }
                        }
                        else {
                            CollectionIdentifier atrCollection = new CollectionIdentifier(r.links().atrBucketName().get(),
                                    r.links().atrScopeName(), r.links().atrCollectionName());

                            LOGGER.info(byAttemptId, "doc {} is in a transaction {}, looking up its status from ATR {} (MAV read)",
                                    DebugUtil.docId(collection, docId), r.links().stagedAttemptId(),  ActiveTransactionRecordUtil.getAtrDebug(atrCollection, r.links().atrId().get()));

                            return lookupStatusFromATR(core, atrCollection, r, byAttemptId, config, span, LOGGER, units, supported);
                        }
                    } else {
                        LOGGER.info(byAttemptId, "doc {} is not in a transaction", DebugUtil.docId(collection, docId));

                        return Mono.just(origTrans.map(v -> v.getT1()));
                    }
                });
    }

    public static Mono<Optional<Tuple2<CoreTransactionGetResult, CoreSubdocGetResult>>>
    justGetDoc(Core core,
               CollectionIdentifier collection,
               String docId,
               Duration timeout,
               @Nullable SpanWrapper span,
               boolean accessDeleted,
               CoreTransactionLogger logger,
               MeteringUnits.MeteringUnitsBuilder units) {
        return TransactionKVHandler.lookupIn(core, collection, docId, timeout, accessDeleted,
                        createClientContext("DocumentGetter::justGetDoc"), span,
                        Arrays.asList(
                                // The design doc details why these specs are fetched (rather than all of "txn")
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.id", true, 0),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.atr", true, 1),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.op.type", true, 2),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.op.stgd", true, false, 3),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.op.crc32", true, 4),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.restore", true, 5),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.fc", true, 6),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "$document", true, 7),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.op.bin", true, true, 8),
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "txn.aux", true, 9),
                                new SubdocGetRequest.Command(SubdocCommandType.GET_DOC, "", false, 10)
                        ))

                .map(fragment -> {
                    units.add(fragment.meta());
                    try {
                        return Optional.of(Tuples.of(CoreTransactionGetResult.createFrom(collection,
                                docId,
                                fragment), fragment));
                    }
                    catch (Throwable err) {
                        logger.info("", "Hit error while decoding doc's transaction metadata {}.{}.{}.{} {}",
                                collection.bucket(), collection.scope(), collection.collection(), docId, DebugUtil.dbg(err));
                        for (int i = 0; i < 10; i ++) {
                            dumpRawLookupInField(logger, fragment, 0);
                        }
                        throw new RuntimeException(err);
                    }
                })

                .onErrorResume(err -> {
                    units.add(err);
                    ErrorClass ec = ErrorClass.classify(err);

                    if (ec == ErrorClass.FAIL_DOC_NOT_FOUND) {
                        return Mono.just(Optional.empty());
                    }
                    else {
                        return Mono.error(err);
                    }
                });
    }

    private static void dumpRawLookupInField(CoreTransactionLogger logger, CoreSubdocGetResult fragment, int index) {
        try {
            if (fragment.field(index).status().success()) {
                byte[] raw = fragment.field(index).value();
                String asStr = new String(raw, StandardCharsets.UTF_8);
                logger.info("", "Field {}: {}", index, asStr);
            }
            else {
                logger.info("", "Field {} not found", index);
            }
        }
        catch (Throwable err) {
            logger.info("", "Error on field {}: {}", index, DebugUtil.dbg(err));
        }
    }

    private static Mono<Optional<CoreTransactionGetResult>> lookupStatusFromATR(Core core,
                                                                                CollectionIdentifier collection,
                                                                                CoreTransactionGetResult doc,
                                                                                String byAttemptId,
                                                                                CoreMergedTransactionConfig config,
                                                                                SpanWrapper span,
                                                                                @Nullable CoreTransactionLogger logger,
                                                                                MeteringUnits.MeteringUnitsBuilder units,
                                                                                CoreTransactionsSupportedExtensions supported) {
        CbPreconditions.check(doc.links().isDocumentInTransaction());
        CbPreconditions.check(doc.links().atrId().isPresent());
        CbPreconditions.check(doc.links().stagedAttemptId().isPresent());

        String atrId = doc.links().atrId().get();
        String attemptIdOfDoc = doc.links().stagedAttemptId().get();

        return ActiveTransactionRecord.findEntryForTransaction(core, collection, atrId, attemptIdOfDoc, config, span, logger, units)
                .onErrorResume(err -> {
                    units.add(err);
                    ErrorClass ec = ErrorClass.classify(err);

                    if (ec == ErrorClass.FAIL_DOC_NOT_FOUND) {
                        return Mono.error(new ActiveTransactionRecordNotFoundException(atrId, attemptIdOfDoc));
                    }
                    else {
                        return Mono.error(err);
                    }
                })
                .flatMap(atrDocOpt -> {
                    if (!atrDocOpt.isPresent()) {
                        return Mono.error(new ActiveTransactionRecordEntryNotFoundException(atrId, attemptIdOfDoc));
                    } else {
                        return atrFound(core, doc, byAttemptId, atrDocOpt.get(), logger, supported);
                    }
                });
    }

    private static Mono<Optional<CoreTransactionGetResult>> atrFound(Core core,
                                                                     CoreTransactionGetResult doc,
                                                                     String byAttemptId,
                                                                     ActiveTransactionRecordEntry entry,
                                                                     CoreTransactionLogger logger,
                                                                     CoreTransactionsSupportedExtensions supported) {
        if (doc.links().stagedAttemptId().isPresent()
                && entry.attemptId().equals(byAttemptId)) {
            // Attempt is reading its own writes
            // This is here as backup, it should be returned from the in-memory cache instead
            if (doc.links().isDocumentBeingRemoved()) {
                return Mono.just(Optional.empty());
            }
            else {
                return Mono.just(Optional.of(CoreTransactionGetResult.createFrom(doc,
                        doc.links().stagedContentJsonOrBinary().get())));
            }
        } else {
            return ForwardCompatibility.check(core, ForwardCompatibilityStage.GETS_READING_ATR, entry.forwardCompatibility(), logger, supported)

                    .then(Mono.defer(() -> {
                        logger.info(byAttemptId, "found ATR for MAV read in state: {}", entry);

                        switch (entry.state()) {
                            case COMMITTED:
                            case COMPLETED:
                                if (doc.links().isDocumentBeingRemoved()) {
                                    return Mono.just(Optional.empty());
                                } else {
                                    return Mono.just(Optional.of(CoreTransactionGetResult.createFrom(doc,
                                            doc.links().stagedContentJsonOrBinary().get())));
                                }

                            default:
                                if (doc.links().op().isPresent() && doc.links().op().get().equals(INSERT)) {
                                    // This document is being inserted, so shouldn't be visible yet
                                    return Mono.just(Optional.empty());
                                } else {
                                    // Could make this more efficient with a custom transcoder that can return byte[] directly, but this code path
                                    // won't be hit often
                                    return Mono.just(Optional.of(CoreTransactionGetResult.createFrom(doc,
                                            doc.contentAsBytes())));
                                }
                        }
                    }));
        }
    }
}
