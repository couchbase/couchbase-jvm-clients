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

package com.couchbase.client.core.transaction;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.util.RawValue;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordEntryNotFoundException;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordFullException;
import com.couchbase.client.core.error.transaction.ActiveTransactionRecordNotFoundException;
import com.couchbase.client.core.error.transaction.AttemptExpiredException;
import com.couchbase.client.core.error.transaction.AttemptNotFoundOnQueryException;
import com.couchbase.client.core.error.transaction.CommitNotPermittedException;
import com.couchbase.client.core.error.transaction.ConcurrentOperationsDetectedOnSameDocumentException;
import com.couchbase.client.core.error.transaction.ForwardCompatibilityFailureException;
import com.couchbase.client.core.error.transaction.PreviousOperationFailedException;
import com.couchbase.client.core.error.transaction.RetryTransactionException;
import com.couchbase.client.core.error.transaction.RollbackNotPermittedException;
import com.couchbase.client.core.error.transaction.TransactionAlreadyAbortedException;
import com.couchbase.client.core.error.transaction.TransactionAlreadyCommittedException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionCommitAmbiguousException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionExpiredException;
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException;
import com.couchbase.client.core.error.transaction.internal.ForwardCompatibilityRequiresRetryException;
import com.couchbase.client.core.error.transaction.internal.RetryAtrCommitException;
import com.couchbase.client.core.error.transaction.internal.RetryOperationException;
import com.couchbase.client.core.error.transaction.internal.WrappedTransactionOperationFailedException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateResponse;
import com.couchbase.client.core.msg.query.CoreQueryAccessor;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.reactor.Jitter;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordUtil;
import com.couchbase.client.core.transaction.components.DocRecord;
import com.couchbase.client.core.transaction.components.DocumentGetter;
import com.couchbase.client.core.transaction.components.DocumentMetadata;
import com.couchbase.client.core.transaction.components.DurabilityLevelUtil;
import com.couchbase.client.core.transaction.components.OperationTypes;
import com.couchbase.client.core.transaction.components.TransactionLinks;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibilityStage;
import com.couchbase.client.core.transaction.forwards.Supported;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.core.transaction.support.OptionsUtil;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.support.StagedMutation;
import com.couchbase.client.core.transaction.support.StagedMutationType;
import com.couchbase.client.core.transaction.support.TransactionFields;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.LockTokens;
import com.couchbase.client.core.transaction.util.LogDeferThrowable;
import com.couchbase.client.core.transaction.util.MonoBridge;
import com.couchbase.client.core.transaction.util.QueryUtil;
import com.couchbase.client.core.transaction.util.ReactiveLock;
import com.couchbase.client.core.transaction.util.ReactiveWaitGroup;
import com.couchbase.client.core.transaction.util.TransactionKVHandler;
import com.couchbase.client.core.transaction.util.TriFunction;
import com.couchbase.client.core.transaction.error.internal.ErrorClass;
import com.couchbase.client.core.cnc.events.transaction.IllegalDocumentStateEvent;
import com.couchbase.client.core.transaction.util.MeteringUnits;
import com.couchbase.client.core.util.BucketConfigUtil;
import com.couchbase.client.core.util.CbPreconditions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_ATR_COMMIT;
import static com.couchbase.client.core.config.BucketCapabilities.SUBDOC_REVIVE_DOCUMENT;
import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.Builder.createError;
import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.FinalErrorToRaise;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_AMBIGUOUS;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_ATR_FULL;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_CAS_MISMATCH;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_DOC_ALREADY_EXISTS;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_DOC_NOT_FOUND;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_EXPIRY;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_HARD;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_OTHER;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_PATH_ALREADY_EXISTS;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.FAIL_TRANSIENT;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.TRANSACTION_OPERATION_FAILED;
import static com.couchbase.client.core.transaction.error.internal.ErrorClass.classify;
import static com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks.HOOK_COMMIT_DOC_CHANGED;
import static com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC_CHANGED;
import static com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks.HOOK_STAGING_DOC_CHANGED;

/**
 * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well
 * as commit or rollback the transaction.
 */
@Stability.Internal
public class CoreTransactionAttemptContext {
    static class QueryContext {
        public final NodeIdentifier queryTarget;
        public final @Nullable String bucketName;
        public final @Nullable String scopeName;

        public QueryContext(NodeIdentifier queryTarget, @Nullable String bucketName, @Nullable String scopeName) {
            this.queryTarget = Objects.requireNonNull(queryTarget);
            this.bucketName = bucketName;
            this.scopeName = scopeName;
        }
    }

    public static int TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED = 0x1;
    public static int TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED = 0x2;
    public static int TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK = 0x4;
    public static int TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY = 0x8;

    // Bits 0-3: BehaviourFlags
    // Bits 4-6: FinalErrorToRaise
    public static int STATE_BITS_POSITION_FINAL_ERROR = 4;
    public static int STATE_BITS_MASK_FINAL_ERROR = 0b1110000;
    public static int STATE_BITS_MASK_BITS =        0b0001111;

    private final AtomicInteger stateBits = new AtomicInteger(0);

    private final CoreTransactionAttemptContextHooks hooks;
    private final Core core;
    private final CoreMergedTransactionConfig config;

    // TXNJ-64: Commit documents in the order they were staged
    // Does not need to be threadsafe, always written under lock (in addStageMutation we need to atomically remove and add it)
    private final ArrayList<StagedMutation> stagedMutationsLocked = new ArrayList<>();

    private final String attemptId;
    private final CoreTransactionContext overall;
    // The time this particular attempt started
    private final Duration startTimeClient;
    private Optional<String> atrId = Optional.empty();
    private Optional<CollectionIdentifier> atrCollection = Optional.empty();
    final CoreTransactionLogger LOGGER;
    private AttemptState state = AttemptState.NOT_STARTED;
    private final CoreTransactionsReactive parent;
    private final SpanWrapper attemptSpan;

    // [EXP-ROLLBACK] [EXP-COMMIT-OVERTIME]: Internal flag to indicate that transaction has expired.  After,
    // this, one attempt will be made to rollback the txn (or commit it, if the expiry happened during commit).
    private volatile boolean expiryOvertimeMode = false;
    private volatile @Nullable QueryContext queryContext = null;
    // Purely for debugging (so we don't have to log the statement everywhere), associate each statement with an id
    private final AtomicInteger queryStatementIdx = new AtomicInteger(0);

    // lockDebugging is true currently as full thread-safety is a new addition. It will be changed to false by default in future releases.
    private final boolean lockDebugging = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.debug.lock", "true"));
    private final boolean monoBridgeDebugging = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.debug.monoBridge", "false"));
    private final boolean threadSafetyEnabled = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.threadSafety", "true"));

    // Note a KV operation that's being performed via N1QL is still counted as a query op.
    private final ReactiveWaitGroup kvOps = new ReactiveWaitGroup(this, lockDebugging);

    private final ReactiveLock mutex = new ReactiveLock(this, lockDebugging);

    private final int EXPIRY_THRESHOLD = Integer.parseInt(System.getProperty("com.couchbase.transactions.expiryThresholdMs", "10"));

    private final CoreQueryAccessor coreQueryAccessor;
    private MeteringUnits.MeteringUnitsBuilder meteringUnitsBuilder = new MeteringUnits.MeteringUnitsBuilder();

    // Just a safety measure to make sure we don't get into hard tight loops
    public static Duration DEFAULT_DELAY_RETRYING_OPERATION = Duration.ofMillis(3);

    private static final reactor.util.retry.Retry RETRY_OPERATION_UNTIL_EXPIRY = Retry.anyOf(RetryOperationException.class)
            .exponentialBackoff(Duration.ofMillis(1), Duration.ofMillis(100))
            .jitter(Jitter.random()).toReactorRetry();

    private static final reactor.util.retry.Retry RETRY_OPERATION_UNTIL_EXPIRY_WITH_FIXED_RETRY = Retry.anyOf(RetryOperationException.class)
            .fixedBackoff(DEFAULT_DELAY_RETRYING_OPERATION)
            .toReactorRetry();

    public CoreTransactionAttemptContext(Core core, CoreTransactionContext overall, CoreMergedTransactionConfig config, String attemptId,
                                         CoreTransactionsReactive parent, Optional<SpanWrapper> parentSpan, CoreTransactionAttemptContextHooks hooks) {
        this.core = Objects.requireNonNull(core);
        this.overall = Objects.requireNonNull(overall);
        this.LOGGER = Objects.requireNonNull(overall.LOGGER);
        this.config = Objects.requireNonNull(config);
        this.attemptId = Objects.requireNonNull(attemptId);
        this.startTimeClient = Duration.ofNanos(System.nanoTime());
        this.parent = Objects.requireNonNull(parent);
        this.hooks = Objects.requireNonNull(hooks);
        this.coreQueryAccessor = new CoreQueryAccessor(core);

        attemptSpan = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_ATTEMPT, parentSpan.orElse(null));
    }

    private ObjectNode makeQueryTxDataLocked() {
        assertLocked("makeQueryTxData");

        ObjectNode out = Mapper.createObjectNode();
                out.set("id", Mapper.createObjectNode()
                        .put("txn", transactionId())
                        .put("atmpt", attemptId));
                out.set("state", Mapper.createObjectNode()
                        .put("timeLeftMs", expiryRemainingMillis()));
                out.set("config", Mapper.createObjectNode()
                        .put("kvTimeoutMs",
                                core.context().environment().timeoutConfig().kvDurableTimeout().toMillis())
                        .put("durabilityLevel", config.durabilityLevel().name())
                        .put("numAtrs", config.numAtrs()));

        ArrayNode mutations = Mapper.createArrayNode();
        stagedMutationsLocked.forEach(sm -> {
           mutations.add(Mapper.createObjectNode()
                   .put("scp", sm.collection.scope().orElse(DEFAULT_SCOPE))
                   .put("coll", sm.collection.collection().orElse(DEFAULT_COLLECTION))
                   .put("bkt", sm.collection.bucket())
                   .put("id", sm.id)
                   .put("cas", Long.toString(sm.cas))
                   .put("type", sm.type.name()));
        });

        out.set("mutations", mutations);

        if (atrCollection.isPresent() && atrId.isPresent()) {
            out.set("atr", Mapper.createObjectNode()
                    .put("id", atrId.get())
                    .put("bkt", atrCollection.get().bucket())
                    .put("scp", atrCollection.get().scope().orElse(DEFAULT_SCOPE))
                    .put("coll", atrCollection.get().collection().orElse(DEFAULT_COLLECTION)));
        }
        else if (config.metadataCollection().isPresent()) {
            CollectionIdentifier mc = config.metadataCollection().get();
            out.set("atr", Mapper.createObjectNode()
                    .put("bkt", mc.bucket())
                    .put("scp", mc.scope().orElse(DEFAULT_SCOPE))
                    .put("coll", mc.collection().orElse(DEFAULT_COLLECTION)));
        }

        return out;
    }

    public Core core() {
        return core;
    }

    /**
     * Bear in mind, in this code with the blocking API:
     *
     * cluster.transactions().run((ctx) -> {
     *     Thread ct = Thread.currentThread();
     *
     *     ctx.insert(collection, docId, content);
     *
     *     Thread ct2 = Thread.currentThread();
     * });
     *
     * ct will _always_ equal ct2 (which is what we want), regardless of what we do with schedulers.  As there's no way
     * to change the current thread of execution in that way.
     *
     * We put things onto our scheduler for the benefit of reactive users.  Because we don't want to pass control back
     * to user space (the lambda) while still on a limited internal SDK thread.
     */
    public Scheduler scheduler() {
        return core.context().environment().transactionsSchedulers().schedulerBlocking();
    }

    /**
     * Returns the globally unique ID of this attempt, which may be useful for debugging and logging purposes.
     */
    public String attemptId() {
        return attemptId;
    }

    /**
     * Returns the globally unique ID of the overall transaction owning this attempt, which may be useful for debugging
     * and logging purposes.
     */
    public String transactionId() {
        return overall.transactionId();
    }

    private List<StagedMutation> stagedReplacesLocked() {
        assertLocked("stagedReplaces");
        assertNotQueryMode("stagedReplaces");
        return stagedMutationsLocked.stream().filter(v -> v.type == StagedMutationType.REPLACE).collect(Collectors.toList());
    }

    private List<StagedMutation> stagedRemovesLocked() {
        assertLocked("stagedRemoves");
        assertNotQueryMode("stagedRemoves");
        return stagedMutationsLocked.stream().filter(v -> v.type == StagedMutationType.REMOVE).collect(Collectors.toList());
    }

    private List<StagedMutation> stagedInsertsLocked() {
        assertNotQueryMode("stagedInserts");
        assertLocked("stagedInserts");
        return stagedMutationsLocked.stream().filter(v -> v.type == StagedMutationType.INSERT).collect(Collectors.toList());
    }

    private Optional<StagedMutation> checkForOwnWriteLocked(CollectionIdentifier collection, String id) {
        assertLocked("checkForOwnWrite");
        assertNotQueryMode("checkForOwnWrite");
        Optional<StagedMutation> ownReplace = stagedReplacesLocked().stream()
                .filter(v -> v.collection.equals(collection) && v.id.equals(id))
                .findFirst();

        if (ownReplace.isPresent()) {
            return ownReplace;
        }

        Optional<StagedMutation> ownInserted = stagedInsertsLocked().stream()
                .filter(v -> v.collection.equals(collection) && v.id.equals(id))
                .findFirst();

        if (ownInserted.isPresent()) {
            return ownInserted;
        }

        return Optional.empty();
    }

    private Mono<Void> errorIfExpiredAndNotInExpiryOvertimeMode(String stage, Optional<String> docId) {
        if (expiryOvertimeMode) {
            LOGGER.info(attemptId, "not doing expiry check in %s as already in expiry-overtime-mode", stage);
            return Mono.empty();
        }
        else if (hasExpiredClientSide(stage, docId)) {
            LOGGER.info(attemptId, "has expired in stage %s", stage);

            // We don't set expiry-overtime-mode here, that's done by the error handler
            return Mono.error(new AttemptExpiredException("Attempt has expired in stage " + stage));
        }

        return Mono.empty();
    }

    private void checkExpiryPreCommitAndSetExpiryOvertimeMode(String stage, Optional<String> docId) {
        if (hasExpiredClientSide(stage, docId)) {
            LOGGER.info(attemptId, "has expired in stage %s, setting expiry-overtime-mode", stage);

            // Combo of setting this mode and throwing AttemptExpired will result in a attempt to
            // rollback, which will ignore expiries, and bail out if anything fails
            expiryOvertimeMode = true;

            throw operationFailed(createError()
                    .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                    .build());
        }
    }

    /**
     * Gets a document with the specified <code>id</code> and from the specified Couchbase <code>bucket</code>.
     *
     * @param collection the Couchbase collection the document exists on
     * @param id         the document's ID
     * @return an <code>Optional</code> containing the document, or <code>Optional.empty()</code> if not found
     */
    private Mono<Optional<CoreTransactionGetResult>> getInternal(CollectionIdentifier collection, String id, SpanWrapper pspan) {

        return doKVOperation("get " + DebugUtil.docId(collection, id), pspan, CoreTransactionAttemptContextHooks.HOOK_GET, collection, id,
                (operationId, span, lockToken) -> Mono.defer(() -> {
                    if (queryModeLocked()) {
                        return getWithQueryLocked(collection, id, lockToken, span);
                    } else {
                        return getWithKVLocked(collection, id, Optional.empty(), span, lockToken);
                    }
                }));
    }

    private Mono<Optional<CoreTransactionGetResult>> getWithKVLocked(CollectionIdentifier collection,
                                                                     String id,
                                                                     Optional<String> resolvingMissingATREntry,
                                                                     SpanWrapper pspan,
                                                                     ReactiveLock.Waiter lockToken) {
        return Mono.defer(() -> {
            assertLocked("getWithKV");

            LOGGER.info(attemptId, "getting doc %s, resolvingMissingATREntry=%s", DebugUtil.docId(collection, id),
                    resolvingMissingATREntry.orElse("<empty>"));

            Optional<StagedMutation> ownWrite = checkForOwnWriteLocked(collection, id);
            if (ownWrite.isPresent()) {
                StagedMutation ow = ownWrite.get();
                // Can only use if the content is there.  If not, we will read our own-write from KV instead.  This is just an optimisation.
                boolean usable = ow.content != null;
                LOGGER.info(attemptId, "found own-write of mutated doc %s, usable = %s", DebugUtil.docId(collection, id), usable);
                if (usable) {
                    // Use the staged content as the body.  The staged content in the output TransactionGetResult should not be used for anything so we are safe to pass null.
                    return unlock(lockToken, "found own-write of mutation")
                            .then(Mono.just(Optional.of(createTransactionGetResult(ow.operationId, collection, id, ow.content, null, ow.cas,
                                    ow.documentMetadata, ow.type.toString(), ow.crc32))));
                }
            }
            Optional<StagedMutation> ownRemove = stagedRemovesLocked().stream().filter(v -> {
                return v.collection.equals(collection) && v.id.equals(id);
            }).findFirst();

            if (ownRemove.isPresent()) {
                LOGGER.info(attemptId, "found own-write of removed doc %s",
                        DebugUtil.docId(collection, id));

                return unlock(lockToken, "found own-write of removed")
                        .then(Mono.just(Optional.empty()));
            }

            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

            return hooks.beforeUnlockGet.apply(this, id) // in-lock test hook

                    .then(unlock(lockToken, "standard"))

                    .then(hooks.beforeDocGet.apply(this, id)) // post-lock test hook

                    .then(DocumentGetter.getAsync(core,
                            LOGGER,
                            collection,
                            config,
                            id,
                            attemptId,
                            false,
                            pspan,
                            resolvingMissingATREntry,
                            units))

                    .publishOn(scheduler())

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().cause(err);
                        MeteringUnits built = addUnits(units.build());

                        LOGGER.warn(attemptId, "got error while getting doc %s%s in %dus: %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(built), pspan.elapsedMicros(), dbg(err));

                        if (err instanceof ForwardCompatibilityRequiresRetryException
                                || err instanceof ForwardCompatibilityFailureException) {
                            TransactionOperationFailedException.Builder error = createError()
                                    .cause(new ForwardCompatibilityFailureException());
                            if (err instanceof ForwardCompatibilityRequiresRetryException) {
                                error.retryTransaction();
                            }
                            return Mono.error(operationFailed(error.build()));
                        } else if (ec == TRANSACTION_OPERATION_FAILED) {
                            return Mono.error(err);
                        }
                        // These internal errors can be raised from getAsync
                        // BF-CBD-3705
                        else if (err instanceof ActiveTransactionRecordNotFoundException || err instanceof ActiveTransactionRecordEntryNotFoundException) {
                            String attemptIdToCheck;

                            if (err instanceof ActiveTransactionRecordNotFoundException) {
                                attemptIdToCheck = ((ActiveTransactionRecordNotFoundException) err).attemptId();
                            } else {
                                attemptIdToCheck = ((ActiveTransactionRecordEntryNotFoundException) err).attemptId();
                            }

                            return lock("get relock")
                                    .flatMap(newLockToken -> getWithKVLocked(collection,
                                                id,
                                                Optional.of(attemptIdToCheck),
                                                pspan,
                                                newLockToken)
                                                .onErrorResume(e ->
                                                        unlock(newLockToken, "relock error")
                                                                .then(Mono.error(e))));
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(builder.doNotRollbackAttempt().build()));
                        } else if (ec == FAIL_TRANSIENT) {
                            return Mono.error(operationFailed(builder.retryTransaction().build()));
                        } else {
                            return Mono.error(operationFailed(builder.build()));
                        }
                    })

                    .flatMap(v -> {
                        long elapsed = pspan.elapsedMicros();
                        MeteringUnits built = addUnits(units.build());
                        if (v.isPresent()) {
                            LOGGER.info(attemptId, "completed get of %s%s in %dus", v.get(), DebugUtil.dbg(built), elapsed);
                        } else {
                            LOGGER.info(attemptId, "completed get of %s%s, could not find, in %dus",
                                    DebugUtil.docId(collection, id), DebugUtil.dbg(built), elapsed);
                        }

                        // Testing hook
                        return hooks.afterGetComplete.apply(this, id).thenReturn(v);
                    })

                    .flatMap(doc -> {
                        if (doc.isPresent()) {
                            return forwardCompatibilityCheck(ForwardCompatibilityStage.GETS, doc.flatMap(v -> v.links().forwardCompatibility()))
                                    .thenReturn(doc);
                        } else {
                            return Mono.just(doc);
                        }
                    });
        });
    }

    private ObjectNode makeTxdata() {
        return Mapper.createObjectNode()
                .put("kv", true);
    }

    private Mono<Optional<CoreTransactionGetResult>> getWithQueryLocked(CollectionIdentifier collection, String id, ReactiveLock.Waiter lockToken, SpanWrapper span) {
        return Mono.defer(() -> {
            assertLocked("getWithQuery");

            int sidx = queryStatementIdx.getAndIncrement();
            AtomicReference<ReactiveLock.Waiter> lt = new AtomicReference<>(lockToken);

            ArrayNode params = Mapper.createArrayNode()
                    .add(makeKeyspace(collection))
                    .add(id);

            ObjectNode queryOptions = Mapper.createObjectNode();
            queryOptions.set("args", params);

            return queryWrapperBlockingLocked(sidx,
                    null,
                    null,
                    "EXECUTE __get",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_GET,
                    false,
                    true,
                    makeTxdata(),
                    params,
                    span,
                    false,
                    lt,
                    true)

                    .map(result -> result.rows)

                    .map(rows -> {
                        Optional<CoreTransactionGetResult> ret;
                        if (rows.isEmpty()) {
                            ret = Optional.empty();
                        } else {
                            ObjectNode row;
                            byte[] content;
                            try {
                                row = Mapper.reader().readValue(rows.get(0).data(), ObjectNode.class);
                                JsonNode doc = row.path("doc");
                                content = Mapper.writer().writeValueAsBytes(doc);
                            } catch (IOException e) {
                                throw new DecodingFailureException(e);
                            }
                            String scas = row.path("scas").textValue();
                            long cas = Long.parseLong(scas);
                            // Will only be present if doc was in a transaction
                            JsonNode txnMeta = row.path("txnMeta");
                            Optional<String> crc32 = Optional.ofNullable(row.path("crc32").textValue());

                            logger().info(attemptId, "got doc %s from query with scas=%s meta=%s",
                                DebugUtil.docId(collection, id), scas, txnMeta.isMissingNode() ? "null" : txnMeta.textValue());

                            ret = Optional.of(new CoreTransactionGetResult(id,
                                    content,
                                    cas,
                                    collection,
                                    null,
                                    Optional.empty(),
                                    txnMeta.isMissingNode() ? Optional.empty() : Optional.of(txnMeta),
                                    crc32));
                        }
                        return ret;
                    })

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().cause(err);
                        span.recordExceptionAndSetErrorStatus(err);

                        if (err instanceof DocumentNotFoundException) {
                            return Mono.just(Optional.empty());
                        } else if (err instanceof TransactionOperationFailedException) {
                            return Mono.error(err);
                        } else {
                            return Mono.error(operationFailed(builder.build()));
                        }
                    })

                    // unlock is here rather than in usual place straight after queryWrapper, so it handles the DocumentNotFoundException case too
                    .flatMap(result -> unlock(lt.get(), "getWithQueryLocked end", false)
                            .thenReturn(result));
        });
    }

    /**
     * Gets a document with the specified <code>id</code> and from the specified Couchbase <code>bucket</code>.
     * <p>
     *
     * @param collection the Couchbase collection the document exists on
     * @param id         the document's ID
     * @return a <code>TransactionGetResultInternal</code> containing the document
     */
    public Mono<CoreTransactionGetResult> get(CollectionIdentifier collection, String id) {
        return Mono.defer(() -> {
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_GET, attemptSpan);
            return getInternal(collection, id, span)
                    .doOnError(err -> span.finishWithErrorStatus())
                    .flatMap(doc -> {
                        span.finish();
                        if (doc.isPresent()) {
                            return Mono.just(doc.get());
                        } else {
                            return Mono.error(new DocumentNotFoundException(ReducedKeyValueErrorContext.create(id)));
                        }
                    });
        });
    }

    boolean hasExpiredClientSide(String place, Optional<String> docId) {
        boolean over = overall.hasExpiredClientSide();
        boolean hook = hooks.hasExpiredClientSideHook.apply(this, place, docId);

        if (over) LOGGER.info(attemptId, "expired in %s", place);
        if (hook) LOGGER.info(attemptId, "fake expiry in %s", place);

        return over || hook ;
    }

    public Optional<String> atrId() {
        return atrId;
    }

    public Optional<CollectionIdentifier> atrCollection() {
        return atrCollection;
    }

    /**
     * TXNJ-223: Must use the default collection of the bucket that the first mutated document is on, for the ATR.
     */
    private CollectionIdentifier getAtrCollection(CollectionIdentifier docCollection) {
        if (config.metadataCollection().isPresent()) {
            return config.metadataCollection().get();
        }
        else {
            return new CollectionIdentifier(docCollection.bucket(), Optional.of(DEFAULT_SCOPE), Optional.of(DEFAULT_COLLECTION));
        }
    }

    private static String makeKeyspace(CollectionIdentifier collection) {
        return String.format("default:`%s`.`%s`.`%s`",
                collection.bucket(),
                collection.scope().orElse(DEFAULT_SCOPE),
                collection.collection().orElse(DEFAULT_COLLECTION));
    }
    
    /**
     * Inserts a new document into the specified Couchbase <code>collection</code>.
     *
     * @param collection the Couchbase collection in which to insert the doc
     * @param id         the document's unique ID
     * @param content    the content to insert
     * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResultInternal</code>
     */
    public Mono<CoreTransactionGetResult> insert(CollectionIdentifier collection, String id, byte[] content, SpanWrapper pspan) {

        return doKVOperation("insert " + DebugUtil.docId(collection, id), pspan, CoreTransactionAttemptContextHooks.HOOK_INSERT, collection, id,
                (operationId, span, lockToken) -> insertInternal(operationId, collection, id, content, span, lockToken));
    }

    private Mono<CoreTransactionGetResult> insertInternal(String operationId, CollectionIdentifier collection, String id, byte[] content,
                                                          SpanWrapper span, ReactiveLock.Waiter lockToken) {
        return Mono.defer(() -> {
            if (queryModeLocked()) {
                return insertWithQueryLocked(collection, id, content, lockToken, span);
            } else {
                return insertWithKVLocked(operationId, collection, id, content, span, lockToken);
            }
        });
    }

    private Mono<CoreTransactionGetResult> insertWithKVLocked(String operationId, CollectionIdentifier collection, String id, byte[] content, SpanWrapper span, ReactiveLock.Waiter lockToken) {
        assertLocked("insertWithKV");
        Optional<StagedMutation> existing = findStagedMutationLocked(collection, id);

        if (existing.isPresent()) {
            StagedMutation op = existing.get();

            if (op.type == StagedMutationType.INSERT || op.type == StagedMutationType.REPLACE) {
                return Mono.error(new DocumentExistsException(null));
            }

            // REMOVE handling is below
        }

        return initAtrIfNeededLocked(collection, id, span)

                .then(hooks.beforeUnlockInsert.apply(this, id)) // testing hook

                .then(unlock(lockToken, "standard"))

                .then(Mono.defer(() -> {
                    if (existing.isPresent() && existing.get().type == StagedMutationType.REMOVE) {
                        // Use the doc of the remove to ensure CAS
                        // It is ok to pass null for contentOfBody, since we are replacing a remove
                        return createStagedReplace(operationId, existing.get().collection, existing.get().id, existing.get().cas,
                                existing.get().documentMetadata, existing.get().crc32, content, null, span, false);
                    } else {
                        return createStagedInsert(operationId, collection, id, content, span, Optional.empty());
                    }
                }));
    }

    private Mono<CoreTransactionGetResult> insertWithQueryLocked(CollectionIdentifier collection, String id, byte[] content, ReactiveLock.Waiter lockToken, SpanWrapper span) {
        return Mono.defer(() -> {
            ArrayNode params = Mapper.createArrayNode()
                    .add(makeKeyspace(collection))
                    .add(id)
                    .addRawValue(new RawValue(new String(content, StandardCharsets.UTF_8)))
                    .add(Mapper.createObjectNode());

            int sidx = queryStatementIdx.getAndIncrement();
            AtomicReference<ReactiveLock.Waiter> lt = new AtomicReference<>(lockToken);

            ObjectNode queryOptions = Mapper.createObjectNode();
            queryOptions.set("args", params);

            // All KV ops are done with the scanConsistency set at the BEGIN WORK default, e.g. from
            // PerTransactionConfig & TransactionConfig
            return queryWrapperBlockingLocked(sidx,
                    null,
                    null,
                    "EXECUTE __insert",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_INSERT,
                    false,
                    true,
                    makeTxdata(),
                    params,
                    span,
                    false,
                    lt,
                    true)

                    .flatMap(result -> unlock(lt.get(), "insertWithQueryLocked end", false)
                            .thenReturn(result.rows))

                    .map(rows -> {
                        if (rows.isEmpty()) {
                            throw operationFailed(TransactionOperationFailedException.Builder.createError()
                                    .cause(new IllegalStateException("Did not get any rows back while KV inserting with query"))
                                    .build());
                        }

                        ObjectNode row;
                        try {
                            row = Mapper.reader().readValue(rows.get(0).data(), ObjectNode.class);
                        } catch (IOException e) {
                            throw new DecodingFailureException(e);
                        }
                        String scas = row.path("scas").textValue();
                        long cas = Long.parseLong(scas);

                        return CoreTransactionGetResult.createFromInsert(collection,
                                id,
                                content.toString().getBytes(StandardCharsets.UTF_8),
                                transactionId(),
                                attemptId,
                                null, null, null, null,
                                cas);
                    })
                .onErrorResume(err -> {
                    // Error has already gone through convertQueryError, and may be a TransactionOperationFailedException already
                    if (err instanceof TransactionOperationFailedException) {
                        return Mono.error(err);
                    }

                    if (err instanceof DocumentExistsException) {
                        return Mono.error(err);
                    }

                    ErrorClass ec = classify(err);
                    TransactionOperationFailedException out = operationFailed(createError().cause(err).build());
                    span.recordExceptionAndSetErrorStatus(err);
                    return Mono.error(out);
                });
        });
    }

    protected String randomAtrIdForVbucket(CoreTransactionAttemptContext self, Integer vbucketIdForDoc, int numAtrs) {
        return hooks.randomAtrIdForVbucket.apply(self)
                .orElse(ActiveTransactionRecordIds.randomAtrIdForVbucket(vbucketIdForDoc, numAtrs));
    }


    private Mono<ReactiveLock.Waiter> lock(String dbg) {
        return Mono.defer(() -> {
            if (threadSafetyEnabled) {
                return mutex.lock(dbg, Duration.ofMillis(expiryRemainingMillis()));
            }
            return Mono.empty();
        });
    }

    private Mono<Void> unlock(ReactiveLock.Waiter lockToken, String dbgExtra, boolean removeFromWaiters) {
        return Mono.defer(() -> {
            if (threadSafetyEnabled) {
                return mutex.unlock(lockToken, dbgExtra, removeFromWaiters);
            }
            return Mono.empty();
        });
    }

    private Mono<Void> unlock(ReactiveLock.Waiter lockToken, String dbgExtra) {
        return unlock(lockToken, dbgExtra, false);
    }

    private Mono<LockTokens> lockAndIncKVOps(String dbg) {
        return lock(dbg)
                .flatMap(lt -> kvOps.add(dbg)
                        .map(opsToken -> new LockTokens(lt, opsToken)));
    }

    private Mono<ReactiveLock.Waiter> waitForAllKVOpsThenLock(String dbg) {
        return Mono.fromRunnable(() -> {
                    assertNotLocked(dbg);
                    logger().info(attemptId, "waiting for %d KV ops finish for %s",
                            kvOps.waitingCount(), dbg);
                })
                .then(Mono.defer(() -> {
                    if (threadSafetyEnabled) {
                        return kvOps.await(Duration.ofMillis(expiryRemainingMillis()));
                    }
                    return Mono.empty();
                }))
                .then(lock(dbg))
                .flatMap(lockToken -> {
                    if (kvOps.waitingCount() > 0) {
                        return unlock(lockToken, dbg + " still waiting for KV ops")
                                .then(waitForAllKVOpsThenLock(dbg + " still waiting for KV ops"));
                    }
                    return Mono.just(lockToken);
                });
    }

    private Mono<Void> waitForAllOpsThenDoUnderLock(String dbg,
                                                    @Nullable SpanWrapper span,
                                                    Supplier<Mono<Void>> doUnderLock) {
        return Mono.defer(() -> {
            return waitForAllOps(dbg)
                    .then(lock(dbg))
                    .flatMap(lockToken -> {
                        // Because there is a race between waiting for all ops and getting a lock/setting-isDone (which will block other ops).
                        if (kvOps.waitingCount() > 0) {
                            return unlock(lockToken, dbg + " still waiting for ops")
                                    .then(waitForAllOpsThenDoUnderLock(dbg + " still waiting for ops", span, doUnderLock));
                        }
                        return doUnderLock.get()
                                .then(unlock(lockToken, "after doUnderLock"))
                                .onErrorResume(err -> unlock(lockToken, "onError doUnderLock")
                                        .then(Mono.error(err)));
                    })
                    .doOnError(err -> span.span().status(RequestSpan.StatusCode.ERROR));
        });
    }

    private Mono<Void> waitForAllOps(String dbg) {
        return Mono.fromRunnable(() -> {
                    assertNotLocked(dbg);
                    logger().info(attemptId, "waiting for %d KV ops in %s", kvOps.waitingCount(), dbg);
                })
                .then(Mono.defer(() -> {
                    if (threadSafetyEnabled) {
                        return kvOps.await(Duration.ofMillis(expiryRemainingMillis()));
                    }
                    return Mono.empty();
                }));
    }

    /**
     * Mutates the specified <code>doc</code> with new content, using the
     * document's last {@link CoreTransactionGetResult#cas()}.
     *
     * @param doc     the doc to be mutated
     * @param content the content to replace the doc with
     * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
     * object is modified.
     */
    public Mono<CoreTransactionGetResult> replace(CoreTransactionGetResult doc, byte[] content, SpanWrapper pspan) {
        return doKVOperation("replace " + DebugUtil.docId(doc), pspan, CoreTransactionAttemptContextHooks.HOOK_REPLACE, doc.collection(), doc.id(),
                (operationId, span, lockToken) -> replaceInternalLocked(operationId, doc, content, span, lockToken));
    }

    private <T> Mono<T> createMonoBridge(String debug, Mono<T> internal) {
        if (threadSafetyEnabled) {
            return new MonoBridge<>(internal, debug, this, monoBridgeDebugging ? LOGGER : null).external();
        }

        return internal;
    }

    /**
     * Provide generic support functionality around KV operations (including those done in queryMode), including locking.
     * <p>
     * The callback is required to unlock the mutex iff it returns without error. This method will handle unlocking on errors.
     */
    private <T> Mono<T> doKVOperation(String lockDebugOrig, SpanWrapper span, String stageName, CollectionIdentifier docCollection, String docId,
                                      TriFunction<String, SpanWrapper, ReactiveLock.Waiter, Mono<T>> op) {
        return createMonoBridge(lockDebugOrig, Mono.defer(() -> {
            String operationId = UUID.randomUUID().toString();
            // If two operations on the same doc are done concurrently it can be unclear, so include a partial of the operation id
            String lockDebug = lockDebugOrig + " - " + operationId.substring(0, TransactionLogEvent.CHARS_TO_LOG);
            SpanWrapperUtil.setAttributes(span, this, docCollection, docId);
            // We don't attach the opid to the span, it's too low cardinality to be useful

            return lockAndIncKVOps(lockDebug)

                    // In case the user has selected some scheduler inside the lambda, move to our own
                    .subscribeOn(scheduler())

                    .flatMap(lockTokens ->
                            Mono.defer(() -> {
                                TransactionOperationFailedException returnEarly = canPerformOperation(lockDebug);
                                if (returnEarly != null) {
                                    return Mono.error(returnEarly);
                                }

                                if (hasExpiredClientSide(stageName, Optional.of(docId))) {
                                    LOGGER.info(attemptId, "has expired in stage %s, setting expiry-overtime-mode", stageName);

                                    // Combo of setting this mode and throwing AttemptExpired will result in an attempt to
                                    // rollback, which will ignore expiries, and bail out if anything fails
                                    expiryOvertimeMode = true;

                                    return Mono.error(operationFailed(createError()
                                            .cause(new AttemptExpiredException("Attempt expired in stage " + stageName))
                                            .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                                            .build()));
                                }

                                return Mono.empty();
                            })

                                    // The callback is required to unlock the mutex iff it returns without error. This
                                    // method will handle unlocking on errors.
                                    .then(op.apply(operationId, span, lockTokens.mutexToken))

                                    .doFinally(v -> {
                                        if (v == SignalType.CANCEL || v == SignalType.ON_ERROR) {
                                            LOGGER.info(attemptId, "doKVOperation %s got signal %s", lockDebug, v);

                                            // Only unlock in error cases as normal flow should unlock.  This saves an unnecessary synchronized.
                                            // removeFromWaiters on CANCEL because this operation is dying and does not want to be given the lock
                                            // (CANCEL shouldn't happen with MonoBridge anymore, but leaving in-place just in case)
                                            // Yes it's not great to block inside a reactive operator, but this should be very short-lived, and lets us centralise finally logic
                                            // Note it's essential to not block() on the parallel scheduler as it causes onErrorDropped.
                                            // Parallel scheduler should not be used, it's a bug if so.
                                            unlock(lockTokens.mutexToken, "doKVOperation", v == SignalType.CANCEL).block();
                                        }
                                        kvOps.done(lockTokens.waitGroupToken).block();
                                    }))

                    .doOnError(err -> span.setErrorStatus());
        }));
    }

    /**
     * Doesn't need everything from doKVOperation, as queryWrapper already centralises a lot of logic
     */
    public <T> Mono<T> doQueryOperation(String lockDebugIn, String statement, @Nullable SpanWrapper pspan, TriFunction<Integer, AtomicReference<ReactiveLock.Waiter>, SpanWrapper, Mono<T>> op) {
        return Mono.defer(() -> {
            int sidx = queryStatementIdx.getAndIncrement();
            String lockDebug = lockDebugIn + " q" + sidx;
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_QUERY, pspan)
                    .attribute(TracingIdentifiers.ATTR_STATEMENT, statement);

            return createMonoBridge(lockDebug, Mono.defer(() -> {
                AtomicReference<ReactiveLock.Waiter> lt = new AtomicReference<>();

                return lock(lockDebug)

                        // In case the user has selected some scheduler inside the lambda, move to our own
                        .subscribeOn(scheduler())

                        .flatMap(lockToken -> {
                            // Query is done under lock, except for BEGIN WORK which needs to relock.  Since this changes the lock
                            // and needs to hold it after, we use an AtomicReference to unlock the correct lock here.
                            // *WithQuery KV operations do similar.
                            lt.set(lockToken);
                            return op.apply(sidx, lt, span)

                                    .doFinally(v -> {
                                        if (v == SignalType.CANCEL || v == SignalType.ON_ERROR) {
                                            LOGGER.info(attemptId, "doQueryOperation %s got signal %s", lockDebug, v);
                                        }
                                        // Yes it's not great to block inside a reactive operator, but this should be very short-lived, and there is no onCancelResume

                                        // removeFromWaiters on CANCEL because this operation is dying and does not want to be given the lock
                                        // CANCEL shouldn't happen with MonoBridge anymore, keeping it just in case.
                                        // Unlike doKVOperation, for query we hold the lock for the duration (BEGIN WORK excluded), so always unlock
                                        unlock(lt.get(), "doQueryOperation", v == SignalType.CANCEL).block();
                                    });
                        });
            })).doOnError(err -> span.finishWithErrorStatus())
                    .doOnNext(ignored -> span.finish());
        });
    }

    private Mono<CoreTransactionGetResult> replaceInternalLocked(String operationId,
                                                                 CoreTransactionGetResult doc,
                                                                 byte[] content,
                                                                 SpanWrapper pspan,
                                                                 ReactiveLock.Waiter lockToken) {
        LOGGER.info(attemptId, "replace doc %s, operationId = %s", doc, operationId);

        if (queryModeLocked()) {
            return replaceWithQueryLocked(doc, content, lockToken, pspan);
        } else {
            return replaceWithKVLocked(operationId, doc, content, pspan, lockToken);
        }
    }

    private Mono<CoreTransactionGetResult> replaceWithKVLocked(String operationId, CoreTransactionGetResult doc, byte[] content, SpanWrapper pspan, ReactiveLock.Waiter lockToken) {
        Optional<StagedMutation> existing = findStagedMutationLocked(doc);
        boolean mayNeedToWriteATR = state == AttemptState.NOT_STARTED;

        return hooks.beforeUnlockReplace.apply(this, doc.id()) // testing hook

                .then(unlock(lockToken, "standard"))

                .then(Mono.defer(() -> {
                    if (existing.isPresent()) {
                        StagedMutation op = existing.get();

                        if (op.type == StagedMutationType.REMOVE) {
                            return Mono.error(operationFailed(createError()
                                    .cause(new DocumentNotFoundException(null))
                                    .build()));
                        }

                        // StagedMutationType.INSERT handling is below
                    }

                    return checkAndHandleBlockingTxn(doc, pspan, ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_REPLACING, existing)

                            .then(initATRIfNeeded(mayNeedToWriteATR, doc.collection(), doc.id(), pspan))

                            .then(Mono.defer(() -> {
                                if (existing.isPresent() && existing.get().type == StagedMutationType.INSERT) {
                                    return createStagedInsert(operationId, doc.collection(), doc.id(), content, pspan,
                                            Optional.of(doc.cas()));
                                } else {
                                    return createStagedReplace(operationId, doc.collection(), doc.id(), doc.cas(),
                                            doc.documentMetadata(), doc.crc32OfGet(), content, doc.contentAsBytes(), pspan, doc.links().isDeleted());
                                }
                            }));
                }));
    }

    private Mono<Void> initAtrIfNeededLocked(CollectionIdentifier docCollection,
                               String docId,
                               SpanWrapper pspan) {
        return Mono.defer(() -> {
            if (state == AttemptState.NOT_STARTED) {
                return Mono.fromCallable(() -> selectAtrLocked(docCollection, docId))
                        .flatMap(atrCollection -> atrPendingLocked(atrCollection, pspan))
                        .then();
            } else {
                return Mono.empty();
            }
        });
    }

    private Mono<Void> initATRIfNeeded(boolean mayNeedToWriteATR,
                                       CollectionIdentifier docCollection,
                                       String docId,
                                       SpanWrapper pspan) {
        return Mono.defer(() -> {
            if (mayNeedToWriteATR) {
                return doUnderLock("before ATR " + DebugUtil.docId(docCollection, docId),
                        () -> initAtrIfNeededLocked(docCollection, docId, pspan));
            }

            return Mono.empty();
        });
    }

    private CollectionIdentifier selectAtrLocked(CollectionIdentifier docCollection, String docId) {
        if (atrId.isPresent()) {
            throw new IllegalStateException("Internal bug: two operations have concurrently initialised the ATR");
        }

        long vbucketIdForDoc = ActiveTransactionRecordIds.vbucketForKey(docId, ActiveTransactionRecordIds.NUM_VBUCKETS);
        String atr = randomAtrIdForVbucket(this, (int) vbucketIdForDoc, config.numAtrs());
        atrId = Optional.of(atr);
        if (config.metadataCollection().isPresent()) {
            atrCollection = Optional.of(config.metadataCollection().get());
        } else {
            atrCollection = Optional.of(getAtrCollection(docCollection));
        }

        LOGGER.info(attemptId, "First mutated doc in txn is '%s' on vbucket %d, so using atr %s",
                DebugUtil.docId(docCollection, docId), vbucketIdForDoc, atr);

        return atrCollection.get();
    }

    private Mono<CoreTransactionGetResult> replaceWithQueryLocked(CoreTransactionGetResult doc, byte[] content, ReactiveLock.Waiter lockToken, SpanWrapper span) {
        return Mono.defer(() -> {
            ObjectNode txData = makeTxdata();
            txData.put("scas", Long.toString(doc.cas()));
            doc.txnMeta().ifPresent(v -> txData.set("txnMeta", v));
            ArrayNode params = Mapper.createArrayNode()
                    .add(makeKeyspace(doc.collection()))
                    .add(doc.id())
                    .addRawValue(new RawValue(new String(content, StandardCharsets.UTF_8)))
                    .add(content)
                    .add(Mapper.createObjectNode());

            int sidx = queryStatementIdx.getAndIncrement();
            AtomicReference<ReactiveLock.Waiter> lt = new AtomicReference<>(lockToken);

            ObjectNode queryOptions = Mapper.createObjectNode();
            queryOptions.set("args", params);
            
            return queryWrapperBlockingLocked(sidx,
                    null,
                    null,
                    "EXECUTE __update",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_REPLACE,
                    false,
                    true,
                    txData,
                    params,
                    span,
                    false,
                    lt,
                    true)

                    .flatMap(result -> unlock(lt.get(), "replaceWithQueryLocked end", false)
                            .thenReturn(result.rows))

                    .map(rows -> {
                        if (rows.isEmpty()) {
                            throw operationFailed(TransactionOperationFailedException.Builder.createError()
                                    .cause(new IllegalStateException("Did not get any rows back while KV replacing with query"))
                                    .build());
                        }

                        ObjectNode row;
                        try {
                            row = Mapper.reader().readValue(rows.get(0).data(), ObjectNode.class);
                        } catch (IOException e) {
                            throw new DecodingFailureException(e);
                        }
                        String scas = row.path("scas").textValue();
                        long cas = Long.parseLong(scas);
                        JsonNode updatedDoc = row.path("doc");
                        Optional<String> crc32 = Optional.ofNullable(row.path("crc32").textValue());

                        return new CoreTransactionGetResult(doc.id(),
                                content,
                                cas,
                                doc.collection(),
                                null,
                                Optional.empty(),
                                Optional.empty(),
                                crc32);
                    })

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().cause(err);
                        span.recordExceptionAndSetErrorStatus(err);

                        if (err instanceof TransactionOperationFailedException) {
                            return Mono.error(err);
                        }
                        else if (ec == FAIL_DOC_NOT_FOUND
                                || ec == FAIL_CAS_MISMATCH) {
                            TransactionOperationFailedException out = operationFailed(builder.retryTransaction().build());
                            return Mono.error(out);
                        }
                        else {
                            TransactionOperationFailedException out = operationFailed(builder.build());
                            return Mono.error(out);
                        }
                    });
        });
    }

    private Mono<Void> removeWithQueryLocked(CoreTransactionGetResult doc, ReactiveLock.Waiter lockToken, SpanWrapper span) {
        return Mono.defer(() -> {
            ObjectNode txData = makeTxdata();
            txData.put("scas", Long.toString(doc.cas()));
            doc.txnMeta().ifPresent(v -> txData.set("txnMeta", v));
            ArrayNode params = Mapper.createArrayNode()
                    .add(makeKeyspace(doc.collection()))
                    .add(doc.id())
                    .add(Mapper.createObjectNode());

            int sidx = queryStatementIdx.getAndIncrement();
            AtomicReference<ReactiveLock.Waiter> lt = new AtomicReference<>(lockToken);

            ObjectNode queryOptions = Mapper.createObjectNode();
            queryOptions.set("args", params);

            return queryWrapperBlockingLocked(sidx,
                    null,
                    null,
                    "EXECUTE __delete",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_REMOVE,
                    false,
                    true,
                    txData,
                    params,
                    span,
                    false,
                    lt,
                    true)
                    .flatMap(result -> unlock(lt.get(), "removeWithQueryLocked end", false))

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().cause(err);
                        span.recordExceptionAndSetErrorStatus(err);

                        if (err instanceof TransactionOperationFailedException) {
                            return Mono.error(err);
                        }
                        else if (ec == FAIL_DOC_NOT_FOUND
                                || ec == FAIL_CAS_MISMATCH) {
                            TransactionOperationFailedException out = operationFailed(builder.retryTransaction().build());
                            return Mono.error(out);
                        }
                        else {
                            TransactionOperationFailedException out = operationFailed(builder.build());
                            return Mono.error(out);
                        }
                    });
        });
    }

    private Mono<Void> forwardCompatibilityCheck(ForwardCompatibilityStage stage,
                                                 Optional<ForwardCompatibility> fc) {
        return ForwardCompatibility.check(core, stage, fc, logger(), Supported.SUPPORTED)
                .onErrorResume(err -> {
                    TransactionOperationFailedException.Builder error = createError()
                            .cause(new ForwardCompatibilityFailureException());
                    if (err instanceof ForwardCompatibilityRequiresRetryException) {
                        error.retryTransaction();
                    }
                    return Mono.error(operationFailed(error.build()));
                });
    }

    private Mono<Void> checkATREntryForBlockingDocInternal(CoreTransactionGetResult doc,
                                                           CollectionIdentifier collection,
                                                           SpanWrapper span,
                                                           MeteringUnits.MeteringUnitsBuilder units) {
        return Mono.fromRunnable(() -> {
            checkExpiryPreCommitAndSetExpiryOvertimeMode("staging.check_atr_entry_blocking_doc", Optional.empty());
        })

                .then(hooks.beforeCheckATREntryForBlockingDoc.apply(this, doc.links().atrId().get()))

                .then(ActiveTransactionRecord.findEntryForTransaction(core, collection, doc.links().atrId().get(),
                        doc.links().stagedAttemptId().get(),
                        config,
                        span,
                        logger(),
                        units).flatMap(atrEntry -> {
                    if (atrEntry.isPresent()) {
                        ActiveTransactionRecordEntry ae = atrEntry.get();

                        LOGGER.info(attemptId, "fetched ATR entry for blocking txn: hasExpired=%s entry=%s",
                                ae.hasExpired(), ae);

                        return forwardCompatibilityCheck(ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_READING_ATR, ae.forwardCompatibility())

                                .then(Mono.defer(() -> {

                                    switch (ae.state()) {
                                        case COMPLETED:
                                        case ROLLED_BACK:
                                            LOGGER.info(attemptId, "ATR entry state of %s indicates we can proceed to overwrite",
                                                    atrEntry.get().state());

                                            return Mono.empty();
                                        default:
                                            return Mono.error(new RetryOperationException());
                                    }
                                }));
                    } else {
                        LOGGER.info(attemptId,
                                "blocking txn %s's entry has been removed indicating the txn expired, so proceeding " +
                                        "to overwrite",
                                doc.links().stagedAttemptId().get());

                        return Mono.empty();
                    }
                }))

                .retryWhen(Retry.anyOf(RetryOperationException.class)
                        // Produces 5 reads per second when blocked
                        .exponentialBackoff(Duration.ofMillis(50), Duration.ofMillis(500))
                        .timeout(Duration.ofSeconds(1))
                        .toReactorRetry())
                .publishOn(scheduler()) // after retryWhen triggers, it's on parallel scheduler

                .onErrorResume(err -> {
                    if (err instanceof RetryExhaustedException) {
                        LOGGER.info(attemptId, "still blocked by a valid transaction, retrying to unlock documents");
                    }
                    else if (err instanceof DocumentNotFoundException) {
                        LOGGER.info(attemptId, "blocking txn's ATR has been removed so proceeding to overwrite");
                        return Mono.empty();
                    }
                    else {
                        LOGGER.warn(attemptId, "got error in checkATREntryForBlockingDoc: %s", dbg(err));
                    }

                    return Mono.error(operationFailed(createError()
                            .cause(err)
                            .retryTransaction()
                            .build()));
                })

                .then();
    }

    private Mono<Void> checkATREntryForBlockingDoc(CoreTransactionGetResult doc, SpanWrapper pspan) {
        return Mono.defer(() -> {
            CollectionIdentifier collection = doc.links().collection();
            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

            return checkATREntryForBlockingDocInternal(doc, collection, pspan, units)
                    .doOnTerminate(() -> addUnits(units.build()));
        });
    }

    private RedactableArgument getAtrDebug(CollectionIdentifier collection, Optional<String> atrId) {
        return ActiveTransactionRecordUtil.getAtrDebug(collection, atrId.orElse("-"));
    }

    private RedactableArgument getAtrDebug(Optional<CollectionIdentifier> collection, Optional<String> atrId) {
        return ActiveTransactionRecordUtil.getAtrDebug(collection, atrId);
    }

    long expiryRemainingMillis() {
        long nowNanos = System.nanoTime();
        long expiredMillis = overall.timeSinceStartOfTransactionsMillis(nowNanos);
        long remainingMillis = config.expirationTime().toMillis() - expiredMillis;

        // This bounds the value to [0-expirationTime].  It should always be in this range, this is just to protect
        // against the application clock changing.
        return Math.max(Math.min(remainingMillis, config.expirationTime().toMillis()), 0);
    }

    private RequestTracer tracer() {
        // Will go to the ThresholdRequestTracer by default.  In future, may want our own default tracer.
        return core.context().environment().requestTracer();
    }

    private byte[] serialize(Object in) {
        try {
            return Mapper.writer().writeValueAsBytes(in);
        } catch (JsonProcessingException e) {
            throw new DecodingFailureException(e);
        }
    }

    private Mono<Void> atrPendingLocked(CollectionIdentifier collection, SpanWrapper pspan) {
        return Mono.defer(() -> {
            assertLocked("atrPending");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_PENDING, pspan);

            String prefix = "attempts." + attemptId;

            if (!atrId.isPresent()) return Mono.error(new IllegalStateException("atrId not present"));

            return Mono.defer(() -> {
                LOGGER.info(attemptId, "about to set ATR %s to Pending", getAtrDebug(collection, atrId));
                return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_PENDING, Optional.empty());
            })

                    .then(hooks.beforeAtrPending.apply(this)) // Testing hook

                    .then(TransactionKVHandler.mutateIn(core, collection, atrId.get(), kvTimeoutMutating(),
                                    false, true, false, false, false, 0, durabilityLevel(), OptionsUtil.createClientContext("atrPending"), span,
                                    Arrays.asList(
                                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_TRANSACTION_ID, serialize(transactionId()), true, true, false, 0),
                                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_STATUS, serialize(AttemptState.PENDING.name()), false, true, false, 1),
                                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_START_TIMESTAMP, serialize("${Mutation.CAS}"), false, true, true, 2),
                                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_EXPIRES_AFTER_MILLIS, serialize(expiryRemainingMillis()), false, true, false, 3),
                                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_DURABILITY_LEVEL, serialize(DurabilityLevelUtil.convertDurabilityLevel(config.durabilityLevel())), false, true, false, 3),
                                            new SubdocMutateRequest.Command(SubdocCommandType.SET_DOC, "", new byte[]{0}, false, false, false, 4)
                                            ), logger()))

                    .publishOn(scheduler())

                    // Testing hook
                    .flatMap(v -> hooks.afterAtrPending.apply(this).map(x -> v))

                    .doOnNext(v -> {
                        long elapsed = span.elapsedMicros();
                        addUnits(v.flexibleExtras());
                        LOGGER.info(attemptId, "set ATR %s to Pending in %dus%s", getAtrDebug(collection, atrId), elapsed,
                                DebugUtil.dbg(v.flexibleExtras()));
                        setStateLocked(AttemptState.PENDING);
                        overall.cleanup().addToCleanupSet(collection);
                    })

                    .then()

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder out = createError().cause(err);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        long elapsed = span.finish(err);

                        LOGGER.info(attemptId, "error while setting ATR %s to Pending%s in %dus: %s",
                                getAtrDebug(collection, atrId), DebugUtil.dbg(units), elapsed, dbg(err));

                        if (expiryOvertimeMode) {
                            return mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_ATR_PENDING, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                        } else if (ec == FAIL_EXPIRY) {
                            return setExpiryOvertimeModeAndFail(err, CoreTransactionAttemptContextHooks.HOOK_ATR_PENDING, ec);
                        } else if (ec == FAIL_ATR_FULL) {
                            return Mono.error(operationFailed(out.cause(new ActiveTransactionRecordFullException(err)).build()));
                        } else if (ec == FAIL_AMBIGUOUS) {
                            LOGGER.info(attemptId, "retrying the op on %s to resolve ambiguity", ec);

                            return Mono.delay(DEFAULT_DELAY_RETRYING_OPERATION, scheduler())
                                    .then(atrPendingLocked(collection, span));
                        } else if (ec == FAIL_PATH_ALREADY_EXISTS) {
                            LOGGER.info(attemptId, "assuming this is caused by resolved ambiguity, and proceeding as though successful", ec);
                            return Mono.empty();
                        } else if (ec == FAIL_TRANSIENT) {
                            LOGGER.info(attemptId, "transient error likely to be solved by retry", ec);
                            return Mono.error(operationFailed(out.retryTransaction().build()));
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(out.doNotRollbackAttempt().build()));
                        } else {
                            return Mono.error(operationFailed(out.build()));
                        }
                    })
                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    private void setStateLocked(AttemptState newState) {
        assertLocked("setState " + newState);
        logger().info(attemptId, "changed state to %s", newState);
        state = newState;
    }


    /*
     * Stage a replace on a document, putting the staged mutation into the doc's xattrs.  The document's content is
     * left unchanged.
     * documentMetadata is optional to handle insert->replace case
     */
    private Mono<CoreTransactionGetResult>
    createStagedReplace(String operationId,
                        CollectionIdentifier collection,
                        String id,
                        long cas,
                        Optional<DocumentMetadata> documentMetadata,
                        Optional<String> crc32OfGet,
                        byte[] contentToStage,
                        byte[] contentOfBody,
                        SpanWrapper pspan,
                        boolean accessDeleted) {
        return Mono.defer(() -> {
            assertNotLocked("createStagedReplace");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_REPLACE_STAGE, pspan);

            byte[] txn = createDocumentMetadata(OperationTypes.REPLACE, operationId, documentMetadata);

            return hooks.beforeStagedReplace.apply(this, id) // test hook

                    .then(TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                            false, false, false, true, false, cas, durabilityLevel(), OptionsUtil.createClientContext("createStagedReplace"), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn", txn, true, true, false, 0),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "txn.op.stgd", contentToStage, false, true, false, 1),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "txn.op.crc32", serialize("${Mutation.value_crc32c}"), false, true, true, 2)
                            )))

                    .publishOn(scheduler())

                    .doOnSubscribe(v -> {
                        LOGGER.info(attemptId, "about to replace doc %s with cas %d, accessDeleted=%s",
                                DebugUtil.docId(collection, id), cas, accessDeleted);
                    })

                    // Testing hook
                    .flatMap(result -> hooks.afterStagedReplaceComplete.apply(this, id).map(x -> result))

                    .doOnNext(updatedDoc -> {
                        long elapsed = span.elapsedMicros();
                        addUnits(updatedDoc.flexibleExtras());
                        LOGGER.info(attemptId, "replaced doc %s%s got cas %s, in %dus",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(updatedDoc.flexibleExtras()), updatedDoc.cas(), elapsed);
                    })

                    // Save the new CAS
                    .flatMap(updatedDoc -> {
                        CoreTransactionGetResult out = createTransactionGetResult(operationId, collection, id, contentToStage,
                                contentOfBody, updatedDoc.cas(), documentMetadata, OperationTypes.REPLACE, crc32OfGet);
                        return supportsReplaceBodyWithXattr(collection.bucket())
                                .flatMap(supports -> addStagedMutation(new StagedMutation(operationId, id, collection, updatedDoc.cas(), documentMetadata, crc32OfGet, supports ? null : contentToStage, StagedMutationType.REPLACE))
                                        .thenReturn(out));
                    })

                    .onErrorResume(err -> {
                        return handleErrorOnStagedMutation("replacing", collection, id, err, span, crc32OfGet,
                                (newCas) -> createStagedReplace(operationId, collection, id, newCas, documentMetadata,
                                        crc32OfGet, contentToStage, contentOfBody, pspan, accessDeleted));
                    })

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    /**
     * getWithKv: possibly created from StagedMutation, which may not have content. Could get chained to replace/remove later,
     *     but these don't look at the staged content of the doc
     * createStagedReplace: existing doc, preserve metadata, has body
     * createStagedInsert: new doc, empty contents, all in links
     */
    private CoreTransactionGetResult createTransactionGetResult(String operationId,
                                                                CollectionIdentifier collection,
                                                                String id,
                                                                @Nullable byte[] bodyContent,
                                                                @Nullable byte[] stagedContent,
                                                                long cas,
                                                                Optional<DocumentMetadata> documentMetadata,
                                                                String opType,
                                                                Optional<String> crc32OfFetch) {
        String stagedContentAsStr = stagedContent == null ? null : new String(stagedContent, StandardCharsets.UTF_8);
        TransactionLinks links = new TransactionLinks(
                Optional.ofNullable(stagedContentAsStr),
                atrId,
                atrCollection.map(CollectionIdentifier::bucket),
                atrCollection.flatMap(CollectionIdentifier::scope),
                atrCollection.flatMap(CollectionIdentifier::collection),
                Optional.of(transactionId()),
                Optional.of(attemptId),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(opType),
                true,
                Optional.empty(),
                Optional.empty(),
                Optional.of(operationId)
        );
        CoreTransactionGetResult out = new CoreTransactionGetResult(id,
                bodyContent,
                cas,
                collection,
                links,
                documentMetadata,
                Optional.empty(),
                crc32OfFetch
        );

        return out;
    }

    private byte[] createDocumentMetadata(String opType,
                                          String operationId,
                                          Optional<DocumentMetadata> documentMetadata) {

        ObjectNode op = Mapper.createObjectNode();
        op.put("type", opType);

        ObjectNode ret = Mapper.createObjectNode();
        ret.set("id", Mapper.createObjectNode()
                .put("txn", transactionId())
                .put("atmpt", attemptId)
                .put("op", operationId));
        ret.set("atr", Mapper.createObjectNode()
                .put("id", atrId.get())
                .put("bkt", atrCollection.get().bucket())
                .put("scp", atrCollection.get().scope().orElse(DEFAULT_SCOPE))
                .put("coll", atrCollection.get().collection().orElse(DEFAULT_COLLECTION)));
        ret.set("op", op);

        ObjectNode restore = Mapper.createObjectNode();

        documentMetadata.map(DocumentMetadata::cas).ifPresent(v -> restore.put("CAS", v));
        documentMetadata.map(DocumentMetadata::exptime).ifPresent(v -> restore.put("exptime", v));
        documentMetadata.map(DocumentMetadata::revid).ifPresent(v -> restore.put("revid", v));

        if (restore.size() > 0) {
            ret.set("restore", restore);
        }

        try {
            return Mapper.writer().writeValueAsBytes(ret);
        } catch (JsonProcessingException e) {
            throw new DecodingFailureException(e);
        }
    }

    /*
     * Stage a remove on a document.  This involves putting a marker into the document's xattrs.  The document's
     * content is left unchanged.
     */
    private Mono<Void> createStagedRemove(String operationId, CoreTransactionGetResult doc, long cas, SpanWrapper pspan, boolean accessDeleted) {
        return Mono.defer(() -> {
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), doc.collection(), doc.id(), TracingIdentifiers.TRANSACTION_OP_REMOVE_STAGE, pspan);

            LOGGER.info(attemptId, "about to remove doc %s with cas %d", DebugUtil.docId(doc), cas);

            byte[] txn = createDocumentMetadata(OperationTypes.REMOVE, operationId, doc.documentMetadata());

            return hooks.beforeStagedRemove.apply(this, doc.id()) // testing hook

                    .then(TransactionKVHandler.mutateIn(core, doc.collection(), doc.id(), kvTimeoutMutating(),
                            false, false, false, accessDeleted, false, cas, durabilityLevel(), OptionsUtil.createClientContext("createStagedReplace"), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn", txn, true, true, false, 0),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "txn.op.crc32", serialize("${Mutation.value_crc32c}"), false, true, true, 2)
                            )))

                    .publishOn(scheduler())

                    .flatMap(updatedDoc -> hooks.afterStagedRemoveComplete.apply(this, doc.id()) // Testing hook
                            .thenReturn(updatedDoc))

                    .flatMap(response -> {
                        long elapsed = span.elapsedMicros();
                        addUnits(response.flexibleExtras());
                        LOGGER.info(attemptId, "staged remove of doc %s%s got cas %d, in %dus",
                                DebugUtil.docId(doc), DebugUtil.dbg(response.flexibleExtras()), response.cas(), elapsed);
                        // Save so the staged mutation can be committed/rolled back with the correct CAS
                        doc.cas(response.cas());
                        return addStagedMutation(new StagedMutation(operationId, doc.id(), doc.collection(), doc.cas(),
                                doc.documentMetadata(), doc.crc32OfGet(), null, StagedMutationType.REMOVE));
                    })

                    .then()

                    .onErrorResume(err -> handleErrorOnStagedMutation("removing", doc.collection(), doc.id(), err, span, doc.crc32OfGet(),
                            (newCas) -> createStagedRemove(operationId, doc, newCas, span, accessDeleted)))

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    private Mono<Void> doUnderLock(String dbg,
                                   Supplier<Mono<Void>> whileLocked) {
            return lock(dbg)
                    .flatMap(lockToken -> Mono.defer(() -> whileLocked.get())
                            .doFinally(v -> {
                                unlock(lockToken, "doUnderLock on signal " + v).block();
                            }));
    }

    private Mono<Void> addStagedMutation(StagedMutation sm) {
        return Mono.defer(() -> {
            return doUnderLock("addStagedMutation " + DebugUtil.docId(sm.collection, sm.id),
                    () -> Mono.fromRunnable(() -> {
                        removeStagedMutationLocked(sm.collection, sm.id);
                        stagedMutationsLocked.add(sm);
                    }));
        });
    }

    private <T> Mono<T> handleErrorOnStagedMutation(String stage, CollectionIdentifier collection, String id, Throwable err, SpanWrapper pspan,
                                                Optional<String> crc32FromGet, Function<Long, Mono<T>> callback) {
        ErrorClass ec = classify(err);
        TransactionOperationFailedException.Builder out = createError().cause(err);
        MeteringUnits units = addUnits(MeteringUnits.from(err));

        LOGGER.info(attemptId, "error while %s doc %s%s in %dus: %s",
                stage, DebugUtil.docId(collection, id), DebugUtil.dbg(units), pspan.elapsedMicros(), dbg(err));

        if (expiryOvertimeMode) {
            LOGGER.warn(attemptId, "should not reach here in expiryOvertimeMode");
        }

        if (ec == FAIL_EXPIRY) {
            return setExpiryOvertimeModeAndFail(err, stage, ec);
        } else if (ec == FAIL_CAS_MISMATCH) {
            return handleDocChangedDuringStaging(pspan, id, collection, crc32FromGet, callback);
        } else if (ec == FAIL_DOC_NOT_FOUND) {
            return Mono.error(operationFailed(createError().retryTransaction().build()));
        } else if (ec == FAIL_AMBIGUOUS || ec == FAIL_TRANSIENT) {
            return Mono.error(operationFailed(out.retryTransaction().build()));
        } else if (ec == FAIL_HARD) {
            return Mono.error(operationFailed(out.doNotRollbackAttempt().build()));
        } else {
            return Mono.error(operationFailed(out.build()));
        }

        // Note: all paths must return error. Logic in createStagedReplace depends on this (passes null otherwise which
        // will fail).
    }

    private Optional<StagedMutation> findStagedMutationLocked(CoreTransactionGetResult doc) {
        return findStagedMutationLocked(doc.collection(), doc.id());
    }

    private Optional<StagedMutation> findStagedMutationLocked(CollectionIdentifier collection, String docId) {
        assertLocked("findStagedMutation");
        return stagedMutationsLocked.stream()
                .filter(v -> v.collection.equals(collection) && v.id.equals(docId))
                .findFirst();
    }

    private void removeStagedMutationLocked(CollectionIdentifier collection, String id) {
        assertLocked("removeStagedMutation");
        stagedMutationsLocked.removeIf(v -> v.collection.equals(collection) && v.id.equals(id));
    }

    private static LogDeferThrowable dbg(Throwable err) {
        return DebugUtil.dbg(err);
    }

    private Mono<CoreTransactionGetResult> handleDocExistsDuringStagedInsert(String operationId,
                                                                             CollectionIdentifier collection,
                                                                             String id,
                                                                             byte[] content,
                                                                             SpanWrapper pspan) {
        String bp = "DocExists on " + DebugUtil.docId(collection, id) + ": ";
        MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

        return hooks.beforeGetDocInExistsDuringStagedInsert.apply(this, id) // testing hook

                .then(DocumentGetter.justGetDoc(core, collection, id, kvTimeoutNonMutating(),  pspan, true, logger(), units))

                .publishOn(scheduler())

                .doOnSubscribe(x -> LOGGER.info(attemptId, "%s getting doc (which may be a tombstone)", bp))

                .onErrorResume(err -> {
                    addUnits(units.build());
                    ErrorClass ec = classify(err);
                    TransactionOperationFailedException.Builder e = createError().cause(err);

                    LOGGER.warn(attemptId, "%s got error while getting doc: %s", bp, dbg(err));

                    // FAIL_DOC_NOT_FOUND case is handled by the ifPresent() check below
                    if (ec == FAIL_TRANSIENT || ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                        e.retryTransaction();
                    }

                    return Mono.error(operationFailed(e.build()));
                })

                .flatMap(v -> {
                    if (v.isPresent()) {
                        Tuple2<CoreTransactionGetResult, SubdocGetResponse> results = v.get();
                        CoreTransactionGetResult r = results.getT1();
                        SubdocGetResponse lir = results.getT2();
                        MeteringUnits built = addUnits(units.build());

                        LOGGER.info(attemptId, "%s doc %s exists inTransaction=%s isDeleted=%s%s",
                                bp, DebugUtil.docId(collection, id), r.links(), lir.isDeleted(), DebugUtil.dbg(built));

                        return forwardCompatibilityCheck(ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_INSERTING_GET, r.links().forwardCompatibility())

                                .then(Mono.defer(() -> {
                                    if (lir.isDeleted() && !r.links().isDocumentInTransaction()) {
                                        LOGGER.info(attemptId, "%s doc %s is a regular tombstone without txn metadata, proceeding to overwrite",
                                                bp, DebugUtil.docId(collection, id));

                                        return createStagedInsert(operationId, collection, id, content, pspan, Optional.of(r.cas()));
                                    } else if (!r.links().isDocumentInTransaction()) {
                                        LOGGER.info(attemptId, "%s doc %s exists but is not in txn, raising " +
                                                "DocumentExistsException", bp, DebugUtil.docId(collection, id));

                                        return Mono.error(new DocumentExistsException(ReducedKeyValueErrorContext.create(id)));
                                    } else {
                                        if (r.links().stagedAttemptId().get().equals(attemptId)) {
                                            // stagedOperationId must be present as this transaction is writing it
                                            if (r.links().stagedOperationId().isPresent() && r.links().stagedOperationId().get().equals(operationId)) {
                                                LOGGER.info(attemptId, "%s doc %s has the same operation id, must be a resolved ambiguity, proceeding",
                                                        bp, DebugUtil.docId(collection, id));

                                                return addStagedMutation(new StagedMutation(operationId, r.id(), r.collection(), r.cas(),
                                                        r.documentMetadata(), r.crc32OfGet(), r.links().stagedContent().get().getBytes(StandardCharsets.UTF_8), StagedMutationType.INSERT))
                                                        .thenReturn(r);
                                            }

                                            LOGGER.info(attemptId, "%s doc %s has the same attempt id but a different operation id, must be racing with a concurrent attempt to write the same doc",
                                                    bp, DebugUtil.docId(collection, id));

                                            return Mono.error(operationFailed(createError()
                                                    .cause(new ConcurrentOperationsDetectedOnSameDocumentException())
                                                    .build()));
                                        }

                                        // BF-CBD-3787
                                        if (!r.links().op().get().equals(OperationTypes.INSERT)) {
                                            LOGGER.info(attemptId, "%s doc %s is in a txn but is not a staged insert, raising " +
                                                    "DocumentExistsException", bp, DebugUtil.docId(collection, id));

                                            return Mono.error(new DocumentExistsException(ReducedKeyValueErrorContext.create(id)));
                                        }

                                        // Will return Mono.empty if it's safe to overwrite, Mono.error otherwise
                                        else return checkAndHandleBlockingTxn(r, pspan, ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_INSERTING, Optional.empty())

                                                .then(overwriteStagedInsert(operationId, collection, id, content, pspan, bp, r, lir));
                                    }
                                }));

                    } else {
                        LOGGER.info(attemptId, "%s completed get of %s, could not find, throwing to retry txn which " +
                                "should succeed now", bp, DebugUtil.docId(collection, id));
                        return Mono.error(operationFailed(createError()
                                .retryTransaction()
                                .build()));
                    }
                });
    }

    private Mono<CoreTransactionGetResult> overwriteStagedInsert(String operationId,
                                                                 CollectionIdentifier collection,
                                                                 String id,
                                                                 byte[] content,
                                                                 SpanWrapper pspan,
                                                                 String bp,
                                                                 CoreTransactionGetResult r,
                                                                 SubdocGetResponse lir) {
        return Mono.defer(() -> {
            CbPreconditions.check(r.links().isDocumentInTransaction());
            CbPreconditions.check(r.links().op().get().equals(OperationTypes.INSERT));

            if (lir.isDeleted()) {
                return createStagedInsert(operationId, collection, id, content, pspan, Optional.of(r.cas()));
            }
            else {
                LOGGER.info(attemptId, "%s removing %s as it's a protocol 1.0 staged insert",
                        bp, DebugUtil.docId(collection, id));

                return hooks.beforeOverwritingStagedInsertRemoval.apply(this, id)

                        .then(TransactionKVHandler.remove(core, collection, id, kvTimeoutMutating(), lir.cas(), durabilityLevel(),
                                OptionsUtil.createClientContext("overwriteStagedInsert"), pspan))

                        .doOnNext(v -> addUnits(v.flexibleExtras()))

                        .onErrorResume(err -> {
                            MeteringUnits units = addUnits(MeteringUnits.from(err));

                            LOGGER.warn(attemptId, "%s hit error %s while removing %s%s",
                                    bp, DebugUtil.dbg(err), DebugUtil.docId(collection, id), DebugUtil.dbg(units));

                            ErrorClass ec = classify(err);
                            TransactionOperationFailedException.Builder out = createError().cause(err);

                            if (ec == FAIL_DOC_NOT_FOUND || ec == FAIL_CAS_MISMATCH || ec == FAIL_TRANSIENT) {
                                out.retryTransaction();
                            }

                            return Mono.error(operationFailed(out.build()));
                        })

                        .then(createStagedInsert(operationId, collection, id, content, pspan, Optional.empty()));
            }
        });
    }

    private Mono<Boolean> supportsReplaceBodyWithXattr(String bucketName) {
        return BucketConfigUtil.waitForBucketConfig(core, bucketName, Duration.of(expiryRemainingMillis(), ChronoUnit.MILLIS))
                .map(bc -> bc.bucketCapabilities().contains(SUBDOC_REVIVE_DOCUMENT));
    }

    /*
     * Stage an insert on a document, putting the staged mutation into the doc's xattrs.  The document is created
     * with an empty body.
     */
    private Mono<CoreTransactionGetResult> createStagedInsert(String operationId,
                                                              CollectionIdentifier collection,
                                                              String id,
                                                              byte[] content,
                                                              SpanWrapper pspan,
                                                              Optional<Long> cas) {
        return Mono.defer(() -> {
            assertNotLocked("createStagedInsert");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_INSERT_STAGE, pspan);

            byte[] txn = createDocumentMetadata(OperationTypes.INSERT, operationId, Optional.empty());

            return Mono.defer(() -> {
                LOGGER.info(attemptId, "about to insert staged doc %s as shadow document, cas=%s, operationId=%s",
                        DebugUtil.docId(collection, id), cas, operationId);
                return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_CREATE_STAGED_INSERT, Optional.of(id));
            })

                    .then(hooks.beforeStagedInsert.apply(this, id)) // testing hook

                    .then(TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(), !cas.isPresent(),
                                    false, false, true, true, cas.orElse(0L), durabilityLevel(), OptionsUtil.createClientContext("createStagedInsert"), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn", txn, true, true, false, 0),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn.op.stgd", content, false, true, false, 1),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn.op.crc32", serialize("${Mutation.value_crc32c}"), false, true, true, 2)
                            )))

                    .publishOn(scheduler())

                    .flatMap(response -> hooks.afterStagedInsertComplete.apply(this, id).thenReturn(response)) // testing hook

                    .doOnNext(response -> {
                        long elapsed = span.elapsedMicros();
                        addUnits(response.flexibleExtras());
                        LOGGER.info(attemptId, "inserted doc %s%s got cas %d, in %dus",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(response.flexibleExtras()), response.cas(), elapsed);
                    })

                    .flatMap(updatedDoc -> {
                        CoreTransactionGetResult out = CoreTransactionGetResult.createFromInsert(collection,
                                id,
                                content,
                                transactionId(),
                                attemptId,
                                atrId.get(),
                                atrCollection.get().bucket(),
                                atrCollection.get().scope().get(),
                                atrCollection.get().collection().get(),
                                updatedDoc.cas());

                        return supportsReplaceBodyWithXattr(collection.bucket())
                                .flatMap(supports -> addStagedMutation(new StagedMutation(operationId, out.id(), out.collection(), out.cas(),
                                        out.documentMetadata(), Optional.empty(), supports ? null : content, StagedMutationType.INSERT))
                                        .thenReturn(out));
                    })

                    .onErrorResume(err -> {
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        LOGGER.info(attemptId, "got err while staging insert of %s%s: %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(units), dbg(err));

                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder out = createError().cause(err);

                        if (err instanceof FeatureNotAvailableException) {
                            return Mono.error(operationFailed(out.build()));
                        }
                        else {
                            if (expiryOvertimeMode) {
                                return mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_CREATE_STAGED_INSERT, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                            } else if (ec == FAIL_EXPIRY) {
                                return setExpiryOvertimeModeAndFail(err, CoreTransactionAttemptContextHooks.HOOK_CREATE_STAGED_INSERT, ec);
                            } else if (ec == FAIL_AMBIGUOUS) {
                                return Mono.delay(DEFAULT_DELAY_RETRYING_OPERATION, scheduler())
                                        .then(createStagedInsert(operationId, collection, id, content, span, cas));
                            } else if (ec == FAIL_TRANSIENT) {
                                return Mono.error(operationFailed(out.retryTransaction().build()));
                            } else if (ec == FAIL_HARD) {
                                return Mono.error(operationFailed(out.doNotRollbackAttempt().build()));
                            } else if (ec == FAIL_DOC_ALREADY_EXISTS
                                    || ec == FAIL_CAS_MISMATCH) {
                                return handleDocExistsDuringStagedInsert(operationId, collection, id, content, span);
                            } else {
                                return Mono.error(operationFailed(out.build()));
                            }
                        }
                    })

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }


    /**
     * Removes the specified <code>doc</code>, using the document's last
     * {@link CoreTransactionGetResult#cas()}.
     * <p>
     * @param doc - the doc to be removed
     */
    public Mono<Void> remove(CoreTransactionGetResult doc, SpanWrapper pspan) {
        return doKVOperation("remove " + DebugUtil.docId(doc), pspan, CoreTransactionAttemptContextHooks.HOOK_REMOVE, doc.collection(), doc.id(),
                (operationId, span, lockToken) -> removeInternalLocked(operationId, doc, span, lockToken)
                        // This triggers onNext
                        .thenReturn(1)).then();
    }

    private Mono<Void> removeInternalLocked(String operationId, CoreTransactionGetResult doc, SpanWrapper span, ReactiveLock.Waiter lockToken) {
        return Mono.defer(() -> {
            LOGGER.info(attemptId, "remove doc %s, operationId=%s", DebugUtil.docId(doc), operationId);

            if (queryModeLocked()) {
                return removeWithQueryLocked(doc, lockToken, span);
            } else {
                return removeWithKVLocked(operationId, doc, span, lockToken);
            }
        });
    }

    private Mono<Void> removeWithKVLocked(String operationId, CoreTransactionGetResult doc, SpanWrapper span, ReactiveLock.Waiter lockToken) {
        return Mono.defer(() -> {
            boolean mayNeedToWriteATR = state == AttemptState.NOT_STARTED;
            Optional<StagedMutation> existing = findStagedMutationLocked(doc);

            return hooks.beforeUnlockRemove.apply(this, doc.id()) // testing hook
                        .then(unlock(lockToken, "standard"))

                    .then(Mono.defer(() -> {
                        if (existing.isPresent()) {
                            StagedMutation op = existing.get();
                            LOGGER.info(attemptId, "found previous write of %s as %s on remove", DebugUtil.docId(doc), op.type);

                            if (op.type == StagedMutationType.REMOVE) {
                                return Mono.error(operationFailed(createError()
                                        .cause(new DocumentNotFoundException(null))
                                        .build()));
                            } else if (op.type == StagedMutationType.INSERT) {
                                return removeStagedInsert(doc, span);
                            }
                        }

                        return checkAndHandleBlockingTxn(doc, span, ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_REMOVING, existing)

                                .then(initATRIfNeeded(mayNeedToWriteATR, doc.collection(), doc.id(), span))

                                .then(createStagedRemove(operationId, doc, doc.cas(), span, doc.links().isDeleted()));
                    }));
                });
    }

    private Mono<Void> checkAndHandleBlockingTxn(CoreTransactionGetResult doc,
                                                 SpanWrapper pspan,
                                                 ForwardCompatibilityStage stage,
                                                 Optional<StagedMutation> existingOpt) {


        // A main reason to require doc be fetched inside the txn is so we can detect this on the client side
        if (doc.links().hasStagedWrite()) {
            // Check not just writing the same doc twice in the same txn
            // Note we check the transaction rather than attempt ID.  This is to handle [RETRY-ERR-AMBIG-REPLACE].
            if (doc.links().stagedTransactionId().get().equals(transactionId())) {
                if (doc.links().stagedAttemptId().get().equals(attemptId)) {
                    if (existingOpt.isPresent()) {
                        StagedMutation existing = existingOpt.get();

                        if (existing.cas != doc.cas()) {
                            LOGGER.info(attemptId, "concurrent op race detected on doc %s: have read a document before a concurrent op wrote its stagedMutation", DebugUtil.docId(doc));

                            return Mono.error(operationFailed(createError()
                                    .cause(new ConcurrentOperationsDetectedOnSameDocumentException())
                                    .build()));
                        }
                    }
                    else {
                        LOGGER.info(attemptId, "concurrent op race detected on doc %s: can see the KV result of another op, but stagedMutation not yet written", DebugUtil.docId(doc));

                        return Mono.error(operationFailed(createError()
                                .cause(new ConcurrentOperationsDetectedOnSameDocumentException())
                                .build()));
                    }
                }

                LOGGER.info(attemptId, "doc %s has been written by a different attempt in transaction, ok to continue",
                        DebugUtil.docId(doc));

                return Mono.empty();
            } else {
                if (doc.links().atrId().isPresent() && doc.links().atrBucketName().isPresent()) {
                    LOGGER.info(attemptId, "doc %s is in another txn %s, checking ATR "
                                    + "entry %s/%s/%s to see if blocked",
                            DebugUtil.docId(doc),
                            doc.links().stagedAttemptId().get(),
                            doc.links().atrBucketName().orElse(""),
                            doc.links().atrCollectionName().orElse(""), doc.links().atrId().orElse(""));

                    return forwardCompatibilityCheck(stage, doc.links().forwardCompatibility())

                            .then(checkATREntryForBlockingDoc(doc, pspan));
                } else {
                    LOGGER.info(attemptId, "doc %s is in another txn %s, cannot " +
                                    "check ATR entry - probably a bug, so proceeding to overwrite",
                            DebugUtil.docId(doc),
                            doc.links().stagedAttemptId().get());

                    return Mono.empty();
                }
            }
        } else {
            return Mono.empty();
        }
    }

    private byte[] listToDocRecords(List<StagedMutation> docs) throws JsonProcessingException {
        ArrayNode root = Mapper.createArrayNode();
        docs.forEach(doc -> {
            ObjectNode jn = Mapper.createObjectNode();
            jn.set(TransactionFields.ATR_FIELD_PER_DOC_ID, Mapper.convertValue(doc.id, JsonNode.class));
            jn.set(TransactionFields.ATR_FIELD_PER_DOC_BUCKET, Mapper.convertValue(doc.collection.bucket(), JsonNode.class));
            jn.set(TransactionFields.ATR_FIELD_PER_DOC_SCOPE, Mapper.convertValue(doc.collection.scope().orElse(DEFAULT_SCOPE), JsonNode.class));
            jn.set(TransactionFields.ATR_FIELD_PER_DOC_COLLECTION, Mapper.convertValue(doc.collection.collection().orElse(DEFAULT_COLLECTION), JsonNode.class));
            root.add(jn);
        });

        return Mapper.writer().writeValueAsBytes(root);
    }

    private List<SubdocMutateRequest.Command> addDocsToBuilder(int baseIndex) {
        String prefix = "attempts." + attemptId;

        try {
            return Arrays.asList(
                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_DOCS_INSERTED,
                            listToDocRecords(stagedInsertsLocked()), false, true, false, baseIndex),
                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_DOCS_REPLACED,
                            listToDocRecords(stagedReplacesLocked()), false, true, false, baseIndex + 1),
                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_DOCS_REMOVED,
                            listToDocRecords(stagedRemovesLocked()), false, true, false, baseIndex + 2)
            );
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private @Nullable
    CleanupRequest createCleanupRequestIfNeeded(@Nullable CoreTransactionsCleanup cleanup) {
        if (!config.cleanupConfig().runRegularAttemptsCleanupThread() || cleanup == null) {
            // Don't add a request to a queue that no-one will be processing
            LOGGER.trace(attemptId(), "skipping addition of cleanup request on failure as regular cleanup disabled");
        } else if (queryModeUnlocked()) {
            LOGGER.info(attemptId(), "Skipping cleanup request as in query mode");
        } else if (atrId().isPresent() && atrCollection().isPresent()) {
            switch (state()) {
                case NOT_STARTED:
                case COMPLETED:
                case ROLLED_BACK:
                    LOGGER.trace(attemptId(), "Skipping addition of cleanup request in state %s", state());
                    break;
                default:
                    LOGGER.trace(attemptId(), "Adding cleanup request for %s/%s",
                            atrCollection().get().collection(), atrId().get());

                    return createCleanupRequest();
            }
        } else {
            // No ATR entry to remove
            LOGGER.trace(attemptId(), "Skipping cleanup request as no ATR entry to remove (due to no " +
                    "mutations)");
        }

        return null;
    }

    // Would usually be locked as it's checking stagedMutations, but is only called from places
    // where we are protected from new ops being added to stagedMutations.
    private CleanupRequest createCleanupRequest() {
        CbPreconditions.check(state != AttemptState.NOT_STARTED);
        CbPreconditions.check(state != AttemptState.COMPLETED);

        long transactionElapsedTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - overall.startTimeClient());

        return new CleanupRequest(attemptId,
                atrId().get(),
                atrCollection().get(),
                state,
                // We're not locked here, but we're at the end of the lambda. Nothing can be adding to it.
                toDocRecords(stagedMutationsLocked.stream().filter(v -> v.type == StagedMutationType.REPLACE).collect(Collectors.toList())),
                toDocRecords(stagedMutationsLocked.stream().filter(v -> v.type == StagedMutationType.REMOVE).collect(Collectors.toList())),
                toDocRecords(stagedMutationsLocked.stream().filter(v -> v.type == StagedMutationType.INSERT).collect(Collectors.toList())),
                Duration.ZERO,
                Optional.empty(),
                transactionElapsedTimeMillis,
                Optional.of(config.durabilityLevel()));
    }

    /**
     * Commits the transaction.  All staged replaces, inserts and removals will be written.
     * <p>
     * The semantics are the same as {@link #commit()}, except that the a <code>Mono</code> is returned
     * so the operation can be performed asynchronously.
     */
    public Mono<Void> commit() {
        return commitInternal();
    }

    Mono<Void> implicitCommit(boolean singleQueryTransactionMode) {
        return Mono.defer(() -> {

            // May have done an explicit commit already, or the attempt may have failed.
            if (hasStateBit(TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED)) {
                return Mono.just(this);
            }
            else if (singleQueryTransactionMode) {
                return Mono.just(this);
            }
            else {
                LOGGER.info(attemptId(), "doing implicit commit");

                return commitInternal();
            }
        }).then();
    }

    Mono<Void> commitInternal() {
        // MonoBridge in case someone is trying a concurrent commit
        return createMonoBridge("commit", Mono.defer(() -> {
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_COMMIT, attemptSpan);
            assertNotLocked("commit");
            return waitForAllOpsThenDoUnderLock("commit", span,
                    () -> commitInternalLocked(span))
                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        }));
    }

    private Mono<Void> commitInternalLocked(SpanWrapper span) {
        return Mono.defer(() -> {
            assertLocked("commitInternal");

            TransactionOperationFailedException returnEarly = canPerformCommit("commit");
            if (returnEarly != null) {
                logger().info(attemptId, "commit raising %s", DebugUtil.dbg(returnEarly));
                return Mono.error(returnEarly);
            }

            setStateBits("commit", TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED | TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED, 0);

            if (queryModeLocked()) {
                return commitWithQueryLocked(span);
            } else {
                LOGGER.info(attemptId, "commit %s", this);

                // Commit hasn't started yet, so if we've expired, probably better to make a single attempt to rollback
                // than to commit.
                checkExpiryPreCommitAndSetExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_BEFORE_COMMIT, Optional.empty());

                if (!atrCollection.isPresent() || !atrId.isPresent()) {
                    // no mutation, no need to commit
                    return Mono.create(s -> {
                        LOGGER.info(attemptId, "calling commit on attempt that's got no mutations, skipping");

                        // Leave state as NOTHING_WRITTEN (or NOT_STARTED, as we have to call it in the Java
                        // implementation).  A successful read-only transaction ends in NOTHING_WRITTEN.

                        s.success();
                    });
                } else {
                    return commitActualLocked(span);
                }
            }
        }).subscribeOn(scheduler());
    }

    private Mono<Void> commitActualLocked(SpanWrapper span) {
        return Mono.defer(() -> {
            String prefix = "attempts." + attemptId;

            ArrayList<SubdocMutateRequest.Command> specs = new ArrayList<>();
            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_STATUS, serialize(AttemptState.COMMITTED.name()), false, true, false, 0));
            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_START_COMMIT, serialize("${Mutation.CAS}"), false, true, true, 1));
            specs.addAll(addDocsToBuilder(specs.size()));
            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_COMMIT_ONLY_IF_NOT_ABORTED, serialize(0), false, true, false, specs.size()));

            AtomicReference<Long> overallStartTime = new AtomicReference<>(0l);

            return atrCommitLocked(specs, overallStartTime, span)

                    .then(commitDocsLocked(span))

                    .then(atrCompleteLocked(prefix, overallStartTime, span))

                    .doOnSuccess(ignore -> {
                        LOGGER.info(attemptId, "overall commit completed");
                    })

                    .then();
        });
    }

    private Mono<Void> commitWithQueryLocked(SpanWrapper span) {
        return Mono.defer(() -> {
            int sidx = queryStatementIdx.getAndIncrement();

            return queryWrapperBlockingLocked(sidx,
                    queryContext.bucketName,
                    queryContext.scopeName,
                    "COMMIT",
                    Mapper.createObjectNode(),
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_COMMIT,
                    false,
                    // existingErrorCheck false as it's already been done in commitInternalLocked
                    false,null, null, span, false, null, true)

                    .doOnNext(v -> {
                        // MB-42619 means that query does not return the actual transaction state.  We assume it's
                        // COMPLETED but this isn't correct in some cases, e.g. a read-only transaction will leave
                        // it NOT_STARTED.
                        setStateLocked(AttemptState.COMPLETED);
                    })

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);

                        if (ec == FAIL_EXPIRY) {
                            TransactionOperationFailedException e = operationFailed(createError()
                                    .cause(err)
                                    .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                    .doNotRollbackAttempt()
                                    .build());
                            return Mono.error(e);
                        }
                        else if (ec == TRANSACTION_OPERATION_FAILED) {
                            return Mono.error(err);
                        }

                        TransactionOperationFailedException e = operationFailed(createError()
                                                        .cause(err)
                                                        .doNotRollbackAttempt()
                                                        .build());
                        return Mono.error(e);
                    })

                    .then();
        });
    }

    // The timing of this call is important.
    // Should be done before doOnNext, which tests often make throw an exception.
    // In fact, needs to be done without relying on any onNext signal.  What if the operation times out instead.
    private void checkExpiryDuringCommitOrRollbackLocked(String stage, Optional<String> id) {
        assertLocked("checkExpiryDuringCommitOrRollbackLocked in stage " + stage);
        if (!expiryOvertimeMode) {
            if (hasExpiredClientSide(stage, id)) {
                LOGGER.info(attemptId, "has expired in stage %s, entering expiry-overtime mode (one attempt to complete)",
                        stage);
                expiryOvertimeMode = true;
            }
        } else {
            LOGGER.info(attemptId, "ignoring expiry in stage %s, as in expiry-overtime mode", stage);
        }
    }

    private Mono<Void> atrCompleteLocked(String prefix, AtomicReference<Long> overallStartTime, SpanWrapper pspan) {
        return Mono.defer(() -> {
            assertLocked("atrComplete");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_COMPLETE, pspan);

            LOGGER.info(attemptId, "about to remove ATR entry %s", getAtrDebug(atrCollection, atrId));

            return Mono.defer(() -> {
                if (!expiryOvertimeMode && hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_ATR_COMPLETE, Optional.empty())) {
                    String msg = "has expired in stage atrComplete, but transaction has successfully completed so returning success";
                    LOGGER.info(attemptId, msg);

                    return Mono.error(new AttemptExpiredException(msg));
                } else {
                    return Mono.empty();
                }
            })

                    .then(hooks.beforeAtrComplete.apply(this)) // testing hook

                    .then(TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                            false, false, false, false, false, 0, durabilityLevel(), OptionsUtil.createClientContext("atrComplete"), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, prefix, null, false, true, false, 0)
                            )))

                    .publishOn(scheduler())

                    .flatMap(v -> hooks.afterAtrComplete.apply(this).thenReturn(v))  // Testing hook

                .doOnNext(v -> {
                    setStateLocked(AttemptState.COMPLETED);
                    addUnits(v.flexibleExtras());
                    long now = System.nanoTime();
                    long elapsed = span.elapsedMicros();
                    LOGGER.info(attemptId, "removed ATR %s in %dus%s, overall commit completed in %dus",
                            getAtrDebug(atrCollection, atrId), elapsed, DebugUtil.dbg(v.flexibleExtras()), TimeUnit.NANOSECONDS.toMicros(now - overallStartTime.get()));
                })

                    .then()

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));

                    LOGGER.info(attemptId, "error '%s' ec=%s while removing ATR %s%s", err, ec,
                            getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units));

                    if (ec == FAIL_HARD) {
                        return Mono.error(operationFailed(createError()
                                .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT)
                                .doNotRollbackAttempt()
                                .build()));
                    } else {
                        LOGGER.info(attemptId, "ignoring error during transaction tidyup, regarding as success");

                            return Mono.empty();
                        }
                    })
                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    // [EXP-ROLLBACK] [EXP-COMMIT-OVERTIME]: have been trying to make one attempt to rollback (or finish commit) after
    // expiry, but something's failed.  Give up and raise AttemptExpired.  Do not attempt any further rollback.
    private <T> Mono<T> mapErrorInOvertimeToExpired(boolean updateAppState, String stage, Throwable err, FinalErrorToRaise toRaise) {
        LOGGER.info(attemptId, "in expiry-overtime mode so changing error '%s' to raise %s in stage '%s'; no rollback will be tried",
                err, toRaise, stage);

        if (!expiryOvertimeMode) {
            LOGGER.warn(attemptId, "not in expiry-overtime mode handling error '%s' in stage %s, possibly a bug",
                    err, stage);
        }

        return Mono.error(operationFailed(updateAppState, createError()
                .doNotRollbackAttempt()
                .raiseException(toRaise)
                .cause(new AttemptExpiredException(err)).build()));
    }

    private Mono<Void> removeDocLocked(SpanWrapper span, CollectionIdentifier collection, String id, boolean ambiguityResolutionMode) {
        return Mono.defer(() -> {
            assertLocked("removeDoc");

            return Mono.fromRunnable(() -> {
                LOGGER.info(attemptId, "about to remove doc %s, ambiguityResolutionMode=%s",
                        DebugUtil.docId(collection, id), ambiguityResolutionMode);

                // [EXP-COMMIT-OVERTIME]
                checkExpiryDuringCommitOrRollbackLocked(CoreTransactionAttemptContextHooks.HOOK_REMOVE_DOC, Optional.of(id));
            })

                    .then(hooks.beforeDocRemoved.apply(this, id)) // testing hook

                    // A normal (non-subdoc) remove will also remove a doc's user xattrs too
                    .then(TransactionKVHandler.remove(core,
                                    collection,
                                    id,
                                    kvTimeoutNonMutating(),
                                    0,
                                    durabilityLevel(),
                                    OptionsUtil.createClientContext("commitRemove"),
                                    span))

                    // Testing hook (goes before onErrorResume)
                    .flatMap(mutationResult -> hooks.afterDocRemovedPreRetry.apply(this, id)

                            .thenReturn(mutationResult))

                    .doOnNext(mutationResult -> {
                        addUnits(mutationResult.flexibleExtras());
                        LOGGER.info(attemptId, "commit - removed doc %s%s, mt = %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(mutationResult.flexibleExtras()), mutationResult.mutationToken());
                    })

                    .then()

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder e = createError()
                                .cause(err)
                                .doNotRollbackAttempt()
                                .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));

                        LOGGER.info("got error while removing doc %s%s in %dus: %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                        if (expiryOvertimeMode) {
                            return mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_REMOVE_DOC, err, FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
                        } else if (ec == FAIL_AMBIGUOUS) {
                            return Mono.delay(DEFAULT_DELAY_RETRYING_OPERATION, scheduler())
                                    .then(removeDocLocked(span, collection, id, true));
                        } else if (ec == FAIL_DOC_NOT_FOUND) {
                            return Mono.error(operationFailed(e.build()));
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(e.build()));
                        } else {
                            return Mono.error(operationFailed(e.build()));
                        }
                    })

                    // Testing hook
                    .then(hooks.afterDocRemovedPostRetry.apply(this, id))

                    .then()

                    .doOnError(err -> span.span().status(RequestSpan.StatusCode.ERROR));
        });
    }

    private Mono<Void> commitDocsLocked(SpanWrapper span) {
        return Mono.defer(() -> {
            assertLocked("commitDocs");
            long start = System.nanoTime();

            // TXNJ-64 - commit in the order the docs were staged
            return Flux.fromIterable(stagedMutationsLocked)
                    .publishOn(scheduler())

                    .concatMap(staged -> {
                        return commitDocWrapperLocked(span, staged);
                    })

                    .then(Mono.defer(() -> {
                        long elapsed = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                        LOGGER.info(attemptId, "commit - all %d docs committed in %dus",
                                stagedMutationsLocked.size(), elapsed);

                        // Testing hook
                        return hooks.afterDocsCommitted.apply(this);
                    }))

                    .then()

                    .doOnError(err -> span.span().status(RequestSpan.StatusCode.ERROR));
        });
    }

    private static String msgDocChangedUnexpectedly(CollectionIdentifier collection, String id) {
        return "Tried committing document " + DebugUtil.docId(collection, id) + ", but found that it has " +
                "been modified by another party in-between staging and committing.  The application must ensure that " +
                "non-transactional writes cannot happen at the same time as transactional writes on a document." +
                " This document may need manual review to verify that no changes have been lost.";
    }

    private static String msgDocRemovedUnexpectedly(CollectionIdentifier collection, String id, boolean willBeWritten) {
        if (willBeWritten) {
            return "Tried committing document " + DebugUtil.docId(collection, id) + ", but found that it has " +
                    "been removed by another party in-between staging and committing.  " +
                    "The application must ensure that non-transactional writes cannot happen at" +
                    " the same time as transactional writes on a document.  The document will be written.";
        }
        else {
            return "Tried committing document " + DebugUtil.docId(collection, id) + ", but found that it has " +
                    "been removed by another party in-between staging and committing.  " +
                    "The application must ensure that non-transactional writes cannot happen at" +
                    " the same time as transactional writes on a document.  The document " +
                    "will be left removed, and the transaction's changes will not be written to this document";
        }
    }
    private Mono<Void> commitDocWrapperLocked(SpanWrapper pspan,
                                              StagedMutation staged) {
        return Mono.defer(() -> {
            if (staged.type == StagedMutationType.REMOVE) {
                return removeDocLocked(pspan, staged.collection, staged.id, false);
            } else {
                return commitDocLocked(pspan, staged, staged.cas,staged.type == StagedMutationType.INSERT, false);
            }
        });
    }

    private Mono<Void> commitDocLocked(SpanWrapper span,
                                       StagedMutation staged,
                                       long cas,
                                       boolean insertMode,
                                       boolean ambiguityResolutionMode) {
        return Mono.defer(() -> {
            assertLocked("commitDoc");
            String id = staged.id;
            CollectionIdentifier collection = staged.collection;

            return Mono.fromRunnable(() -> {
                LOGGER.info(attemptId, "commit - committing doc %s, cas=%d, insertMode=%s, ambiguity-resolution=%s supportsReplaceBodyWithXattr=%s",
                        DebugUtil.docId(collection, id), cas, insertMode, ambiguityResolutionMode, staged.supportsReplaceBodyWithXattr());

                checkExpiryDuringCommitOrRollbackLocked(CoreTransactionAttemptContextHooks.HOOK_COMMIT_DOC, Optional.of(id));
            })

                    .then(hooks.beforeDocCommitted.apply(this, id)) // testing hook

                    .then(Mono.defer(() -> {
                        if (insertMode) {
                            if (staged.supportsReplaceBodyWithXattr()) {
                                return TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(), false, false, true, true, false,
                                        cas, durabilityLevel(), OptionsUtil.createClientContext("commitDocInsert"), span, Arrays.asList(
                                                new SubdocMutateRequest.Command(SubdocCommandType.REPLACE_BODY_WITH_XATTR, TransactionFields.STAGED_DATA, null, false, true, false, 0),
                                                new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, null, false, true, false, 1)
                                        ))
                                        .doOnNext(v -> {
                                            addUnits(v.flexibleExtras());
                                            LOGGER.info(attemptId, "commit - committed doc insert swap body %s got cas %d%s", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                                        })
                                        .map(SubdocMutateResponse::cas);
                            }
                            else {
                                return TransactionKVHandler.insert(core, collection, id, staged.content, kvTimeoutMutating(),
                                                durabilityLevel(), OptionsUtil.createClientContext("commitDocInsert"), span)
                                        .doOnNext(v -> {
                                            addUnits(v.flexibleExtras());
                                            LOGGER.info(attemptId, "commit - committed doc insert %s got cas %d%s", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                                        })
                                        .map(InsertResponse::cas);
                            }
                        } else {
                            if (staged.supportsReplaceBodyWithXattr()) {
                                return TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(), false, false, false, false, false,
                                                cas, durabilityLevel(), OptionsUtil.createClientContext("commitDoc"), span, Arrays.asList(
                                                        new SubdocMutateRequest.Command(SubdocCommandType.REPLACE_BODY_WITH_XATTR, TransactionFields.STAGED_DATA, null, false, true, false, 0),
                                                        new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, null, false, true, false, 1)
                                                ))
                                        .doOnNext(v -> {
                                            addUnits(v.flexibleExtras());
                                            LOGGER.info(attemptId, "commit - committed doc replace swap body %s got cas %d%s", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                                        })
                                        .map(SubdocMutateResponse::cas);
                            }
                            else {
                                return TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                                                false, false, false, false, false, cas, durabilityLevel(),
                                                OptionsUtil.createClientContext("commitDoc"), span,
                                                Arrays.asList(
                                                        // Upsert this field to better handle illegal doc mutation.  E.g. run shadowDocSameTxnKVInsert without this,
                                                        // fails at this point as path has been removed.  Could also handle with a spec change to handle that.
                                                        new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, serialize(null), false, true, false, 0),
                                                        new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, null, false, true, false, 1),
                                                        new SubdocMutateRequest.Command(SubdocCommandType.SET_DOC, "", staged.content, false, false, false, 2)
                                                ))
                                        .doOnNext(v -> {
                                            addUnits(v.flexibleExtras());
                                            LOGGER.info(attemptId, "commit - committed doc replace %s got cas %d%s", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                                        })
                                        .map(SubdocMutateResponse::cas);
                            }
                        }
                    }))

                    .publishOn(scheduler())

                    // Testing hook
                    .flatMap(v -> hooks.afterDocCommittedBeforeSavingCAS.apply(this, id).thenReturn(v))

                    // Do checkExpiryDuringCommitOrRollback before doOnNext (which tests often make throw)
                    .flatMap(newCas -> hooks.afterDocCommitted.apply(this, id))

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder e = createError().cause(err)
                                .doNotRollbackAttempt()
                                .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));

                        LOGGER.info(attemptId, "error while committing doc %s%s in %dus: %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                        if (expiryOvertimeMode) {
                            return mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_COMMIT_DOC, err, FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT).thenReturn(0);
                        } else if (ec == FAIL_AMBIGUOUS) {
                            // TXNJ-136: The operation may or may not have succeeded.  Retry in ambiguity-resolution mode.
                            LOGGER.warn(attemptId, "%s while committing doc %s: as op is ambiguously successful, retrying " +
                                    "op in ambiguity-resolution mode", DebugUtil.dbg(err), DebugUtil.docId(collection, id));

                            return commitDocLocked(span, staged, cas, insertMode, true).thenReturn(0);
                        } else if (ec == FAIL_CAS_MISMATCH) {
                            return handleDocChangedDuringCommit(span, staged, insertMode).thenReturn(0);
                        } else if (ec == FAIL_DOC_NOT_FOUND) {
                            return handleDocMissingDuringCommit(span, staged);
                        } else if (ec == FAIL_DOC_ALREADY_EXISTS) { // includes CannotReviveAliveDocumentException
                            if (ambiguityResolutionMode) {
                                return Mono.error(e.build());
                            } else {
                                String msg = msgDocChangedUnexpectedly(collection, id);
                                LOGGER.warn(attemptId, msg);
                                LOGGER.eventBus().publish(new IllegalDocumentStateEvent(Event.Severity.WARN, msg, id));

                                if (staged.supportsReplaceBodyWithXattr()) {
                                    // There's nothing can be done, the document data is lost
                                    return Mono.empty();
                                }
                                else {
                                    // Redo as a replace, which will of course fail on CAS.
                                    return Mono.delay(DEFAULT_DELAY_RETRYING_OPERATION)
                                            .then(commitDocLocked(span, staged, cas, false, ambiguityResolutionMode)
                                                    .thenReturn(0));
                                }
                            }
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(e.build()));
                        } else {
                            return Mono.error(operationFailed(e.build()));
                        }
                    })

                    .then()

                    .doOnError(err -> span.span().status(RequestSpan.StatusCode.ERROR));
        });
    }

    private void addUnits(@Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
        meteringUnitsBuilder.add(flexibleExtras);
    }

    private MeteringUnits addUnits(@Nullable MeteringUnits units) {
        meteringUnitsBuilder.add(units);
        return units;
    }

    private Mono<Integer> handleDocMissingDuringCommit(SpanWrapper pspan,
                                                       StagedMutation staged) {
        return Mono.defer(() -> {
            String msg = msgDocRemovedUnexpectedly(staged.collection, staged.id, true);
            LOGGER.warn(attemptId, msg);
            LOGGER.eventBus().publish(new IllegalDocumentStateEvent(Event.Severity.WARN, msg, staged.id));
            return Mono.delay(DEFAULT_DELAY_RETRYING_OPERATION)
                    .then(commitDocLocked(pspan, staged, 0, true, false)
                            .thenReturn(0));
        });
    }

    public RequestSpan span() {
        return attemptSpan.span();
    }

    static class DocChanged {
        public final boolean unclearIfBodyHasChanged;
        public final boolean bodyHasChanged;
        public final boolean inDifferentTransaction;
        public final boolean notInTransaction;

        public DocChanged(boolean unclearIfBodyHasChanged, boolean bodyHasChanged, boolean inDifferentTransaction, boolean notInTransaction) {
            this.unclearIfBodyHasChanged = unclearIfBodyHasChanged;
            this.bodyHasChanged = bodyHasChanged;
            this.inDifferentTransaction = inDifferentTransaction;
            this.notInTransaction = notInTransaction;
        }

        public boolean inSameTransaction() {
            return !notInTransaction && !inDifferentTransaction;
        }
    }
    /**
     * Called after fetching a document, to determine what parts of it have changed.
     *
     * @param crc32Then the CRC32 at time of older fetch. Could be from ctx.get(), or the one written at time of staging.
     *                  There are many times where this is unavailable, hence it being optional.  e.g. if CAS conflict
     *                  between get and replace, or if the get was with a version of query that doesn't return CRC, or if the
     *                  first op was an insert (no body to create a CRC32 from).
     * @param crc32Now the CRC32 now, e.g. what has just been fetched.
     */
    private DocChanged getDocChanged(CoreTransactionGetResult gr, String stage, Optional<String> crc32Then, String crc32Now) {
        // Note that in some cases we're not sure if body has changed,
        boolean unclearIfBodyHasChanged = !crc32Then.isPresent();
        boolean bodyHasChanged = crc32Then.isPresent()
                && !crc32Now.equals(crc32Then.get());
        boolean inDifferentTransaction = gr.links() != null
                && gr.links().stagedAttemptId().isPresent()
                && !gr.links().stagedAttemptId().get().equals(attemptId);
        boolean notInTransaction = gr.links() == null
                || !gr.links().isDocumentInTransaction();
        DocChanged out = new DocChanged(unclearIfBodyHasChanged, bodyHasChanged, inDifferentTransaction, notInTransaction);

        LOGGER.info(attemptId, "handling doc changed during %s fetched doc %s, unclearIfBodyHasChanged = %s, bodyHasChanged = %s, inDifferentTransaction = %s, notInTransaction = %s, inSameTransaction = %s links = %s metadata = %s cas = %s crc32Then = %s, crc32Now = %s",
                stage, DebugUtil.docId(gr.collection(), gr.id()), unclearIfBodyHasChanged, bodyHasChanged,
                inDifferentTransaction, notInTransaction, out.inSameTransaction(), gr.links(), gr.documentMetadata(), gr.cas(), crc32Then, crc32Now);

        return out;
    }

    // No need to pass ambiguityResolutionMode - we are about to resolve the ambiguity
    private Mono<Void> handleDocChangedDuringCommit(SpanWrapper span,
                                                    StagedMutation staged,
                                                    boolean insertMode) {
        return Mono.defer(() -> {
            String id = staged.id;
            CollectionIdentifier collection = staged.collection;
            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

            return Mono.fromRunnable(() -> {
                        LOGGER.info(attemptId, "commit - handling doc changed %s, insertMode=%s",
                                DebugUtil.docId(collection, id), insertMode);
                        if (hasExpiredClientSide(HOOK_COMMIT_DOC_CHANGED, Optional.of(staged.id))) {
                            LOGGER.info(attemptId, "has expired in stage %s", HOOK_COMMIT_DOC_CHANGED);
                            throw operationFailed(createError()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT)
                                    .doNotRollbackAttempt()
                                    .cause(new AttemptExpiredException("Attempt has expired in stage " + HOOK_COMMIT_DOC_CHANGED))
                                    .build());
                        }
                    })
                    .then(hooks.beforeDocChangedDuringCommit.apply(this, id)) // testing hook
                    .then(DocumentGetter.getAsync(core, LOGGER, staged.collection, config, staged.id, attemptId, true, span, Optional.empty(), units))
                    .publishOn(scheduler())
                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        MeteringUnits built = addUnits(units.build());
                        span.recordException(err);
                        LOGGER.info(attemptId, "commit - handling doc changed %s%s, got error %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(built), dbg(err));
                        if (ec == TRANSACTION_OPERATION_FAILED) {
                            return Mono.error(err);
                        } else if (ec == FAIL_TRANSIENT) {
                            return Mono.error(new RetryOperationException());
                        } else {
                            return Mono.error(operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT)
                                    .cause(err)
                                    .build()));
                        }
                        // FAIL_DOC_NOT_FOUND handled elsewhere in this function
                    })
                    .flatMap(doc -> {
                        addUnits(units.build());
                        if (doc.isPresent()) {
                            CoreTransactionGetResult gr = doc.get();
                            DocChanged dc = getDocChanged(gr,
                                    "commit",
                                    // This will usually be present, from staging. But could have got an ERR_AMBIG while committing the doc, and it actually succeeded.  In which case this will be empty.
                                    gr.links().crc32OfStaging(),
                                    gr.crc32OfGet().get()        // this must be present as just fetched with $document
                            );
                            return forwardCompatibilityCheck(ForwardCompatibilityStage.CAS_MISMATCH_DURING_COMMIT, gr.links().forwardCompatibility())
                                    .then(Mono.defer(() -> {
                                        if (dc.inDifferentTransaction || dc.notInTransaction) {
                                            return Mono.empty();
                                        } else {
                                            // Doc is still in same attempt
                                            if (dc.bodyHasChanged) {
                                                String msg = msgDocChangedUnexpectedly(staged.collection, staged.id);
                                                LOGGER.warn(attemptId, msg);
                                                LOGGER.eventBus().publish(new IllegalDocumentStateEvent(Event.Severity.WARN, msg, staged.id));
                                            }
                                            // Retry committing the doc, with the new CAS
                                            return Mono.delay(DEFAULT_DELAY_RETRYING_OPERATION)
                                                    .then(commitDocLocked(span, staged, gr.cas(), insertMode, false));
                                        }
                                    }));
                        } else {
                            return handleDocMissingDuringCommit(span, staged);
                        }
                    })
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY_WITH_FIXED_RETRY)
                    .then()
                    .doOnError(err -> span.setErrorStatus());
        });
    }

    private <T> Mono<T> handleDocChangedDuringStaging(SpanWrapper span,
                                                      String id,
                                                      CollectionIdentifier collection,
                                                      Optional<String> crc32FromGet,
                                                      Function<Long, Mono<T>> callback) {
        return Mono.defer(() -> {
            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();
            return Mono.fromRunnable(() -> {
                        LOGGER.info(attemptId, "handling doc changed during staging %s",
                                DebugUtil.docId(collection, id));
                        throwIfExpired(id, HOOK_STAGING_DOC_CHANGED);
                    })
                    .then(hooks.beforeDocChangedDuringStaging.apply(this, id)) // testing hook
                    .then(DocumentGetter.getAsync(core, LOGGER, collection, config, id, attemptId, true, span, Optional.empty(), units))
                    .publishOn(scheduler())
                    .onErrorResume(err -> {
                        MeteringUnits built = addUnits(units.build());
                        ErrorClass ec = classify(err);
                        LOGGER.info(attemptId, "handling doc changed during staging %s%s, got error %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(built), dbg(err));
                        if (ec == TRANSACTION_OPERATION_FAILED) {
                            return Mono.error(err);
                        } else if (ec == FAIL_TRANSIENT) {
                            return Mono.error(new RetryOperationException());
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                                    .cause(err)
                                    .build()));
                        } else {
                            return Mono.error(operationFailed(createError()
                                    .retryTransaction()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                                    .cause(err)
                                    .build()));
                        }
                        // FAIL_DOC_NOT_FOUND handled elsewhere in this function
                    })
                    .flatMap(doc -> {
                        addUnits(units.build());
                        if (doc.isPresent()) {
                            CoreTransactionGetResult gr = doc.get();
                            DocChanged dc = getDocChanged(gr,
                                    "staging",
                                    crc32FromGet,         // this may or may not be present - see getDocChanged for list
                                    gr.crc32OfGet().get() // this must be present as doc just fetched
                            );
                            return forwardCompatibilityCheck(ForwardCompatibilityStage.CAS_MISMATCH_DURING_STAGING, gr.links().forwardCompatibility())
                                    .then(Mono.defer(() -> {
                                        if (dc.inDifferentTransaction) {
                                            return checkAndHandleBlockingTxn(gr, span, ForwardCompatibilityStage.CAS_MISMATCH_DURING_STAGING, Optional.empty())
                                                    .then(Mono.error(new RetryOperationException()));
                                        } else { // must be bodyHasChanged || notInTransaction || unclearIfBodyHasChanged
                                            if (dc.bodyHasChanged || dc.unclearIfBodyHasChanged) {
                                                return Mono.error(operationFailed(createError()
                                                        .retryTransaction()
                                                        .build()));
                                            } else {
                                                // Retry the operation, with the new CAS
                                                return Mono.delay(DEFAULT_DELAY_RETRYING_OPERATION)
                                                        .then(callback.apply(gr.cas()));
                                            }
                                        }
                                    }));
                        }
                        else {
                            return Mono.error(operationFailed(createError()
                                    .retryTransaction()
                                    .cause(new DocumentNotFoundException(null))
                                    .build()));
                        }
                    })
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY_WITH_FIXED_RETRY)
                    .doFinally(v -> span.finish());
        });
    }

    private void throwIfExpired(String id, String stage) {
        if (hasExpiredClientSide(stage, Optional.of(id))) {
            LOGGER.info(attemptId, "has expired in stage %s", stage);
            throw operationFailed(createError()
                    .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                    .doNotRollbackAttempt()
                    .cause(new AttemptExpiredException("Attempt has expired in stage " + stage))
                    .build());
        }
    }

    private Mono<Void> handleDocChangedDuringRollback(SpanWrapper span,
                                                      String id,
                                                      CollectionIdentifier collection,
                                                      Function<Long, Mono<Void>> callback) {
        return Mono.defer(() -> {
            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();
            return Mono.fromRunnable(() -> {
                        LOGGER.info(attemptId, "handling doc changed during rollback %s",
                                DebugUtil.docId(collection, id));
                        throwIfExpired(id, HOOK_ROLLBACK_DOC_CHANGED);
                    })
                    .then(hooks.beforeDocChangedDuringRollback.apply(this, id)) // testing hook
                    .then(DocumentGetter.getAsync(core, LOGGER, collection, config, id, attemptId, true, span, Optional.empty(), units))
                    .publishOn(scheduler())
                    .onErrorResume(err -> {
                        MeteringUnits built = addUnits(units.build());
                        ErrorClass ec = classify(err);
                        span.recordException(err);
                        LOGGER.info(attemptId, "handling doc changed during rollback %s%s, got error %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(built), dbg(err));
                        if (ec == TRANSACTION_OPERATION_FAILED) {
                            return Mono.error(err);
                        } else if (ec == FAIL_TRANSIENT) {
                            return Mono.error(new RetryOperationException());
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                                    .cause(err)
                                    .build()));
                        } else {
                            return Mono.error(operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                                    .cause(err)
                                    .build()));
                        }
                        // FAIL_DOC_NOT_FOUND handled elsewhere in this function
                    })
                    .flatMap(doc -> {
                        addUnits(units.build());
                        if (doc.isPresent()) {
                            CoreTransactionGetResult gr = doc.get();
                            DocChanged dc = getDocChanged(gr,
                                    "rollback",
                                    gr.links().crc32OfStaging(),   // this should be present from staging
                                    gr.crc32OfGet().get()          // this must be present as doc just fetched
                            );
                            return forwardCompatibilityCheck(ForwardCompatibilityStage.CAS_MISMATCH_DURING_ROLLBACK, gr.links().forwardCompatibility())
                                    .then(Mono.defer(() -> {
                                        if (dc.inDifferentTransaction || dc.notInTransaction) {
                                            return Mono.empty();
                                        } else {
                                            // In same attempt, body may or may not have changed
                                            return callback.apply(gr.cas());
                                        }
                                    }));
                        }
                        else {
                            return Mono.empty();
                        }
                    })
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY_WITH_FIXED_RETRY)
                    .doOnError(err -> span.setErrorStatus())
                    .then();
        });
    }

    private Optional<DurabilityLevel> durabilityLevel() {
        return config.durabilityLevel() == DurabilityLevel.NONE ? Optional.empty() : Optional.of(config.durabilityLevel());
    }

    private Duration kvTimeoutMutating() {
        return OptionsUtil.kvTimeoutMutating(core);
    }

    private Duration kvTimeoutNonMutating() {
        return OptionsUtil.kvTimeoutNonMutating(core);
    }

    private Mono<Void> atrCommitAmbiguityResolutionLocked(AtomicReference<Long> overallStartTime,
                                                          SpanWrapper span) {
        return Mono.defer(() -> {
                    LOGGER.info(attemptId, "about to fetch status of ATR %s to resolve ambiguity, expiryOvertimeMode=%s",
                            getAtrDebug(atrCollection, atrId), expiryOvertimeMode);
                    overallStartTime.set(System.nanoTime());

                    return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_COMMIT_AMBIGUITY_RESOLUTION, Optional.empty());
                })

                .then(hooks.beforeAtrCommitAmbiguityResolution.apply(this)) // testing hook

                .then(TransactionKVHandler.lookupIn(core, atrCollection.get(), atrId.get(), kvTimeoutNonMutating(), false, OptionsUtil.createClientContext("atrCommitAmbiguityResolution"), span,
                        Arrays.asList(
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "attempts." + attemptId + "." + TransactionFields.ATR_FIELD_STATUS, true, 0)
                        )))
                .publishOn(scheduler())

                .flatMap(result -> {
                    String status = null;
                    try {
                        status = Mapper.reader().readValue(result.values()[0].value(), String.class);
                    } catch (IOException e) {
                        LOGGER.info(attemptId, "failed to parse ATR %s status '%s'", getAtrDebug(atrCollection, atrId), new String(result.values()[0].value()));
                        status = "UNKNOWN";
                    }

                    addUnits(result.flexibleExtras());
                    LOGGER.info(attemptId, "got status of ATR %s%s: '%s'", getAtrDebug(atrCollection, atrId),
                            DebugUtil.dbg(result.flexibleExtras()), status);

                    AttemptState state = AttemptState.convert(status);

                    switch (state) {
                        case COMMITTED:
                            return Mono.empty();

                        case ABORTED:
                            return Mono.error(operationFailed(createError()
                                    .retryTransaction()
                                    .build()));

                        default:
                            return Mono.error(operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .cause(new IllegalStateException("This transaction has been changed by another actor to be in unexpected state " + status))
                                    .build()));
                    }
                })

                .then()

                .onErrorResume(err -> {
                    ErrorClass ec = classify(err);
                    TransactionOperationFailedException.Builder builder = createError().doNotRollbackAttempt().cause(err);
                    MeteringUnits units = addUnits(MeteringUnits.from(err));
                    span.recordException(err);

                    if (err instanceof RetryAtrCommitException || ec == TRANSACTION_OPERATION_FAILED) {
                        return Mono.error(err);
                    }

                    LOGGER.info(attemptId, "error while resolving ATR %s ambiguity%s in %dus: %s",
                            getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                    if (ec == FAIL_EXPIRY) {
                        return Mono.error(operationFailed(createError()
                                .doNotRollbackAttempt()
                                .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                .cause(new AttemptExpiredException(err)).build()));
                    } else if (ec == FAIL_HARD) {
                        return Mono.error(operationFailed(builder
                                .doNotRollbackAttempt()
                                .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                .build()));
                    } else if (ec == FAIL_TRANSIENT || ec == FAIL_OTHER) {
                        return Mono.error(new RetryOperationException());
                    } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                        return Mono.error(operationFailed(createError()
                                .doNotRollbackAttempt()
                                .cause(new ActiveTransactionRecordEntryNotFoundException(atrId.get(), attemptId))
                                .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                .build()));
                    } else if (ec == FAIL_DOC_NOT_FOUND) {
                        return Mono.error(operationFailed(createError()
                                .doNotRollbackAttempt()
                                .cause(new ActiveTransactionRecordNotFoundException(atrId.get(), attemptId))
                                .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                .build()));
                    } else {
                        return Mono.error(operationFailed(builder.raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS).build()));
                    }
                })

                // Retry RetryOperation() exceptions
                .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY_WITH_FIXED_RETRY)
                .publishOn(scheduler()) // after retryWhen triggers, it's on parallel scheduler

                .doOnError(err -> span.setErrorStatus());
    }

    private Mono<Void> atrCommitLocked(List<SubdocMutateRequest.Command> specs,
                                       AtomicReference<Long> overallStartTime,
                                       SpanWrapper pspan) {

        return Mono.defer(() -> {
            assertLocked("atrCommit");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TRANSACTION_OP_ATR_COMMIT, pspan);

            AtomicBoolean ambiguityResolutionMode = new AtomicBoolean(false);

            return Mono.defer(() -> {
                LOGGER.info(attemptId, "about to set ATR %s to Committed, expiryOvertimeMode=%s, ambiguityResolutionMode=%s",
                        getAtrDebug(atrCollection, atrId), expiryOvertimeMode, ambiguityResolutionMode);
                overallStartTime.set(System.nanoTime());

                return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_COMMIT, Optional.empty());
            })

                    .then(hooks.beforeAtrCommit.apply(this)) // testing hook

                    .then(TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                            false, false, false, false, false, 0, durabilityLevel(), OptionsUtil.createClientContext("atrCommit"), span, specs))

                    .publishOn(scheduler())

                    // Testing hook
                    .flatMap(v -> hooks.afterAtrCommit.apply(this).thenReturn(v))

                    .doOnNext(v -> {
                        setStateLocked(AttemptState.COMMITTED);
                        addUnits(v.flexibleExtras());
                        LOGGER.info(attemptId, "set ATR %s to Committed%s in %dus", getAtrDebug(atrCollection, atrId),
                                DebugUtil.dbg(v.flexibleExtras()), span.elapsedMicros());
                    })

                    .then()

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().cause(err);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        span.recordException(err);

                        LOGGER.info(attemptId, "error while setting ATR %s to Committed%s in %dus: %s",
                                getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                        if (ec == FAIL_EXPIRY) {
                            FinalErrorToRaise toRaise = ambiguityResolutionMode.get() ? FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS : FinalErrorToRaise.TRANSACTION_EXPIRED;
                            return Mono.error(operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(toRaise)
                                    .cause(new AttemptExpiredException(err)).build()));
                        } else if (ec == FAIL_AMBIGUOUS) {
                            ambiguityResolutionMode.set(true);
                            return Mono.error(new RetryOperationException());
                        } else if (ec == FAIL_HARD) {
                            if (ambiguityResolutionMode.get()) {
                                return Mono.error(operationFailed(createError()
                                        .doNotRollbackAttempt()
                                        .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                        .cause(err).build()));
                            } else {
                                return Mono.error(operationFailed(builder.doNotRollbackAttempt().build()));
                            }
                        } else if (ec == FAIL_TRANSIENT) {
                            if (ambiguityResolutionMode.get()) {
                                throw new RetryOperationException();
                            } else {
                                return Mono.error(operationFailed(builder.retryTransaction().build()));
                            }
                        } else if (ec == FAIL_PATH_ALREADY_EXISTS) {
                            return atrCommitAmbiguityResolutionLocked(overallStartTime, pspan)
                                    .onErrorResume(e -> {
                                        if (e instanceof RetryAtrCommitException) {
                                            ambiguityResolutionMode.set(false);
                                            throw new RetryOperationException();
                                        } else {
                                            return Mono.error(e);
                                        }
                                    });
                        } else {
                            Throwable cause = err;
                            boolean rollback = true;
                            switch (ec) {
                                case FAIL_PATH_NOT_FOUND:
                                    cause = new ActiveTransactionRecordEntryNotFoundException(atrId.get(), attemptId);
                                    rollback = false;
                                    break;
                                case FAIL_DOC_NOT_FOUND:
                                    cause = new ActiveTransactionRecordNotFoundException(atrId.get(), attemptId);
                                    rollback = false;
                                    break;
                                case FAIL_ATR_FULL:
                                    cause = new ActiveTransactionRecordFullException(cause);
                                    rollback = false;
                                    break;
                            }

                            if (ambiguityResolutionMode.get()) {
                                return Mono.error(operationFailed(createError()
                                        .doNotRollbackAttempt()
                                        .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                        .cause(cause).build()));
                            } else {
                                return Mono.error(operationFailed(builder.cause(cause)
                                        .rollbackAttempt(rollback)
                                        .build()));
                            }
                        }
                    })

                    // Retry RetryOperation() exceptions
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY)
                    .publishOn(scheduler()) // after retryWhen triggers, it's on parallel scheduler

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    private <T> Mono<T> setExpiryOvertimeMode(String stage) {
        return Mono.fromRunnable(() -> {
            LOGGER.info(attemptId, "moving to expiry-overtime-mode in stage %s", stage);
            expiryOvertimeMode = true;
        });
    }

    private <T> Mono<T> setExpiryOvertimeModeAndFail(Throwable err, String stage, ErrorClass ec) {
        LOGGER.info(attemptId, "moving to expiry-overtime-mode in stage %s, and raising error", stage);
        expiryOvertimeMode = true;

        // This error should cause a rollback and then a TransactionExpired to be raised to app
        // But if any problem happens, then mapErrorInOvertimeToExpire logic should capture it, and also cause a
        // TransactionExpired to be raised to app
        return Mono.error(operationFailed(createError()
                .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                .cause(new AttemptExpiredException(err)).build()));
    }

    /**
     * Rolls back the transaction.  All staged replaces, inserts and removals will be removed.  The transaction will not
     * be retried, so this will be the final attempt.
     * <p>
     * The semantics are the same as {@link AttemptContext#rollback()}, except that the a <code>Mono</code> is returned
     * so the operation can be performed asynchronously.
     */
    public Mono<Void> rollback() {
        return createMonoBridge("rollback", Mono.defer(() -> {
            return waitForAllOpsThenDoUnderLock("app-rollback", attemptSpan,
                    () -> rollbackInternalLocked(true));
        }));
    }

    Mono<Void> rollbackAuto() {
        return createMonoBridge("rollbackAuto", Mono.defer(() -> {
            return waitForAllOpsThenDoUnderLock("auto-rollback", attemptSpan,
                    () -> rollbackInternalLocked(false));
        }));
    }

    /**
     * Rolls back the transaction.
     *
     * @param isAppRollback whether this is an app-rollback or auto-rollback
     */
    private Mono<Void> rollbackInternalLocked(boolean isAppRollback) {
        return Mono.defer(() -> {
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_ROLLBACK, attemptSpan);

            return Mono.defer(() -> {
                TransactionOperationFailedException returnEarly = canPerformRollback("rollbackInternal", isAppRollback);
                if (returnEarly != null) {
                    logger().info(attemptId, "rollback raising %s", DebugUtil.dbg(returnEarly));
                    return Mono.error(returnEarly);
                }

                int sb = TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED | TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED;
                setStateBits("rollback-" + (isAppRollback ? "app" : "auto"), sb, 0);

                // In queryMode we always ROLLBACK, as there is possibly delta table state to cleanup, and there may be an
                // ATR - we don't know
                if (state == AttemptState.NOT_STARTED && !queryModeUnlocked()) {
                    LOGGER.info(attemptId, "told to auto-rollback but in NOT_STARTED state, so nothing to do - skipping rollback");
                    return Mono.empty();
                }

                if (queryModeLocked()) {
                    return rollbackQueryLocked(isAppRollback, span);
                } else {
                    return rollbackWithKVLocked(isAppRollback, span);
                }
            }).doOnError(err -> span.finishWithErrorStatus())
                    .doOnTerminate(() -> span.finish());
        }).subscribeOn(scheduler());
    }

    private Mono<Void> rollbackWithKVLocked(boolean isAppRollback, SpanWrapper span) {
        return Mono.defer(() -> {
            assertLocked("rollbackWithKV");
            LOGGER.info(attemptId, "rollback %s expiryOvertimeMode=%s isAppRollback=%s",
                    this, expiryOvertimeMode, isAppRollback);

            // [EXP-ROLLBACK] - ignore expiries on singleAttempt, assumes we're doing this as already expired
            if (!expiryOvertimeMode && hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_ROLLBACK, Optional.empty())) {
                LOGGER.info(attemptId, "has expired before rollback, entering expiry-overtime mode");
                // Make one attempt to complete the rollback anyway
                expiryOvertimeMode = true;
            }

            if (!atrCollection.isPresent() || !atrId.isPresent()) {
                // no mutation, no need to rollback
                return Mono.create(s -> {
                    LOGGER.info(attemptId, "Calling rollback when it's had no mutations, so nothing to do");
                    // Leave state as it is (NOT_STARTED), to indicate that nothing has been written to cluster
                    s.success();
                });
            } else {
                return rollbackWithKVActual(isAppRollback, span);
            }
        });
    }

    private Mono<Void> rollbackWithKVActual(boolean isAppRollback, SpanWrapper span) {
        String prefix = "attempts." + attemptId;

        return atrAbortLocked(prefix, span, isAppRollback, false)

                .then(rollbackDocsLocked(isAppRollback, span))

                .then(atrRollbackCompleteLocked(isAppRollback, prefix, span))

                // [RETRY-ERR-ROLLBACK]
                .onErrorResume(err -> {
                    if (err instanceof ActiveTransactionRecordNotFoundException) {
                        LOGGER.info(attemptId, "ActiveTransactionRecordNotFound indicates that nothing needs " +
                                "to be done for this rollback: treating as successful rollback");
                        return Mono.empty();
                    } else {
                        return Mono.error(err); // propagate
                    }
                });
    }

    private Mono<Void> rollbackQueryLocked(boolean appRollback, SpanWrapper span) {
        return Mono.defer(() -> {
            assertLocked("rollbackQuery");

            int statementIdx = queryStatementIdx.getAndIncrement();

            return queryWrapperBlockingLocked(statementIdx,
                    queryContext.bucketName,
                    queryContext.scopeName,
                    "ROLLBACK",
                    Mapper.createObjectNode(),
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_ROLLBACK,
                    false, false,null, null, span, false, null, appRollback)

                    .then(Mono.fromRunnable(() -> {
                        setStateLocked(AttemptState.ROLLED_BACK);
                    }))

                    .onErrorResume(err -> {
                        span.recordExceptionAndSetErrorStatus(err);

                         if (err instanceof TransactionOperationFailedException) {
                             return Mono.error(err);
                        }

                        if (err instanceof AttemptNotFoundOnQueryException) {
                            // Indicates that query has already rolled back this attempt.
                            return Mono.empty();
                        } else {
                            TransactionOperationFailedException e = operationFailed(createError()
                                    .cause(err)
                                    .doNotRollbackAttempt()
                                    .build());
                            return Mono.error(e);
                        }
                    })

                    .then();
        });
    }

    private Mono<Void> atrRollbackCompleteLocked(boolean isAppRollback, String prefix, SpanWrapper pspan) {
        return Mono.defer(() -> {
            assertLocked("atrRollbackComplete");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_ROLLBACK, pspan);

            return Mono.defer(() -> {
                        LOGGER.info(attemptId, "removing ATR %s as rollback complete", getAtrDebug(atrCollection, atrId));

                        return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_ROLLBACK_COMPLETE, Optional.empty());
                    })

                    .then(hooks.beforeAtrRolledBack.apply(this))

                    .then(TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                            false, false, false, false, false, 0, durabilityLevel(), OptionsUtil.createClientContext(CoreTransactionAttemptContextHooks.HOOK_ATR_ROLLBACK_COMPLETE), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, prefix, null, false, true, false, 0)
                                    )))

                    .publishOn(scheduler())

                    .flatMap(v -> hooks.afterAtrRolledBack.apply(this).thenReturn(v)) // testing hook

                    .doOnNext(v -> {
                        setStateLocked(AttemptState.ROLLED_BACK);
                        long elapsed = span.elapsedMicros();
                        addUnits(v.flexibleExtras());
                        LOGGER.info(attemptId, "rollback - atr rolled back%s in %dus", DebugUtil.dbg(v.flexibleExtras()), elapsed);
                    })

                    .onErrorResume(err -> {
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        span.recordException(err);

                        LOGGER.info(attemptId, "error while marking ATR %s as rollback complete%s in %dus: %s",
                                getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder error = createError().doNotRollbackAttempt();

                        if (expiryOvertimeMode) {
                            return mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_ATR_ROLLBACK_COMPLETE, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                        } else if (ec == FAIL_EXPIRY) {
                            return Mono.error(operationFailed(isAppRollback, createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                                    .build()));
                        } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND
                                || ec == FAIL_DOC_NOT_FOUND) {
                            // Possibly the ATRs have been deleted and/or recreated, possibly we retried on a FAIL_AMBIGUOUS,
                            // either way, the entry has been removed.
                            return Mono.empty();
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(isAppRollback, error.build()));
                        } else {
                            return Mono.error(new RetryOperationException());
                        }
                    })

                    // Retry RetryOperation() exceptions
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY)
                    .publishOn(scheduler()) // after retryWhen triggers, it's on parallel scheduler

                    .then()

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    private Mono<Void> rollbackDocsLocked(boolean isAppRollback, SpanWrapper span) {
        return Mono.defer(() -> {
            return Flux.fromIterable(stagedMutationsLocked)
                    .publishOn(scheduler())

                    .concatMap(staged -> {
                        switch (staged.type) {
                            case INSERT:
                                return rollbackStagedInsertLocked(isAppRollback, span, staged.collection, staged.id, staged.cas);
                            default:
                                return rollbackStagedReplaceOrRemoveLocked(isAppRollback, span, staged.collection, staged.id, staged.cas);
                        }
                    })

                    .doOnNext(v -> {
                        LOGGER.info(attemptId, "rollback - docs rolled back");
                    })

                    .then();
        });
    }

    private Mono<Void> rollbackStagedReplaceOrRemoveLocked(boolean isAppRollback, SpanWrapper span, CollectionIdentifier collection, String id, long cas) {
        return Mono.defer(() -> {
            return Mono.defer(() -> {
                LOGGER.info(attemptId, "rolling back doc %s with cas %d by removing staged mutation",
                        DebugUtil.docId(collection, id), cas);
                return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC, Optional.of(id));
            })

                    .then(hooks.beforeDocRolledBack.apply(this, id)) // testing hook

                    .then(TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                            false, false, false, false, false, cas, durabilityLevel(), OptionsUtil.createClientContext("rollbackDoc"), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn", null, false, true, false, 0)
                            )))

                    .publishOn(scheduler())

                    .flatMap(updatedDoc -> hooks.afterRollbackReplaceOrRemove.apply(this, id) // Testing hook

                            .thenReturn(updatedDoc))

                    .doOnNext(updatedDoc -> {
                        addUnits(updatedDoc.flexibleExtras());
                        LOGGER.info(attemptId, "rolled back doc %s%s, got cas %d and mt %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(updatedDoc.flexibleExtras()), updatedDoc.cas(), updatedDoc.mutationToken());
                    })

                    .then()

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().doNotRollbackAttempt().cause(err);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        span.recordException(err);

                        logger().info(attemptId, "got error while rolling back doc %s%s in %dus: %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                        if (expiryOvertimeMode) {
                            return mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                        } else if (ec == FAIL_EXPIRY) {
                            return setExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC)
                                    .then(Mono.error(new RetryOperationException()));
                        } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                            LOGGER.info(attemptId,
                                    "got PATH_NOT_FOUND while cleaning up staged doc %s, it must have already been "
                                            + "rolled back, continuing",
                                    DebugUtil.docId(collection, id));
                            return Mono.empty();
                        } else if (ec == FAIL_DOC_NOT_FOUND) {
                            // Ultimately rollback has happened here
                            return Mono.empty();
                        } else if (ec == FAIL_CAS_MISMATCH) {
                            return handleDocChangedDuringRollback(span, id, collection,
                                    (newCas) -> rollbackStagedReplaceOrRemoveLocked(isAppRollback, span, collection, id, newCas));
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(isAppRollback, builder.doNotRollbackAttempt().build()));
                        } else {
                            return Mono.error(new RetryOperationException());
                        }
                    })

                    // Retry RetryOperation() exceptions
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY)
                    .publishOn(scheduler()) // after retryWhen triggers, it's on parallel scheduler

                    .doOnError(err -> span.setErrorStatus());
        });
    }

    private Mono<Void> rollbackStagedInsertLocked(boolean isAppRollback,
                                                       SpanWrapper span,
                                                       CollectionIdentifier collection,
                                                       String id,
                                                       long cas) {
        return Mono.defer(() -> {
            return Mono.defer(() -> {
                LOGGER.info(attemptId, "rolling back staged insert %s with cas %d",
                        DebugUtil.docId(collection, id), cas);
                return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_DELETE_INSERTED, Optional.of(id));
            })

                    .then(hooks.beforeRollbackDeleteInserted.apply(this, id))

                    .then(TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                            false, false, false, true, false, cas, durabilityLevel(), OptionsUtil.createClientContext("rollbackStagedInsert"), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn", null, false, true, false, 0)
                            )))

                    .publishOn(scheduler())

                    // Testing hook
                    .flatMap(updatedDoc -> hooks.afterRollbackDeleteInserted.apply(this, id)

                            .thenReturn(updatedDoc))

                    .doOnNext(updatedDoc -> {
                        addUnits(updatedDoc.flexibleExtras());
                        LOGGER.info(attemptId, "deleted inserted doc %s%s, mt %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(updatedDoc.flexibleExtras()), updatedDoc.mutationToken());
                    })

                    .then()

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().cause(err);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        span.recordException(err);

                        LOGGER.info(attemptId, "error while rolling back inserted doc %s%s in %dus: %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                        if (expiryOvertimeMode) {
                            return mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_REMOVE_DOC, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                        } else if (ec == FAIL_EXPIRY) {
                            return setExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_REMOVE)
                                    .then(Mono.error(new RetryOperationException()));
                        } else if (ec == FAIL_DOC_NOT_FOUND
                                || ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                            LOGGER.info(attemptId,
                                    "got %s while removing staged insert doc %s, it must "
                                            + "have already been rolled back, continuing",
                                    ec, DebugUtil.docId(collection, id));
                            return Mono.empty();
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(isAppRollback, builder.doNotRollbackAttempt().build()));
                        } else if (ec == FAIL_CAS_MISMATCH) {
                            return handleDocChangedDuringRollback(span, id, collection,
                                    (newCas) -> rollbackStagedInsertLocked(isAppRollback, span, collection, id, newCas));
                        } else {
                            return Mono.error(new RetryOperationException());
                        }
                    })

                    // Retry RetryOperation() exceptions
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY)
                    .publishOn(scheduler()) // after retryWhen triggers, it's on parallel scheduler

                    .doOnError(err -> span.setErrorStatus());
        });
    }

    /**
     * Very similar to rollbackStagedInsert, but with slightly different requirements as it's not performed
     * during rollback.
     */
    private Mono<Void> removeStagedInsert(CoreTransactionGetResult doc, SpanWrapper span) {
        return Mono.defer(() -> {
            assertNotLocked("removeStagedInsert");
            CollectionIdentifier collection = doc.collection();
            String id = doc.id();

            return Mono.defer(() -> {
                LOGGER.info(attemptId, "removing staged insert %s with cas %d",
                        DebugUtil.docId(collection, id), doc.cas());

                if (hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_REMOVE_STAGED_INSERT, Optional.of(id))) {
                    return Mono.error(operationFailed(createError()
                            .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                            .doNotRollbackAttempt()
                            .cause(new AttemptExpiredException("Attempt has expired in stage " + CoreTransactionAttemptContextHooks.HOOK_REMOVE_STAGED_INSERT))
                            .build()));
                } else {
                    return Mono.empty();
                }
            })

                    .then(hooks.beforeRemoveStagedInsert.apply(this, id))

                    .then(TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                            false, false, false, true, false, doc.cas(), durabilityLevel(), OptionsUtil.createClientContext("removeStagedInsert"), span,
                            Arrays.asList(
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn", null, false, true, false, 0)
                            )))

                    .publishOn(scheduler())

                    .flatMap(updatedDoc -> hooks.afterRemoveStagedInsert.apply(this, id)
                            .thenReturn(updatedDoc))

                    .doOnNext(v -> addUnits(v.flexibleExtras()))

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError()
                                .retryTransaction()
                                .cause(err);
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        span.recordException(err);

                        LOGGER.info(attemptId, "error while removing staged insert doc %s%s in %dus: %s",
                                DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                        if (ec == TRANSACTION_OPERATION_FAILED) {
                            return Mono.error(err);
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(builder.doNotRollbackAttempt().build()));
                        } else {
                            return Mono.error(operationFailed(builder.build()));
                        }
                    })

                    .flatMap(v -> {
                        // Save so the subsequent KV ops can be done with the correct CAS
                        doc.cas(v.cas());
                        long elapsed = span.elapsedMicros();
                        LOGGER.info(attemptId, "removed staged insert from doc %s in %dus", DebugUtil.docId(collection, id), elapsed);

                        return doUnderLock("removeStagedInsert " + DebugUtil.docId(collection, id),
                                () -> Mono.fromRunnable(()-> {
                                    removeStagedMutationLocked(doc.collection(), doc.id());
                                }));
                    })

                    .then();
        });
    }

    private Mono<Void> atrAbortLocked(String prefix,
                                      SpanWrapper pspan,
                                      boolean isAppRollback,
                                      boolean ambiguityResolutionMode) {
        return Mono.defer(() -> {
            assertLocked("atrAbort");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_ABORT, pspan);

            ArrayList<SubdocMutateRequest.Command> specs = new ArrayList<>();

            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_STATUS, serialize(AttemptState.ABORTED.name()), false, true, false, 0));
            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_TIMESTAMP_ROLLBACK_START, serialize("${Mutation.CAS}"), false, true, true, 1));

            specs.addAll(addDocsToBuilder(specs.size()));

            return Mono.defer(() -> {
                        LOGGER.info(attemptId, "aborting ATR %s isAppRollback=%s ambiguityResolutionMode=%s",
                                getAtrDebug(atrCollection, atrId), isAppRollback, ambiguityResolutionMode);
                        return errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_ABORT, Optional.empty());
                    })

                    .then(hooks.beforeAtrAborted.apply(this)) // testing hook

                    .then(TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                            false, false, false, false, false, 0, durabilityLevel(), OptionsUtil.createClientContext("atrAbort"), span, specs))

                    .publishOn(scheduler())

                    // Debug hook
                    .flatMap(v -> hooks.afterAtrAborted.apply(this).thenReturn(v))

                    .doOnNext(v -> {
                        setStateLocked(AttemptState.ABORTED);
                        addUnits(v.flexibleExtras());
                        LOGGER.info(attemptId, "aborted ATR %s%s in %dus", getAtrDebug(atrCollection, atrId),
                                DebugUtil.dbg(v.flexibleExtras()), span.elapsedMicros());
                    })

                    .then()

                    .onErrorResume(err -> {
                        ErrorClass ec = classify(err);
                        TransactionOperationFailedException.Builder builder = createError().cause(err).doNotRollbackAttempt();
                        MeteringUnits units = addUnits(MeteringUnits.from(err));
                        span.recordException(err);

                        LOGGER.info(attemptId, "error %s while aborting ATR %s%s",
                                DebugUtil.dbg(err), getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units));

                        if (expiryOvertimeMode) {
                            return mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_ATR_ABORT, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                        } else if (ec == FAIL_EXPIRY) {
                            return setExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_ABORT)
                                    .then(Mono.error(new RetryOperationException()));
                        } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                            return Mono.error(operationFailed(isAppRollback, builder.cause(new ActiveTransactionRecordEntryNotFoundException(atrId.get(), attemptId)).build()));
                        } else if (ec == FAIL_DOC_NOT_FOUND) {
                            return Mono.error(operationFailed(isAppRollback, builder.cause(new ActiveTransactionRecordNotFoundException(atrId.get(), attemptId)).build()));
                        } else if (ec == FAIL_ATR_FULL) {
                            return Mono.error(operationFailed(isAppRollback, builder.cause(new ActiveTransactionRecordFullException(err)).build()));
                        } else if (ec == FAIL_HARD) {
                            return Mono.error(operationFailed(isAppRollback, builder.doNotRollbackAttempt().build()));
                        } else {
                            return Mono.error(new RetryOperationException());
                        }
                    })

                    // Will retry RetryOperation errors
                    .retryWhen(RETRY_OPERATION_UNTIL_EXPIRY)
                    .publishOn(scheduler()) // after retryWhen triggers, it's on parallel scheduler

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    private void assertLocked(String dbg) {
        if (threadSafetyEnabled && !mutex.isLocked()) {
            throw new IllegalStateException("Internal bug hit: mutex must be locked in " + dbg + " but isn't");
        }
    }

    private void assertNotLocked(String dbg) {
        if (threadSafetyEnabled && mutex.debugAsSingleThreaded() && mutex.isLocked()) {
            throw new IllegalStateException("Internal bug hit: mutex must be unlocked in " + dbg + " but isn't");
        }
    }

    private void assertNotQueryMode(String dbg) {
        if (queryModeLocked()) {
            throw new IllegalStateException("Internal bug hit: must not be in queryMode in " + dbg);
        }
    }

    @Nullable
    TransactionOperationFailedException canPerformOperation(String dbg) {
        return canPerformOperation(dbg, true);
    }

    /**
     * @param canPerformPendingCheck permits rollback
     */
    @Nullable
    TransactionOperationFailedException canPerformOperation(String dbg, boolean canPerformPendingCheck) {
        // Note that this doesn't have to be locked, it's an atomic/volatile variable.
        // Usually it's checked under lock anyway, but there is a specific time (implicit commit) where we can avoid
        // otherwise locking just to check this.
        switch (state) {
            case NOT_STARTED:
            case PENDING:
                // Due to canPerformCommit check we can be confident this is not a double-commit but definitely down to PreviousOperationFailed
                // Update: could also be an empty transaction that's committed. But as that's pretty niche we'll continue to report the most probable case.
                if (canPerformPendingCheck && hasStateBit(TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED)) {
                    logger().info(attemptId, "failing operation %s as not allowed to commit (probably as previous operations have failed)", dbg);
                    // Does not pass through operationFailed as informational
                    return createError()
                            .cause(new PreviousOperationFailedException())
                            .build();
                }
                break;

            case COMMITTED:
            case COMPLETED:
                // Does not pass through operationFailed as informational
                return createError()
                        .cause(new TransactionAlreadyCommittedException())
                        .doNotRollbackAttempt()
                        .build();

            case ABORTED:
            case ROLLED_BACK:
                // Does not pass through operationFailed as informational
                return createError()
                        .cause(new TransactionAlreadyAbortedException())
                        .doNotRollbackAttempt()
                        .build();
        }

        // Operation is allowed
        return null;
    }

    @Nullable
    TransactionOperationFailedException canPerformRollback(String dbg, boolean appRollback) {
        if (appRollback && hasStateBit(TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED)) {
            LOGGER.info(attemptId, "state bits indicate app-rollback is not allowed");
            // Does not pass through operationFailed as informational
            return createError()
                    .cause(new RollbackNotPermittedException())
                    .doNotRollbackAttempt()
                    .build();
        }

        TransactionOperationFailedException out = canPerformOperation(dbg, false);
        if (out != null) {
            return out;
        }

        return null;
    }

    @Nullable
    TransactionOperationFailedException canPerformCommit(String dbg) {
        if (hasStateBit(TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED)) {
            LOGGER.info(attemptId, "state bits indicate commit is not allowed");
            // Does not pass through operationFailed as informational
            return createError()
                    .cause(new CommitNotPermittedException())
                    .doNotRollbackAttempt()
                    .build();
        }

        TransactionOperationFailedException out = canPerformOperation(dbg);
        if (out != null) {
            return out;
        }

        return null;
    }

    private boolean hasStateBit(int stateBit) {
        return (stateBits.get() & stateBit) != 0;
    }

    private void setStateBits(String dbg, int newBehaviourFlags, int newFinalErrorToRaise) {
        int oldValue = stateBits.get();
        int newValue = oldValue | newBehaviourFlags;
        // Only save the new ToRaise if it beats what's there now
        if (newFinalErrorToRaise > ((oldValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR)) {
            newValue = (newValue & STATE_BITS_MASK_BITS) | (newFinalErrorToRaise << STATE_BITS_POSITION_FINAL_ERROR);
        }
        while (!stateBits.compareAndSet(oldValue, newValue)) {
            oldValue = stateBits.get();
            newValue = oldValue | newBehaviourFlags;
            if (newFinalErrorToRaise > ((oldValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR)) {
                newValue = (newValue & STATE_BITS_MASK_BITS) | (newFinalErrorToRaise << STATE_BITS_POSITION_FINAL_ERROR);
            }
        }

        boolean wasShouldNotRollback = (oldValue & TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK) != 0;
        boolean wasShouldNotRetry = (oldValue & TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY) != 0;
        boolean wasShouldNotCommit = (oldValue & TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED) != 0;
        boolean wasAppRollbackNotAllowed = (oldValue & TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED) != 0;
        FinalErrorToRaise wasToRaise = FinalErrorToRaise.values()[(oldValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR];

        boolean shouldNotRollback = (newValue & TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK) != 0;
        boolean shouldNotRetry = (newValue & TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY) != 0;
        boolean shouldNotCommit = (newValue & TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED) != 0;
        boolean appRollbackNotAllowed = (newValue & TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED) != 0;
        FinalErrorToRaise toRaise = FinalErrorToRaise.values()[(newValue & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR];

        StringBuilder sb = new StringBuilder("changed state bits in ").append(dbg).append(", changed");

        if (wasShouldNotRollback != shouldNotRollback) sb.append(" shouldNotRollback to ").append(shouldNotRollback);
        if (wasShouldNotRetry != shouldNotRetry) sb.append(" shouldNotRetry to ").append(shouldNotRetry);
        if (wasShouldNotCommit != shouldNotCommit) sb.append(" shouldNotCommit to ").append(shouldNotCommit);
        if (wasAppRollbackNotAllowed != appRollbackNotAllowed) sb.append(" appRollbackNotAllowed to ").append(appRollbackNotAllowed);
        if (wasToRaise != toRaise) sb.append(" toRaise from ").append(wasToRaise).append(" to ").append(toRaise);

        LOGGER.info(attemptId, sb.toString());
    }

    /**
     * Rollback errors rules:
     * Errors during auto-rollback: do not update internal state, as the user cares more about the original error that provoked the rollback
     * Errors during app-rollback:  do update internal state.  Nothing else has gone wrong with the transaction, and the user will care about rollback problems.
     *
     * If !updateInternalState, the internal state bits are not changed.
     */
    public TransactionOperationFailedException operationFailed(boolean updateInternalState, TransactionOperationFailedException err) {
        if (updateInternalState) {
            return operationFailed(err);
        }

        return err;
    }

    public TransactionOperationFailedException operationFailed(TransactionOperationFailedException err) {
        int sb = TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED;

        if (!err.autoRollbackAttempt()) {
            sb |= TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK;
        }

        if (!err.retryTransaction()) {
            sb |= TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY;
        }

        setStateBits("operationFailed", sb, err.toRaise().ordinal());

        return err;
    }

    AttemptState state() {
        return state;
    }

    boolean queryModeLocked() {
        assertLocked("queryMode");
        return queryContext != null;
    }

    /**
     * Prefer queryModeLocked.  This method exists for a couple of places where it is reasonably
     * safe to be non-threaded, including after the lambda is done and on FAIL_HARD (only done by tests).
     * Though technically the user could have scheduled an op to occur after the lambda is done, that would be an app bug.
     */
    boolean queryModeUnlocked() {
        return queryContext != null;
    }

    /**
     * Query will ignore the timeout of any operations sent inside a transaction, but the SDK will not.  So set a
     * safety timeout that's higher than when the server should return.  In particular want to avoid raising a
     * TransactionCommitAmbiguous where we can.
     *
     * <p>
     * Worse case timeout should be gocbcore starting a KV op just before transaction expiry, that times out.  Plus
     * network latency.
     */
    private Duration queryTimeout() {
        return Duration.ofMillis(expiryRemainingMillis())
                .plus(core.context().environment().timeoutConfig().kvDurableTimeout())
                .plusSeconds(1);
    }

    /**
     * A very low-level query call, see queryWrapper for a call that does useful logging wrapping and basic error
     * handling.
     */
    private Mono<QueryResponse> queryInternal(final int sidx,
                                              @Nullable final String bucketName,
                                              @Nullable final String scopeName,
                                              final String statement,
                                              final ObjectNode options,
                                              @Nullable final SpanWrapper pspan,
                                              final boolean tximplicit) {
        return Mono.defer(() -> {
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.SPAN_REQUEST_QUERY, pspan);

            return hooks.beforeQuery.apply(this, statement)

                    .then(Mono.defer(() -> {
                        assertNotLocked("queryInternal");

                        // The metrics are invaluable for debugging, override user's setting
                        options.put("metrics", true);

                        if (tximplicit) {
                            options.put("tximplicit", true);
                        }

                        if (bucketName != null && scopeName != null) {
                            String queryContext = QueryRequest.queryContext(bucketName, scopeName);
                            options.put("query_context", queryContext);
                        }

                        if (tximplicit) {

                            // Workaround for MB-50914 on older server versions
                            if (!options.has("scan_consistency")) {
                                options.put("scan_consistency", "request_plus");
                            }

                            applyQueryOptions(config, options, expiryRemainingMillis());
                        }

                        logger().info(attemptId, "q%d using query params %s", sidx, options.toString());

                        options.put("statement", statement);

                        byte[] encoded;
                        try {
                            encoded = Mapper.writer().writeValueAsBytes(options);
                        } catch (JsonProcessingException e) {
                            throw new EncodingFailureException(e);
                        }
                        boolean readonly = options.path("readonly").asBoolean(false);

                        QueryRequest request = new QueryRequest(queryTimeout(),
                                core.context(),
                                BestEffortRetryStrategy.INSTANCE,
                                core.context().authenticator(),
                                statement,
                                encoded,
                                readonly,
                                "query",
                                span.span(),
                                bucketName,
                                scopeName,
                                queryContext == null ? null : queryContext.queryTarget);

                        // TODO ESI adhoc (with adhoc=false) get null lastDispatchedToNode
                        return coreQueryAccessor.query(request, true)
                                .publishOn(scheduler())
                                .doOnNext(v -> {
                                    if (queryContext == null) {
                                        queryContext = new QueryContext(request.context().lastDispatchedToNode(), bucketName, scopeName);
                                        logger().info(attemptId, "q%d got query node id %s", sidx, RedactableArgument.redactMeta(queryContext.queryTarget));
                                    }
                                    request.context().logicallyComplete();
                                })
                                // This finishs the span
                                .doOnError(err -> request.context().logicallyComplete(err));
                    }))

                    .flatMap(result -> hooks.afterQuery.apply(this, statement)
                            .thenReturn(result));
        });
    }

    /**
     * This will return a TransactionOperationFailedException if that's what query returns, else will return the error after
     * passing through convertQueryError.
     * <p>
     * If an error is universally handled the same way, then convertQueryError will return a TransactionOperationFailedException.
     * <p>
     * Because this is a general-purpose wrapper, TransactionOperationFaileds will _not_ be added to internal errors array.
     * So the transaction is allowed to continue after failures, unless the caller prevents it.  Update: appears to no
     * longer be the case.
     *
     * Keep this routine in sync with queryWrapperReactiveLocked.
     *
     * @param lockToken if null, the lock is held throughout
     */
    public Mono<QueryResponse> queryWrapperLocked(final int sidx,
                                                   @Nullable final String bucketName,
                                                   @Nullable final String scopeName,
                                                   final String statement,
                                                   final ObjectNode options,
                                                   final String hookPoint,
                                                   final boolean isBeginWork,
                                                   final boolean existingErrorCheck,
                                                   @Nullable final ObjectNode txdata,
                                                   @Nullable final ArrayNode params,
                                                   @Nullable final SpanWrapper span,
                                                   final boolean tximplicit,
                                                   AtomicReference<ReactiveLock.Waiter> lockToken,
                                                   final boolean updateInternalState) {
        return Mono.defer(() -> {
            assertLocked("queryWrapper q" + sidx);

            long start = System.nanoTime();

            logger().debug(attemptId, "q%d: '%s' params=%s txdata=%s tximplicit=%s", sidx,
                    RedactableArgument.redactUser(statement), RedactableArgument.redactUser(params), RedactableArgument.redactUser(txdata), tximplicit);

            Mono<Void> beginWorkIfNeeded = Mono.empty();

            if (!tximplicit && !queryModeLocked() && !isBeginWork) {
                // Thread-safety 7.6: need to unlock (to avoid deadlocks), wait for all KV ops, lock
                beginWorkIfNeeded = beginWorkIfNeeded(sidx, bucketName, scopeName, statement, lockToken, span);
            }

            if (txdata != null) {
                options.set("txdata", txdata);
            }

            // Important: Keep this routine in sync with queryWrapperReactiveLocked, which is an unavoidable C&P.

            return beginWorkIfNeeded

                    .then(queryInternalPreLocked(sidx, statement, hookPoint, existingErrorCheck))

                    .then(Mono.defer(() -> {
                        if (!tximplicit && !isBeginWork) {
                            options.put("txid", attemptId);
                        }
                        return queryInternal(sidx, bucketName, scopeName, statement, options, span, tximplicit);
                    }));
        });
    }

    // This is basically a QueryResult, which we don't have in core
    @Stability.Internal
    public static class BufferedQueryResponse {
        public final QueryChunkHeader header;
        public final List<QueryChunkRow> rows;
        public final QueryChunkTrailer trailer;

        BufferedQueryResponse(QueryChunkHeader header, List<QueryChunkRow> rows, QueryChunkTrailer trailer) {
            this.header = header;
            this.rows = rows;
            this.trailer = trailer;
        }
    }

    // As per CBD-4594, we discovered after release of the API that it's not (easily) possible to support
    // streaming from ctx.query(), as errors will not be handled.  For example gocbcore could
    // report that the transaction should retry, and this would be missed and a generic fast-fail
    // reported at COMMIT time instead.  So, buffer the rows internally so errors are definitely
    // caught.
    private Mono<BufferedQueryResponse> queryWrapperBlockingLocked(final int sidx,
                                                   @Nullable final String bucketName,
                                                   @Nullable final String scopeName,
                                                   final String statement,
                                                   final ObjectNode options,
                                                   final String hookPoint,
                                                   final boolean isBeginWork,
                                                   final boolean existingErrorCheck,
                                                   @Nullable final ObjectNode txdata,
                                                   @Nullable final ArrayNode params,
                                                   @Nullable final SpanWrapper span,
                                                   final boolean tximplicit,
                                                   AtomicReference<ReactiveLock.Waiter> lockToken,
                                                   final boolean updateInternalState) {
        return Mono.defer(() -> {
            return queryWrapperLocked(sidx,
                    bucketName,
                    scopeName,
                    statement,
                    options,
                    hookPoint,
                    isBeginWork,
                    existingErrorCheck,
                    txdata,
                    params,
                    span,
                    tximplicit,
                    lockToken,
                    updateInternalState)

                    .flatMap(result -> {
                        return result.rows()
                                .collectList()
                                .flatMap(rows -> result.trailer()
                                        .onErrorResume(err -> {
                                            RuntimeException converted = convertQueryError(sidx, err, true);
                                            long elapsed = span.elapsedMicros();

                                            logger().warn(attemptId, "q%d got error on rows stream %s after %dus, converted from %s", sidx, dbg(converted),
                                                    elapsed, dbg(err));

                                            if (converted != null) {
                                                return Mono.error(converted);
                                            }

                                            return Mono.error(err);
                                        })
                                        .map(trailer -> new BufferedQueryResponse(result.header(), rows, trailer)));
                    })

                    .onErrorResume(err -> {
                        RuntimeException converted = convertQueryError(sidx, err, updateInternalState);
                         long elapsed = span.elapsedMicros();

                        logger().warn(attemptId, "q%d got error %s after %dus, converted from %s", sidx, dbg(converted),
                                elapsed, dbg(err));

                        if (converted != null) {
                            return Mono.error(converted);
                        }

                        return Mono.error(err);
                    })

                    .flatMap(result -> {
                        long elapsed = span.elapsedMicros();

                        try {
                            JsonNode metrics = Mapper.reader().readValue(result.trailer.metrics().get(), JsonNode.class);
                            logger().info(attemptId, "q%d returned with metrics %s after %dus", sidx,
                                    metrics, elapsed);
                        } catch (IOException e) {
                            logger().info(attemptId, "q%d returned after %dus, unable to parse metrics %s", sidx,
                                    elapsed, e);
                        }


                        if (result.trailer.status().equals("FATAL")) {
                            TransactionOperationFailedException err = operationFailed(updateInternalState, createError().build());
                            return Mono.error(err);
                        } else {
                            return Mono.just(result);
                        }
                    });
        });
    }

    private Mono<Void> beginWorkIfNeeded(int sidx,
                                         @Nullable final String bucketName,
                                         @Nullable final String scopeName,
                                         String statement,
                                         AtomicReference<ReactiveLock.Waiter> lockToken,
                                         SpanWrapper span) {
        // Thread-safety 7.6: need to unlock (to avoid deadlocks), wait for all KV ops, lock
        return hooks.beforeUnlockQuery.apply(this, statement) // test hook

                .then(unlock(lockToken.get(), "before BEGIN WORK q" + sidx))

                .then(waitForAllKVOpsThenLock("queryWrapper q" + sidx))

                .flatMap(newLockToken -> {
                    lockToken.set(newLockToken);
                    boolean stillNeedsBeginWork = !queryModeLocked();
                    LOGGER.info(attemptId, "q%d after reacquiring lock stillNeedsBeginWork=%s", sidx, stillNeedsBeginWork);
                    if (!queryModeLocked()) {
                        return queryBeginWorkLocked(bucketName, scopeName, span);
                    } else {
                        return Mono.empty();
                    }
                });
    }
    
    /**
     * Converts raw query error codes into useful exceptions.
     * <p>
     * Once query is returning TransactionOperationFailedException details, this can also raise a TransactionOperationFailedException.
     */
    private RuntimeException convertQueryError(int sidx, Throwable err, boolean updateInternalState) {
        RuntimeException out = QueryUtil.convertQueryError(err);

        if (out instanceof TransactionOperationFailedException) {
            return operationFailed(updateInternalState, (TransactionOperationFailedException) out);
        }

        return out;
    }

    private static ObjectNode applyQueryOptions(CoreMergedTransactionConfig config, ObjectNode options, long txtimeout) {
        // The BEGIN WORK scan consistency determines the default for all queries in the transaction.
        String scanConsistency = null;

        if (config.scanConsistency().isPresent()) {
            scanConsistency = config.scanConsistency().get();
        }

        // If not set, will default (in query) to REQUEST_PLUS
        if (scanConsistency != null) {
            options.put("scan_consistency", scanConsistency);
        }

        // http://src.couchbase.org/source/xref/cheshire-cat/goproj/src/github.com/couchbase/query/server/http/service_request.go search '// Request argument names'
        DurabilityLevel durabilityLevel = config.durabilityLevel();
        String durabilityLevelString;
        switch (durabilityLevel) {
            case NONE:
                durabilityLevelString = "none";
                break;
            case MAJORITY:
                durabilityLevelString = "majority";
                break;
            case MAJORITY_AND_PERSIST_TO_ACTIVE:
                durabilityLevelString = "majorityAndPersistActive";
                break;
            case PERSIST_TO_MAJORITY:
                durabilityLevelString = "persistToMajority";
                break;
            default:
                throw new IllegalArgumentException("Unknown durability level " + durabilityLevel);
        }

        options.put("durability_level", durabilityLevelString);
        options.put("txtimeout", txtimeout + "ms");
        config.metadataCollection().ifPresent(metadataCollection -> {
            options.put("atrcollection",
                    String.format("`%s`.`%s`.`%s`",
                            metadataCollection.bucket(),
                            metadataCollection.scope().orElse(DEFAULT_SCOPE),
                            metadataCollection.collection().orElse(DEFAULT_COLLECTION)));
        });
        options.put("numatrs", config.numAtrs());

        return options;
    }

    private Mono<Void> queryBeginWorkLocked(@Nullable final String bucketName,
                                            @Nullable final String scopeName,
                                            final SpanWrapper span) {
        return Mono.defer(() -> {
            assertLocked("queryBeginWork");

            ObjectNode txdata = makeQueryTxDataLocked();

            ObjectNode options = Mapper.createObjectNode();
            applyQueryOptions(config, options, expiryRemainingMillis());

            String statement = "BEGIN WORK";
            int statementIdx = queryStatementIdx.getAndIncrement();

            // Using a null lockToken will keep it locked throughout BEGIN WORK.
            // Update: no longer important as the body of queryWrapper does not unlock anymore.
            return queryWrapperBlockingLocked(statementIdx,
                    bucketName,
                    scopeName,
                    statement,
                    options,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_BEGIN_WORK,
                    true,
                    true,
                    txdata,
                    null,
                    span,
                    false,
                    null,
                    true)

                    .doOnNext(v -> {
                        assertLocked("beginWork");
                        // Query/gocbcore will maintain the mutations now
                        stagedMutationsLocked.clear();
                        if (queryContext == null) {
                            throw operationFailed(TransactionOperationFailedException.Builder.createError()
                                    .cause(new IllegalAccessError("Internal error: Must have a queryTarget after BEGIN WORK"))
                                    .build());
                        }
                    })

                    .then();
        });
    }

    /**
     * Used by AttemptContext, buffers all query rows in-memory.
     */

    public Mono<BufferedQueryResponse> queryBlocking(final String statement,
                                                     @Nullable final String bucketName,
                                                     @Nullable final String scopeName,
                                                     final ObjectNode options,
                                                     final boolean tximplicit) {
        return doQueryOperation("query blocking",
                statement,
                attemptSpan,
                (sidx, lockToken, span) -> {
                    if (tximplicit) {
                        span.attribute(TracingIdentifiers.ATTR_TRANSACTION_SINGLE_QUERY, true);
                    }
                    return queryWrapperBlockingLocked(sidx, bucketName, scopeName, statement, options, CoreTransactionAttemptContextHooks.HOOK_QUERY, false, true, null, null, span, tximplicit, lockToken, true);
                });
    }

    /**
     * This pre-amble will be called before literally any query.
     *
     * @param existingErrorCheck exists so we can call ROLLBACK even in the face of there being an existing error
     */
    Mono<Void> queryInternalPreLocked(final int sidx, final String statement, final String hookPoint, final boolean existingErrorCheck) {
        return Mono.defer(() -> {
            assertLocked("queryInternalPre");

            if (existingErrorCheck) {
                TransactionOperationFailedException returnEarly = canPerformOperation("queryInternalPre " + sidx);
                if (returnEarly != null) {
                    return Mono.error(returnEarly);
                }
            }

            long remaining = expiryRemainingMillis();
            boolean expiresSoon = remaining < EXPIRY_THRESHOLD;

            // Still call hasExpiredClientSide, so we can inject expiry from tests
            if (hasExpiredClientSide(hookPoint, Optional.of(statement)) || expiresSoon) {
                logger().info(attemptId, "transaction has expired in stage '%s' remaining=%d threshold=%d",
                    hookPoint, remaining, EXPIRY_THRESHOLD);

                return Mono.error(operationFailed(createError()
                        .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                        .doNotRollbackAttempt()
                        .build()));
            }

            return Mono.empty();
        });
    }

    private List<DocRecord> toDocRecords(final List<StagedMutation> mutations) {
        return mutations.stream()
                .map(m -> new DocRecord(m.collection.bucket(),
                        m.collection.scope().orElse(DEFAULT_SCOPE),
                        m.collection.collection().orElse(DEFAULT_COLLECTION),
                        m.id))
                .collect(Collectors.toList());
    }


    @Stability.Internal
    private Mono<Void> addCleanup(@Nullable CoreTransactionsCleanup cleanup) {
        return Mono.fromRunnable(() -> {
            CleanupRequest cleanupRequest = createCleanupRequestIfNeeded(cleanup);
            if (cleanupRequest != null && cleanup != null) {
                cleanup.add(cleanupRequest);
            }
        });
    }

    @Stability.Internal
    Mono<Void> lambdaEnd(@Nullable CoreTransactionsCleanup cleanup, @Nullable Throwable err, boolean singleQueryTransactionMode) {
        return Mono.defer(() -> {
            int sb = stateBits.get();
            boolean shouldNotRollback = (sb & TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK) != 0;
            int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
            FinalErrorToRaise finalError = FinalErrorToRaise.values()[maskedFinalError];
            boolean rollbackNeeded = finalError != FinalErrorToRaise.TRANSACTION_SUCCESS && !shouldNotRollback && !singleQueryTransactionMode;

            LOGGER.info(attemptId, "reached post-lambda in %dus, shouldNotRollback=%s finalError=%s rollbackNeeded=%s, err (only cause of this will be used)=%s tximplicit=%s%s",
                    TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTimeClient.toNanos()), shouldNotRollback,
                    finalError, rollbackNeeded, err, singleQueryTransactionMode,
                    // Don't display the aggregated units if queryMode as it won't be accurate
                    queryModeUnlocked() ? "" : meteringUnitsBuilder.toString());

            core.transactionsContext().counters().attempts().incrementBy(1);

            return Mono.defer(() -> {
                        if (rollbackNeeded) {
                            return rollbackAuto()
                                    .onErrorResume(er -> {
                                        // If rollback fails, want to raise original error as cause, but with retry=false.
                                        overall.LOGGER.info(attemptId, "rollback failed with %s. Original error will be raised as cause, and retry should be disabled", DebugUtil.dbg(er));
                                        setStateBits("lambdaEnd", TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY, 0);
                                        return Mono.empty();
                                    });
                        }

                        return Mono.empty();
                    })
                    // Only want to add the attempt after doing the rollback, so the attempt has the correct state (hopefully
                    // ROLLED_BACK)
                    .then(addCleanup(cleanup))
                    .doOnTerminate(() -> {
                        if (err != null) {
                            attemptSpan.finishWithErrorStatus();
                        }
                        else {
                            attemptSpan.finish();
                        }
                    })
                    .then(Mono.defer(() -> retryIfRequired(err)));
        });
    }

    private Mono<Void> retryIfRequired(Throwable err) {
        int sb = stateBits.get();
        boolean shouldNotRetry = (sb & TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY) != 0;
        int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
        FinalErrorToRaise finalError = FinalErrorToRaise.values()[maskedFinalError];
        boolean retryNeeded = finalError != FinalErrorToRaise.TRANSACTION_SUCCESS && !shouldNotRetry;

        if (retryNeeded) {
            if (hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_BEFORE_RETRY, Optional.empty())) {
                // This will set state bits and raise a cause for transactionEnd
                return Mono.error(operationFailed(createError()
                        .doNotRollbackAttempt()
                        .raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED)
                        .build()));
            }
        }

        LOGGER.info(attemptId, "reached end of lambda post-rollback (if needed), shouldNotRetry=%s finalError=%s retryNeeded=%s",
                shouldNotRetry, finalError, retryNeeded);

        if (retryNeeded) {
            return Mono.error(new RetryTransactionException());
        }

        if (err != null) {
            return Mono.error(err);
        }

        return Mono.empty();
    }

    @Stability.Internal
    Mono<CoreTransactionResult> transactionEnd(@Nullable Throwable err, boolean singleQueryTransactionMode) {
        return Mono.defer(() -> {
            boolean unstagingComplete = state == AttemptState.COMPLETED;

            CoreTransactionResult result = new CoreTransactionResult(overall.LOGGER,
                    Duration.ofNanos(System.nanoTime() - overall.startTimeClient()),
                    overall.transactionId(),
                    unstagingComplete);

            int sb = stateBits.get();
            int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
            FinalErrorToRaise finalError = FinalErrorToRaise.values()[maskedFinalError];

            LOGGER.info(attemptId, "reached end of transaction, toRaise=%s, err=%s", finalError, err);

            core.transactionsContext().counters().transactions().incrementBy(1);

            Throwable cause = null;
            if (err != null) {
                if (!(err instanceof TransactionOperationFailedException)) {
                    if (!singleQueryTransactionMode) {
                        // A bug.  Only TransactionOperationFailedException is allowed to reach here.
                        logger().info(attemptId, "Non-TransactionOperationFailedException '" + DebugUtil.dbg(err) + "' received, this is a bug");
                    }
                } else {
                    TransactionOperationFailedException e = (TransactionOperationFailedException) err;
                    cause = e.getCause();
                }
            }

            Throwable ret = null;

            switch (finalError) {
                case TRANSACTION_FAILED_POST_COMMIT:
                    break;

                case TRANSACTION_SUCCESS:
                    if (singleQueryTransactionMode) {
                        ret = err;
                    }
                    break;

                case TRANSACTION_EXPIRED: {
                    String msg = "Transaction has expired configured timeout of " + overall.expirationTime().toMillis() + "ms.  The transaction is not committed.";
                    ret = new CoreTransactionExpiredException(cause, logger(), overall.transactionId(), msg);
                    break;
                }
                case TRANSACTION_COMMIT_AMBIGUOUS: {
                    String msg = "It is ambiguous whether the transaction committed";
                    ret = new CoreTransactionCommitAmbiguousException(cause, logger(), overall.transactionId(), msg);
                    break;
                }
                default:
                    ret = new CoreTransactionFailedException(cause, logger(), overall.transactionId());
                    break;
            }

            if (ret != null) {
                LOGGER.info(attemptId, "raising final error %s based on state bits %d masked %d tximplicit %s",
                        ret, sb, maskedFinalError, singleQueryTransactionMode);

                return Mono.error(ret);
            }

            return Mono.just(result);
        });
    }

    @Stability.Internal
    Throwable convertToOperationFailedIfNeeded(Throwable e, boolean singleQueryTransactionMode) {
        // If it's an TransactionOperationFailedException, the error originator has already chosen the error handling behaviour.  All
        // transaction internals will only raise this.
        if (e instanceof TransactionOperationFailedException) {
            return (TransactionOperationFailedException) e;
        }
        else if (e instanceof WrappedTransactionOperationFailedException) {
            return ((WrappedTransactionOperationFailedException) e).wrapped();
        }
        else if (singleQueryTransactionMode) {
            logger().info(attemptId(), "Caught exception from application's lambda %s, not converting", DebugUtil.dbg(e));
            return e;
        }
        else {
            // If we're here, it's an error thrown by the application's lambda, e.g. not from transactions internals
            // (these only raise TransactionOperationFailedException).
            // All such errors should try to rollback, and then fail the transaction.
            TransactionOperationFailedException.Builder builder = TransactionOperationFailedException.Builder.createError().cause(e);

            // This is the method for the lambda to explicitly request an auto-rollback-and-retry
            if (e instanceof RetryTransactionException) {
                builder.retryTransaction();
            }
            TransactionOperationFailedException out = builder.build();

            // Do not use getSimpleName() here (or anywhere)!  TXNJ-237
            // We redact the exception's name and message to be on the safe side
            logger().info(attemptId(), "Caught exception from application's lambda %s, converted it to %s",
                    DebugUtil.dbg(e),
                    DebugUtil.dbg(out));

            attemptSpan.recordExceptionAndSetErrorStatus(e);

            // Pass it through operationFailed to account for cases like insert raising DocExists, or query SLA errors
            return operationFailed(out);
        }
    }

    /**
     * Returns the {@link CoreTransactionLogger} used by this instance, so the developer can insert their own log messages.
     */
    public CoreTransactionLogger logger() {
        return LOGGER;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AttemptContextReactive{");
        sb.append("id=").append(attemptId.substring(0, TransactionLogEvent.CHARS_TO_LOG));
        sb.append(",state=").append(state);
        sb.append(",atr=").append(ActiveTransactionRecordUtil.getAtrDebug(atrCollection, atrId));
        sb.append(",staged=").append(stagedMutationsLocked.stream().map(StagedMutation::toString).collect(Collectors.toList()));
        sb.append('}');
        return sb.toString();
    }
}
