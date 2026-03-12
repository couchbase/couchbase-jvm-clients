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
import com.couchbase.client.core.annotation.UsedBy;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.api.query.CoreQueryContext;
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.api.query.CoreQueryOptionsTransactions;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.api.query.CoreQueryStatus;
import com.couchbase.client.core.classic.query.ClassicCoreReactiveQueryResult;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.tracing.RequestTracerAndDecorator;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.cnc.events.transaction.IllegalDocumentStateEvent;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.BooleanNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.IntNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.util.RawValue;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.TimeoutException;
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
import com.couchbase.client.core.error.transaction.internal.WrappedTransactionOperationFailedException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.msg.kv.RemoveResponse;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateResponse;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.cleanup.CleanupRequest;
import com.couchbase.client.core.transaction.cleanup.CoreTransactionsCleanup;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordUtil;
import com.couchbase.client.core.transaction.components.CoreTransactionGetMultiSpec;
import com.couchbase.client.core.transaction.components.DocRecord;
import com.couchbase.client.core.transaction.components.DocumentGetter;
import com.couchbase.client.core.transaction.components.DocumentMetadata;
import com.couchbase.client.core.transaction.components.DurabilityLevelUtil;
import com.couchbase.client.core.transaction.components.OperationTypes;
import com.couchbase.client.core.transaction.components.TransactionLinks;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.error.internal.ErrorClass;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsExtension;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibility;
import com.couchbase.client.core.transaction.forwards.ForwardCompatibilityStage;
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiOptions;
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiPhase;
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiSignal;
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiSignalAndReason;
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiState;
import com.couchbase.client.core.transaction.getmulti.CoreGetMultiUtil;
import com.couchbase.client.core.transaction.getmulti.CoreTransactionGetMultiMode;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.core.transaction.support.AttemptState;
import com.couchbase.client.core.transaction.support.OptionsUtil;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.support.StagedMutation;
import com.couchbase.client.core.transaction.support.StagedMutationType;
import com.couchbase.client.core.transaction.support.TransactionFields;
import com.couchbase.client.core.transaction.util.BlockingWaitGroup;
import com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.LogDeferThrowable;
import com.couchbase.client.core.transaction.util.MeteringUnits;
import com.couchbase.client.core.transaction.util.QueryUtil;
import com.couchbase.client.core.transaction.util.BlockingRetryHandler;
import com.couchbase.client.core.transaction.util.TransactionKVHandler;
import com.couchbase.client.core.util.BucketConfigUtil;
import com.couchbase.client.core.util.CbPreconditions;
import com.couchbase.client.core.util.Either;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.annotation.UsedBy.Project.SPRING_DATA_COUCHBASE;
import static com.couchbase.client.core.api.kv.CoreExpiry.LATEST_VALID_EXPIRY_INSTANT;
import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_ATR_COMMIT;
import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_GET_MULTI_REPLICAS_FROM_PREFERRED_SERVER_GROUP;
import static com.couchbase.client.core.cnc.TracingIdentifiers.TRANSACTION_OP_GET_MULTI;
import static com.couchbase.client.core.config.BucketCapabilities.SUBDOC_REVIVE_DOCUMENT;
import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.Builder.createError;
import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.FinalErrorToRaise;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;
import static com.couchbase.client.core.msg.kv.CodecFlags.BINARY_COMMON_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.JSON_COMMON_FLAGS;
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
import static com.couchbase.client.core.transaction.getmulti.CoreGetMultiState.DEFAULT_INITIAL_DOC_FETCH_BOUND;
import static com.couchbase.client.core.transaction.getmulti.CoreGetMultiState.DEFAULT_READ_SKEW_BOUND;
import static com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks.HOOK_COMMIT_DOC_CHANGED;
import static com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC_CHANGED;
import static com.couchbase.client.core.transaction.util.CoreTransactionAttemptContextHooks.HOOK_STAGING_DOC_CHANGED;

/**
 * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well
 * as commit or rollback the transaction.
 */
@Stability.Internal
public class CoreTransactionAttemptContext {
    static class TransactionQueryContext {
        public final NodeIdentifier queryTarget;
        public final @Nullable CoreQueryContext queryContext;

        public TransactionQueryContext(NodeIdentifier queryTarget, @Nullable CoreQueryContext queryContext) {
            this.queryTarget = Objects.requireNonNull(queryTarget);
            this.queryContext = queryContext;
        }
    }

    public static final byte[] NEAR_EMPTY_BYTE_ARRAY = new byte[]{'n', 'u', 'l', 'l'};
    public static final int TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED = 0x1;
    public static final int TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED = 0x2;
    public static final int TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK = 0x4;
    public static final int TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY = 0x8;

    // Bits 0-3: BehaviourFlags
    // Bits 4-6: FinalErrorToRaise
    public static final int STATE_BITS_POSITION_FINAL_ERROR = 4;
    public static final int STATE_BITS_MASK_FINAL_ERROR = 0b1110000;
    public static final int STATE_BITS_MASK_BITS = 0b0001111;
    public static final int UNSTAGING_PARALLELISM = Integer.parseInt(System.getProperty("com.couchbase.transactions.unstagingParallelism", "1000"));

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
    private final SpanWrapper attemptSpan;

    // [EXP-ROLLBACK] [EXP-COMMIT-OVERTIME]: Internal flag to indicate that transaction has expired.  After,
    // this, one attempt will be made to rollback the txn (or commit it, if the expiry happened during commit).
    private volatile boolean expiryOvertimeMode = false;
    private volatile @Nullable TransactionQueryContext queryContext = null;
    // Purely for debugging (so we don't have to log the statement everywhere), associate each statement with an id
    private final AtomicInteger queryStatementIdx = new AtomicInteger(0);

    private final boolean lockDebugging = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.debug.lock", "false"));
    private final boolean monoBridgeDebugging = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.debug.monoBridge", "false"));
    private final boolean threadSafetyEnabled = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.threadSafety", "true"));

    // Note a KV operation that's being performed via N1QL is still counted as a query op.
    private final BlockingWaitGroup kvOps;

    // Using this over ReentrantReadWriteLock and StampedLocked as many writers will be typical flow.
    private final ReentrantLock mutex = new ReentrantLock();

    private final int EXPIRY_THRESHOLD = Integer.parseInt(System.getProperty("com.couchbase.transactions.expiryThresholdMs", "10"));

    private final CoreQueryOps queryOps;
    private MeteringUnits.MeteringUnitsBuilder meteringUnitsBuilder = new MeteringUnits.MeteringUnitsBuilder();

    // Just a safety measure to make sure we don't get into hard tight loops
    public static final Duration DEFAULT_DELAY_RETRYING_OPERATION = Duration.ofMillis(3);

    public CoreTransactionAttemptContext(Core core, CoreTransactionContext overall, CoreMergedTransactionConfig config, String attemptId,
                                         Optional<SpanWrapper> parentSpan, CoreTransactionAttemptContextHooks hooks) {
        this.core = Objects.requireNonNull(core);
        this.overall = Objects.requireNonNull(overall);
        this.LOGGER = Objects.requireNonNull(overall.LOGGER);
        this.config = Objects.requireNonNull(config);
        this.attemptId = Objects.requireNonNull(attemptId);
        this.startTimeClient = Duration.ofNanos(System.nanoTime());
        this.hooks = Objects.requireNonNull(hooks);
        this.queryOps = core.queryOps();

        this.attemptSpan = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_ATTEMPT, parentSpan.orElse(null));
        this.kvOps = new BlockingWaitGroup(
                (message, cause) -> AccessorUtil.operationFailed(this, createError()
                        .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                        .doNotRollbackAttempt()
                        .cause(new AttemptExpiredException(message, cause)).build()),
                (String format, Object... args) -> logger().info(attemptId, format, args),
                lockDebugging);
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

    private void errorIfExpiredAndNotInExpiryOvertimeMode(String stage, Optional<String> docId) {
        if (expiryOvertimeMode) {
            LOGGER.info(attemptId, "not doing expiry check in {} as already in expiry-overtime-mode", stage);
            return;
        }
        else if (hasExpiredClientSide(stage, docId)) {
            LOGGER.info(attemptId, "has expired in stage {}", stage);

            // We don't set expiry-overtime-mode here, that's done by the error handler
            // Intentionally a raw AttemptExpiredException here as this is a raw internal call.  The caller will convert to a TransactionOperationFailed.
            throw new AttemptExpiredException("Attempt has expired in stage " + stage);
        }
    }

    private void checkExpiryPreCommitAndSetExpiryOvertimeMode(String stage, Optional<String> docId) {
        if (hasExpiredClientSide(stage, docId)) {
            LOGGER.info(attemptId, "has expired in stage {}, setting expiry-overtime-mode", stage);

            // Combo of setting this mode and throwing AttemptExpired will result in an attempt to
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
     * @param id the document's ID
     * @return an <code>Optional</code> containing the document, or <code>Optional.empty()</code> if not found
     */
    private Optional<CoreTransactionGetResult> getInternal(CollectionIdentifier collection, String id, SpanWrapper pspan) {

        return doKVOperation("get " + DebugUtil.docId(collection, id), pspan, CoreTransactionAttemptContextHooks.HOOK_GET, collection, id,
                (operationId, span) -> {
                    if (queryModeLocked()) {
                        return getWithQueryLocked(collection, id, span);
                    } else {
                        return getWithKVLocked(collection, id, Optional.empty(), span, false);
                    }
                });
    }

    private Optional<CoreTransactionGetResult> getReplicaFromPreferredServerGroupInternal(CollectionIdentifier collection, String id, SpanWrapper pspan) {

        return doKVOperation("get " + DebugUtil.docId(collection, id), pspan, CoreTransactionAttemptContextHooks.HOOK_GET, collection, id,
                (operationId, span) -> {
                    if (queryModeLocked()) {
                        throw new FeatureNotAvailableException("getReplicaFromPreferredServerGroup cannot presently be used in a transaction that has previously involved the query service.  It can however be used before any query call.");
                    } else {
                        return getWithKVLocked(collection, id, Optional.empty(), span, true);
                    }
                });
    }

    private Optional<CoreTransactionGetResult> getWithKVLocked(CollectionIdentifier collection,
                                                               String id,
                                                               Optional<String> resolvingMissingATREntry,
                                                               SpanWrapper pspan,
                                                               boolean preferredReplicaMode) {
        assertLocked("getWithKV");

        LOGGER.info(attemptId, "getting doc {}, resolvingMissingATREntry={}, preferredReplicaMode={}", DebugUtil.docId(collection, id),
                resolvingMissingATREntry.orElse("<empty>"), preferredReplicaMode);

        Optional<StagedMutation> ownWrite = checkForOwnWriteLocked(collection, id);
        if (ownWrite.isPresent()) {
            StagedMutation ow = ownWrite.get();
            // Can only use if the content is there.  If not, we will read our own-write from KV instead.  This is just an optimisation.
            boolean usable = ow.content != null;
            LOGGER.info(attemptId, "found own-write of mutated doc {}, usable = {}", DebugUtil.docId(collection, id), usable);
            if (usable) {
                // Use the staged content as the body.  The staged content & userFlags in the output TransactionGetResult should not be used for anything so we are safe to pass null & 0.
                unlock("found own-write of mutation");
                return Optional.of(createTransactionGetResult(ow.operationId, collection, id, ow.content, ow.stagedUserFlags, null, 0, ow.cas,
                        ow.documentMetadata, ow.type.toString(), ow.crc32, Optional.ofNullable(ow.expiry)));
            }
        }
        Optional<StagedMutation> ownRemove = stagedRemovesLocked().stream().filter(v -> {
            return v.collection.equals(collection) && v.id.equals(id);
        }).findFirst();

        if (ownRemove.isPresent()) {
            LOGGER.info(attemptId, "found own-write of removed doc {}",
                    DebugUtil.docId(collection, id));

            unlock("found own-write of removed");
            return Optional.empty();
        }

        MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

        try {
            hooks.beforeUnlockGet.accept(this, id); // in-lock test hook

            unlock("standard");

            hooks.beforeDocGet.accept(this, id); // post-lock test hook

            Optional<CoreTransactionGetResult> result = DocumentGetter.get(core,
                    LOGGER,
                    collection,
                    config,
                    id,
                    attemptId,
                    false,
                    pspan,
                    resolvingMissingATREntry,
                    units,
                    overall.supported(),
                    preferredReplicaMode);


            long elapsed = pspan.elapsedMicros();
            MeteringUnits built = addUnits(units.build());
            if (result.isPresent()) {
                LOGGER.info(attemptId, "completed get of {}{} in {}us", result.get(), DebugUtil.dbg(built), elapsed);
            } else {
                LOGGER.info(attemptId, "completed get of {}{}, could not find, in {}us",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(built), elapsed);
            }

            // Testing hook
            hooks.afterGetComplete.accept(this, id);

            if (result.isPresent()) {
                forwardCompatibilityCheck(ForwardCompatibilityStage.GETS, result.flatMap(v -> v.links().forwardCompatibility()));
            }

            return result;
        } catch (Exception err) {
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder builder = createError().cause(err);
            MeteringUnits built = addUnits(units.build());

            LOGGER.warn(attemptId, "got error while getting doc {}{} in {}us: {}",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(built), pspan.elapsedMicros(), dbg(err));

            if (err instanceof DocumentUnretrievableException) {
                throw err;
            } else if (err instanceof ForwardCompatibilityRequiresRetryException
                    || err instanceof ForwardCompatibilityFailureException) {
                TransactionOperationFailedException.Builder error = createError()
                        .cause(new ForwardCompatibilityFailureException());
                if (err instanceof ForwardCompatibilityRequiresRetryException) {
                    error.retryTransaction();
                }
                throw operationFailed(error.build());
            } else if (ec == TRANSACTION_OPERATION_FAILED) {
                throw err;
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

                lock("get relock");
                try {
                    return getWithKVLocked(collection,
                            id,
                            Optional.of(attemptIdToCheck),
                            pspan,
                            preferredReplicaMode);
                } finally {
                    unlock("relock error");
                }

            } else if (ec == FAIL_HARD) {
                throw operationFailed(builder.doNotRollbackAttempt().build());
            } else if (ec == FAIL_TRANSIENT) {
                throw operationFailed(builder.retryTransaction().build());
            } else {
                throw operationFailed(builder.build());
            }
        }
    }

    private ObjectNode makeTxdata() {
        return Mapper.createObjectNode()
                .put("kv", true);
    }

    private Optional<CoreTransactionGetResult> getWithQueryLocked(CollectionIdentifier collection, String id, SpanWrapper span) {
        assertLocked("getWithQuery");

        try {
            int sidx = queryStatementIdx.getAndIncrement();

            ArrayNode params = Mapper.createArrayNode()
                    .add(makeKeyspace(collection))
                    .add(id);

            CoreQueryOptionsTransactions queryOptions = new CoreQueryOptionsTransactions();
            queryOptions.raw("args", params);

            CoreQueryResult result = queryWrapperBlockingLocked(sidx,
                    queryContext.queryContext,
                    "EXECUTE __get",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_GET,
                    false,
                    true,
                    makeTxdata(),
                    params,
                    span,
                    false,
                    true);

            List<QueryChunkRow> rows = result.collectRows();

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

                logger().info(attemptId, "got doc {} from query with scas={} meta={}",
                        DebugUtil.docId(collection, id), scas, txnMeta.isMissingNode() ? "null" : txnMeta.textValue());

                ret = Optional.of(new CoreTransactionGetResult(id,
                        content,
                        JSON_COMMON_FLAGS, // If fetching from query, currently is must be JSON (binary not supported)
                        cas,
                        collection,
                        null,
                        Optional.empty(),
                        txnMeta.isMissingNode() ? Optional.empty() : Optional.of(txnMeta),
                        crc32));
            }

            // unlock is here rather than in usual place straight after queryWrapper, so it handles the DocumentNotFoundException case too
            unlock("getWithQueryLocked end");

            return ret;
        } catch (Exception err) {
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder builder = createError().cause(err);
            span.recordExceptionAndSetErrorStatus(err);

            if (err instanceof DocumentNotFoundException) {
                unlock("getWithQueryLocked no document found");
                return Optional.empty();
            } else if (err instanceof TransactionOperationFailedException) {
                throw err;
            } else {
                throw operationFailed(builder.build());
            }
        }
    }

    /**
     * Gets a document with the specified <code>id</code> and from the specified Couchbase <code>bucket</code>.
     * <p>
     *
     * @param collection the Couchbase collection the document exists on
     * @param id the document's ID
     * @return a <code>CoreTransactionGetResult</code> containing the document
     */
    public CoreTransactionGetResult get(CollectionIdentifier collection, String id) {
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_GET, attemptSpan);
        try {
            Optional<CoreTransactionGetResult> doc = getInternal(collection, id, span);
            span.finish();
            if (doc.isPresent()) {
                return doc.get();
            } else {
                throw new DocumentNotFoundException(ReducedKeyValueErrorContext.create(id));
            }
        } catch (Exception err) {
            span.finishWithErrorStatus();
            throw err;
        }
    }

    public Mono<CoreTransactionGetResult> getReactive(CollectionIdentifier collection, String id) {
        return Mono.fromCallable(() -> get(collection, id))
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    public CoreTransactionGetResult getReplicaFromPreferredServerGroup(CollectionIdentifier collection, String id) {
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_GET_REPLICA_FROM_PREFERRED_SERVER_GROUP, attemptSpan);
        try {
            Optional<CoreTransactionGetResult> doc = getReplicaFromPreferredServerGroupInternal(collection, id, span);
            span.finish();
            if (doc.isPresent()) {
                return doc.get();
            } else {
                throw new DocumentUnretrievableException(ReducedKeyValueErrorContext.create(id));
            }
        } catch (Exception err) {
            span.finishWithErrorStatus();
            throw err;
        }
    }

    public Mono<CoreTransactionGetResult> getReplicaFromPreferredServerGroupReactive(CollectionIdentifier collection, String id) {
        return Mono.fromCallable(() -> getReplicaFromPreferredServerGroup(collection, id))
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    static class RetryGetMulti extends RuntimeException {
        public RetryGetMulti(String message) {
            super(message);
        }
    }

    static class ResetAndRetryGetMulti extends RuntimeException {
        public ResetAndRetryGetMulti(String message) {
            super(message);
        }
    }

    static class BoundExceeded extends RuntimeException {
    }

    public List<CoreTransactionOptionalGetMultiResult> getMultiAlgo(List<CoreTransactionGetMultiSpec> specs, CoreGetMultiOptions options, boolean replicasFromPreferredServerGroup) {
        SpanWrapper pspan = SpanWrapperUtil.createOp(this, tracer(), null, null,
                replicasFromPreferredServerGroup ? TRANSACTION_OP_GET_MULTI_REPLICAS_FROM_PREFERRED_SERVER_GROUP : TRANSACTION_OP_GET_MULTI
                , attemptSpan);

        if (replicasFromPreferredServerGroup) {
            if (core.environment().preferredServerGroup() == null) {
                throw new CouchbaseException("Preferred server group must be set previously at the environment level");
            }
        }

        try {
            return doKVOperation("getMulti", pspan, CoreTransactionAttemptContextHooks.HOOK_GET_MULTI, null, null,
                    (operationId, span) -> {
                        if (queryModeLocked()) {
                            throw new FeatureNotAvailableException("getMulti cannot be used in a transaction after any SQL++ commands have been executed.  If possible then move the getMulti to before any SQL++ commands.");
                        } else {
                            unlock("getMulti");
                            return getMultiAlgoInternal(specs, span, options, replicasFromPreferredServerGroup);
                        }
                    });
        } finally {
            pspan.finish();
        }
    }

    public Mono<List<CoreTransactionOptionalGetMultiResult>> getMultiAlgoReactive(List<CoreTransactionGetMultiSpec> specs, CoreGetMultiOptions options, boolean replicasFromPreferredServerGroup) {
        return Mono.fromCallable(() -> getMultiAlgo(specs, options, replicasFromPreferredServerGroup))
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    private List<CoreTransactionOptionalGetMultiResult> getMultiAlgoInternal(List<CoreTransactionGetMultiSpec> specs, SpanWrapper pspan, CoreGetMultiOptions options, boolean replicasFromPreferredServerGroup) {
        Instant deadline = Instant.now().plus(DEFAULT_INITIAL_DOC_FETCH_BOUND);
        CoreGetMultiState operationState = new CoreGetMultiState(specs, deadline, replicasFromPreferredServerGroup, options);

        BlockingRetryHandler retry = BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ofMillis(1000)).build();

        try {
            do {
                retry.shouldRetry(false);
                LOGGER.info(attemptId, "getMulti: state={}", operationState.toString());

                throwIfExpired(CoreTransactionAttemptContextHooks.HOOK_GET_MULTI);

                CoreGetMultiSignal getMultiDocumentSignal = getMultiDocumentFetch(operationState);

                switch (getMultiDocumentSignal) {
                    case CONTINUE:
                        break;
                    case COMPLETED:
                        return operationState.alreadyFetchedSorted();
                    case RESET_AND_RETRY:
                        operationState.reset(LOGGER);
                        retry.sleepForNextBackoffAndSetShouldRetry();
                        continue;
                    case RETRY:
                        retry.sleepForNextBackoffAndSetShouldRetry();
                        continue;
                    case BOUND_EXCEEDED:
                        throw new BoundExceeded();
                    default:
                        throw operationFailed(createError().cause(new RuntimeException("Internal bug: Unexpected signal " + getMultiDocumentSignal + " received")).build());
                }

                CoreGetMultiSignalAndReason documentDisambiguationSignal = getMultiDocumentDisambiguation(operationState);

                LOGGER.info(attemptId, "getMulti: documentDisambiguation returned {}", documentDisambiguationSignal);
                switch (documentDisambiguationSignal.signal) {
                    case COMPLETED:
                        return operationState.alreadyFetchedSorted();
                    case RESET_AND_RETRY:
                        operationState.reset(LOGGER);
                        retry.sleepForNextBackoffAndSetShouldRetry();
                        continue;
                    case RETRY:
                        retry.sleepForNextBackoffAndSetShouldRetry();
                        continue;
                    case BOUND_EXCEEDED:
                        throw new BoundExceeded();
                    case CONTINUE:
                        CoreGetMultiSignalAndReason readSkewSignal = getMultiReadSkewResolution(operationState);
                        LOGGER.info(attemptId, "getMulti: readSkewResolution returned {}", readSkewSignal);
                        switch (readSkewSignal.signal) {
                            case COMPLETED:
                                return operationState.alreadyFetchedSorted();
                            case RESET_AND_RETRY:
                                operationState.reset(LOGGER);
                                retry.sleepForNextBackoffAndSetShouldRetry();
                                continue;
                            case RETRY:
                                retry.sleepForNextBackoffAndSetShouldRetry();
                                continue;
                            case BOUND_EXCEEDED:
                                throw new BoundExceeded();
                            default:
                                throw operationFailed(createError().cause(new RuntimeException("Internal bug: Unexpected signal " + readSkewSignal + " received")).build());
                        }
                    default:
                        throw operationFailed(createError().cause(new RuntimeException("Internal bug: Unexpected signal " + documentDisambiguationSignal + " received")).build());
                }
            } while (retry.shouldRetry());

            return operationState.alreadyFetchedSorted();
        } catch (BoundExceeded err) {
            if (operationState.alreadyFetched().size() == operationState.originalSpecs.size()) {
                LOGGER.info(attemptId, "getMulti: deadline expiring and have all docs, returning");
                return operationState.alreadyFetchedSorted();
            } else {
                LOGGER.info(attemptId, "getMulti: deadline expiring and do not have all docs, failing");
                throw operationFailed(createError()
                        .retryTransaction()
                        .cause(new RuntimeException("Operation timeout was exceeded, but do not have results for all documents (have " + operationState.alreadyFetched().size() + " of " + operationState.originalSpecs.size() + ")"))
                        .build());
            }
        }
    }

    public Either<CoreTransactionOptionalGetMultiResult, CoreGetMultiSignal> getMultiSingleDocumentFetch(CoreTransactionGetMultiSpec spec, CoreGetMultiState operationState) {
        long start = System.nanoTime();
        Duration operationTimeout = Duration.ofMillis(Math.min(
                Duration.between(Instant.now(), operationState.deadline).toMillis(),
                kvTimeoutNonMutating().toMillis()));
        long attempt = 0;

        BlockingRetryHandler retry = BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ofMillis(1000)).build();
        do {
            LOGGER.info(attemptId, "getMulti: getting doc {} with timeout {}, attempt {}", spec, operationTimeout, attempt);
            attempt += 1;
            retry.shouldRetry(false);

            throwIfExpired(CoreTransactionAttemptContextHooks.HOOK_GET_MULTI_INDIVIDUAL_DOCUMENT);

            try {
                Optional<Tuple2<CoreTransactionGetResult, CoreSubdocGetResult>> doc = DocumentGetter.justGetDoc(core, spec.collectionIdentifier, spec.id, operationTimeout, null, !operationState.replicasFromPreferredServerGroup, LOGGER, meteringUnitsBuilder, operationState.replicasFromPreferredServerGroup);

                LOGGER.info(attemptId, "getMulti: completed get of {} in {}us, present={}, inTransaction={}",
                        spec, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start), doc.isPresent(), doc.map(v -> v.getT1().isInTransaction()));
                if (doc.isPresent()) {
                    if (doc.get().getT1().isInTransaction()) {
                        forwardCompatibilityCheck(ForwardCompatibilityStage.GET_MULTI_GET, Objects.requireNonNull(doc.get().getT1().links()).forwardCompatibility());
                        forwardCompatibilityCheck(ForwardCompatibilityStage.GETS, Objects.requireNonNull(doc.get().getT1().links()).forwardCompatibility());
                    }

                    return Either.ofLeft(new CoreTransactionOptionalGetMultiResult(spec, doc.map(Tuple2::getT1)));
                } else {
                    return Either.ofLeft(new CoreTransactionOptionalGetMultiResult(spec, Optional.empty()));
                }
            } catch (Exception err) {
                ErrorClass ec = classify(err);
                LOGGER.info("getMulti: document-fetch hit error on {}: {}", spec, err);
                if (err instanceof DocumentUnretrievableException) {
                    return Either.ofLeft(new CoreTransactionOptionalGetMultiResult(spec, Optional.empty()));
                } else if (err instanceof TimeoutException) {
                    if (operationState.options.mode == CoreTransactionGetMultiMode.PRIORITISE_READ_SKEW_DETECTION) {
                        retry.sleepForNextBackoffAndSetShouldRetry();
                        continue;
                    }
                    return Either.ofRight(CoreGetMultiSignal.BOUND_EXCEEDED);
                } else if (ec == FAIL_TRANSIENT) {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                    continue;
                }
                throw operationFailed(createError()
                        .cause(err)
                        .rollbackAttempt(ec == FAIL_HARD)
                        .build());
            }
        } while (retry.shouldRetry());

        throw operationFailed(createError().cause(new IllegalStateException("Internal bug, should not be able to reach here")).build());
    }

    private RuntimeException interrupted(String dbg) {
        logger().info(attemptId, "Interrupted at point " + dbg);
        Thread.currentThread().interrupt();
        throw operationFailed(createError()
                .cause(new RuntimeException("Thread interrupted at " + dbg))
                .build());
    }

    public CoreGetMultiSignal getMultiDocumentFetch(CoreGetMultiState operationState) {
        LOGGER.info(attemptId, "getMulti: document fetch {}", operationState.toString());
        int desiredParallelism = 100;
        long start = System.nanoTime();

        List<CoreTransactionGetMultiSpec> toFetch = operationState.toFetch();
        CountDownLatch latch = new CountDownLatch(toFetch.size());
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        ExecutorService executor = core.context().environment().transactionsSchedulers().blockingExecutor();
        Semaphore semaphore = new Semaphore(desiredParallelism);

        List<Either<CoreTransactionOptionalGetMultiResult, CoreGetMultiSignal>> allDocs = Collections.synchronizedList(new ArrayList<>());

        for (CoreTransactionGetMultiSpec spec : toFetch) {
            executor.submit(() -> {
                try {
                    // Short-circuit on failure.
                    if (failure.get() != null) {
                        return;
                    }
                    semaphore.acquire();
                    try {
                        if (failure.get() == null) {
                            allDocs.add(getMultiSingleDocumentFetch(spec, operationState));
                        }
                    } finally {
                        semaphore.release();
                    }
                } catch (RuntimeException err) {
                    failure.compareAndSet(null, err);
                } catch (Throwable err) {
                    failure.compareAndSet(null, new RuntimeException(err));
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw interrupted("getMultiDocumentFetch");
        }

        if (failure.get() != null) {
            throw failure.get();
        }

        LOGGER.info(attemptId, "getMulti: have got %d doc reports (not necessarily docs) in {}us",
                allDocs.size(), TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start));
        List<CoreTransactionOptionalGetMultiResult> alreadyFetched = new ArrayList<>();
        alreadyFetched.addAll(operationState.alreadyFetched());
        alreadyFetched.addAll(allDocs.stream()
                .filter(v -> v.left().isPresent())
                .map(v -> v.left().get())
                .collect(Collectors.toList()));
        operationState.update(LOGGER, Collections.emptyList(),
                alreadyFetched,
                operationState.phase(),
                operationState.deadline);
        Optional<Either<CoreTransactionOptionalGetMultiResult, CoreGetMultiSignal>> anySignal = allDocs.stream()
                .filter(v -> v.right().isPresent())
                .findAny();
        if (anySignal.isPresent()) {
            CoreGetMultiSignal signal = anySignal.get().right().get();
            LOGGER.info(attemptId, "getMulti: returning signal {} from individual doc fetch", signal);
            return signal;
        }
        if (operationState.options.mode == CoreTransactionGetMultiMode.DISABLE_READ_SKEW_DETECTION) {
            return CoreGetMultiSignal.COMPLETED;
        }
        if (operationState.phase() == CoreGetMultiPhase.FIRST_DOC_FETCH) {
            Instant newDeadline = operationState.options.mode == CoreTransactionGetMultiMode.PRIORITISE_LATENCY
                    ? Instant.now().plus(DEFAULT_READ_SKEW_BOUND)
                    : Instant.now().plus(Duration.ofMillis(expiryRemainingMillis()));
            LOGGER.info(attemptId, "getMulti: setting deadline to {}", newDeadline);
            operationState.update(LOGGER,
                    operationState.toFetch(),
                    operationState.alreadyFetched(),
                    CoreGetMultiPhase.SUBSEQUENT_TO_FIRST_DOC_FETCH,
                    newDeadline);
        }
        return CoreGetMultiSignal.CONTINUE;
    }

    private CoreGetMultiSignalAndReason getMultiDocumentDisambiguation(CoreGetMultiState operationState) {
        List<CoreTransactionGetMultiResult> fetchedAndPresent = operationState.fetchedAndPresent();

        if (fetchedAndPresent.isEmpty()) {
            LOGGER.info(attemptId, "getMultiDD: no docs present");
            return CoreGetMultiSignalAndReason.COMPLETED;
        } else if (fetchedAndPresent.size() == 1) {
            LOGGER.info(attemptId, "getMultiDD: just one doc present, performing MAV");
            CoreTransactionGetMultiResult single = fetchedAndPresent.get(0);

            Optional<CoreTransactionGetResult> mavRead = DocumentGetter.get(core, LOGGER, single.spec.collectionIdentifier, config, single.spec.id, attemptId, false, attemptSpan, Optional.empty(), meteringUnitsBuilder, overall.supported(), false);

            CoreTransactionOptionalGetMultiResult r = new CoreTransactionOptionalGetMultiResult(single.spec, mavRead);

            List<CoreTransactionOptionalGetMultiResult> updated = new ArrayList<>(operationState.alreadyFetched().stream()
                    .filter(result -> result.spec.specIndex != single.spec.specIndex)
                    .collect(Collectors.toList()));
            updated.add(r);

            operationState.update(logger(), operationState.toFetch(), updated, operationState.phase(), operationState.deadline);
            return CoreGetMultiSignalAndReason.COMPLETED;
        } else {
            // 2 or more docs, read skew possible
            Set<String> transactionIdsInvolved = fetchedAndPresent.stream()
                    .filter(v -> Objects.requireNonNull(v.internal.links()).isDocumentInTransaction())
                    .map(v -> Objects.requireNonNull(v.internal.links()).stagedTransactionId().get())
                    .collect(Collectors.toSet());

            LOGGER.info(attemptId, "getMultiDD: involves these other transactions: {}", transactionIdsInvolved);

            if (transactionIdsInvolved.isEmpty()) {
                // No docs are mid-transaction, so we either have no read skew or missed it.
                return CoreGetMultiSignalAndReason.COMPLETED;
            } else if (transactionIdsInvolved.size() > 1) {
                throw new RetryGetMulti("Too many transaction ids involved " + transactionIdsInvolved.size());
            } else {
                return CoreGetMultiSignalAndReason.CONTINUE;
            }
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private CoreGetMultiSignalAndReason getMultiReadSkewResolution(CoreGetMultiState operationState) {
        LOGGER.info(attemptId, "getMultiRSR: {}", operationState.toString());

        // 2+ docs and 1+ of them are staged in the same transaction T1.
        CoreGetMultiSignalAndReason signal = operationState.assertInReadSkewResolutionState();
        if (signal.signal != CoreGetMultiSignal.CONTINUE) {
            return signal;
        }

        if (operationState.deadlineExceededSoon()) {
            return CoreGetMultiSignalAndReason.BOUND_EXCEEDED;
        }

        List<CoreTransactionGetMultiResult> fetchedAndPresent = operationState.fetchedAndPresent();

        TransactionLinks t1 = fetchedAndPresent.stream()
                .filter(v -> v.internal.isInTransaction())
                .findFirst()
                // This is ok due to assertInReadSkewResolutionState check above
                .get()
                .internal.links();

        CollectionIdentifier atrCollection = new CollectionIdentifier(t1.atrBucketName().get(), t1.atrScopeName(), t1.atrCollectionName());
        String t1TransactionId = t1.stagedTransactionId().get();

        Duration operationTimeout = Duration.ofMillis(Math.min(
                Duration.between(Instant.now(), operationState.deadline).toMillis(),
                kvTimeoutNonMutating().toMillis()));
        LOGGER.info(attemptId, "getMultiRSR: getting T1 entry {} with timeout {}", t1TransactionId, operationTimeout);

        Optional<ActiveTransactionRecordEntry> atrResult = ActiveTransactionRecord.findEntryForTransaction(core, atrCollection, t1.atrId().get(), t1.stagedAttemptId().get(), config, null, LOGGER, meteringUnitsBuilder, operationTimeout);

        return getMultiReadSkewResolutionAfterT1AtrEntryRead(operationState, atrResult, t1TransactionId);
    }

    private boolean isIn(List<CoreTransactionGetMultiResult> results, CoreTransactionGetMultiResult check) {
        return results.stream()
                .anyMatch(v -> v.internal.collection().equals(check.internal.collection()) && v.internal.id().equals(check.internal.id()));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private CoreGetMultiSignalAndReason getMultiReadSkewResolutionAfterT1AtrEntryRead(CoreGetMultiState operationState,
                                                                                      Optional<ActiveTransactionRecordEntry> atrResult,
                                                                                      String t1TransactionId) {
        List<CoreTransactionGetMultiResult> fetchedAndPresent = operationState.fetchedAndPresent();

        List<CoreTransactionGetMultiResult> fetchedInT1 = fetchedAndPresent.stream()
                .filter(result -> result.internal.isInTransaction(t1TransactionId))
                .collect(Collectors.toList());

        List<CoreTransactionOptionalGetMultiResult> fetchedNotInT1 = operationState.alreadyFetched().stream()
                .filter(result -> !result.isPresent() || !isIn(fetchedInT1, result.get()))
                .collect(Collectors.toList());

        LOGGER.info(attemptId, "getMultiRSRA: T1={} fetchedInT1={} fetchedNotInT1={} state={}", atrResult,
                fetchedInT1.stream().map(v -> DebugUtil.docId(v.spec.collectionIdentifier, v.spec.id)).collect(Collectors.toList()),
                fetchedNotInT1.size(),
                operationState.toString());

        if (fetchedNotInT1.size() + fetchedInT1.size() != operationState.originalSpecs.size()) {
            return new CoreGetMultiSignalAndReason(CoreGetMultiSignal.RESET_AND_RETRY, "Internal bug");
        }

        if (!atrResult.isPresent()) {
            if (operationState.phase() == CoreGetMultiPhase.RESOLVING_T1_ATR_ENTRY_MISSING) {
                if (!fetchedInT1.isEmpty()) {
                    return CoreGetMultiSignalAndReason.COMPLETED;
                } else {
                    return new CoreGetMultiSignalAndReason(CoreGetMultiSignal.RESET_AND_RETRY, "Remain in ambiguous state after refetching when T1 ATR entry is missing.  Polling until simpler state");
                }
            } else {
                CoreGetMultiSignalAndReason signal = operationState.update(LOGGER, CoreGetMultiUtil.toSpecs(fetchedInT1),
                        fetchedNotInT1,
                        CoreGetMultiPhase.RESOLVING_T1_ATR_ENTRY_MISSING,
                        operationState.deadline);
                if (signal.signal != CoreGetMultiSignal.CONTINUE) {
                    return signal;
                }
                return new CoreGetMultiSignalAndReason(CoreGetMultiSignal.RETRY, "Have found T1's ATR entry missing.  Refetching its docs to disambiguate T1 state.");
            }
        } else {
            AttemptState state = atrResult.get().state();
            switch (state) {
                case PENDING:
                    // If T1 is uncommitted, ok to use these docs for reads.  Writes are likely to hit a WWC and retry then though.
                    return CoreGetMultiSignalAndReason.COMPLETED;
                case ABORTED:
                    // If T1 is rolling back, ok to use these docs for reads.  Writes are likely to hit a WWC and retry then though.
                    return CoreGetMultiSignalAndReason.COMPLETED;
                case COMMITTED:

                    if (operationState.phase() == CoreGetMultiPhase.SUBSEQUENT_TO_FIRST_DOC_FETCH) {
                        List<CoreTransactionGetMultiResult> wereInT1 = fetchedNotInT1.stream()
                                .filter(CoreTransactionOptionalGetMultiResult::isPresent).map(CoreTransactionOptionalGetMultiResult::get)
                                .filter(v -> atrResult.get().containsDocument(v.internal.collection(), v.internal.id()))
                                .collect(Collectors.toList());
                        List<CoreTransactionOptionalGetMultiResult> remainder = operationState.alreadyFetched().stream()
                                .filter(v -> !v.isPresent() || !isIn(wereInT1, v.get()))
                                .collect(Collectors.toList());

                        if (wereInT1.isEmpty()) {
                            List<CoreTransactionOptionalGetMultiResult> toReturn = new ArrayList<>(fetchedNotInT1);
                            toReturn.addAll(fetchedInT1.stream()
                                    .map(v -> v.convertToPostTransaction().toOptional())
                                    .collect(Collectors.toList()));
                            CoreGetMultiSignalAndReason signal = operationState.update(LOGGER, Collections.emptyList(), toReturn, CoreGetMultiPhase.SUBSEQUENT_TO_FIRST_DOC_FETCH, operationState.deadline);
                            if (signal != CoreGetMultiSignalAndReason.CONTINUE) {
                                return signal;
                            }
                            return CoreGetMultiSignalAndReason.COMPLETED;
                        } else {
                            CoreGetMultiSignalAndReason signal = operationState.update(LOGGER, CoreGetMultiUtil.toSpecs(wereInT1), remainder, CoreGetMultiPhase.SUBSEQUENT_TO_FIRST_DOC_FETCH, operationState.deadline);
                            if (signal != CoreGetMultiSignalAndReason.CONTINUE) {
                                return signal;
                            }
                            return new CoreGetMultiSignalAndReason(CoreGetMultiSignal.RETRY, "Fetched some docs that we've later discovered to be involved in T1: refetching them");
                        }

                    } else if (operationState.phase() == CoreGetMultiPhase.DISCOVERED_DOCS_IN_T1) {
                        CoreGetMultiSignalAndReason signal = operationState.update(LOGGER,
                                Collections.emptyList(),
                                operationState.alreadyFetched().stream()
                                        .map(CoreTransactionOptionalGetMultiResult::convertToPostTransaction)
                                        .collect(Collectors.toList()),
                                CoreGetMultiPhase.SUBSEQUENT_TO_FIRST_DOC_FETCH,
                                operationState.deadline);
                        if (signal != CoreGetMultiSignalAndReason.CONTINUE) {
                            return signal;
                        }
                        return CoreGetMultiSignalAndReason.COMPLETED;
                    } else {
                        // should be impossible - got here in ModeResolvingT1ATREntryMissing but T1's entry is now present
                        return new CoreGetMultiSignalAndReason(CoreGetMultiSignal.RESET_AND_RETRY, "Impossible situation");
                    }
                default:
                    return new CoreGetMultiSignalAndReason(CoreGetMultiSignal.RESET_AND_RETRY, "Unknown ATR state " + state);
            }
        }
    }

    boolean hasExpiredClientSide(String place, Optional<String> docId) {
        boolean over = overall.hasExpiredClientSide();
        boolean hook = hooks.hasExpiredClientSideHook.apply(this, place, docId);

        if (over) LOGGER.info(attemptId, "expired in {}", place);
        if (hook) LOGGER.info(attemptId, "fake expiry in {}", place);

        return over || hook;
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
     * @param id the document's unique ID
     * @param content the content to insert
     * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResultInternal</code>
     */
    public CoreTransactionGetResult insert(CollectionIdentifier collection, String id, byte[] content, int flagsToStage, @Nullable CoreExpiry expiry, SpanWrapper pspan) {
        SpanWrapperUtil.addOperationAttribute(tracer(), pspan, TracingIdentifiers.TRANSACTION_OP_INSERT);
        return doKVOperation("insert " + DebugUtil.docId(collection, id), pspan, CoreTransactionAttemptContextHooks.HOOK_INSERT, collection, id,
                (operationId, span) -> insertInternal(operationId, collection, id, content, flagsToStage, expiry, span));
    }

    public Mono<CoreTransactionGetResult> insertReactive(CollectionIdentifier collection, String id, byte[] content, int flagsToStage, @Nullable CoreExpiry expiry, SpanWrapper pspan) {
        return Mono.fromCallable(() -> insert(collection, id, content, flagsToStage, expiry, pspan))
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    /**
     * @deprecated in favor of {@link #insert(CollectionIdentifier, String, byte[], int, CoreExpiry, SpanWrapper)}
     * which takes an additional 'flags' argument.
     */
    @UsedBy(SPRING_DATA_COUCHBASE)
    @Deprecated
    public Mono<CoreTransactionGetResult> insert(CollectionIdentifier collection, String id, byte[] content, SpanWrapper pspan) {
        return insertReactive(collection, id, content, JSON_COMMON_FLAGS, null, pspan);
    }

    private CoreTransactionGetResult insertInternal(String operationId, CollectionIdentifier collection, String id, byte[] content, int flagsToStage,
                                                    @Nullable CoreExpiry expiry,
                                                    SpanWrapper span) {
        if (queryModeLocked()) {
            if (expiry != null) {
                return featureNotAvailableKvExpiryInQueryMode();
            }
            return insertWithQueryLocked(collection, id, content, flagsToStage, span);
        } else {
            return insertWithKVLocked(operationId, collection, id, content, flagsToStage, expiry, span);
        }
    }

    private CoreTransactionGetResult insertWithKVLocked(String operationId, CollectionIdentifier collection, String id,
                                                        byte[] content, int flagsToStage,
                                                        @Nullable CoreExpiry expiry,
                                                        SpanWrapper span) {
        assertLocked("insertWithKV");
        Optional<StagedMutation> existing = findStagedMutationLocked(collection, id);

        if (existing.isPresent()) {
            StagedMutation op = existing.get();

            if (op.type == StagedMutationType.INSERT || op.type == StagedMutationType.REPLACE) {
                throw new DocumentExistsException(null);
            }

            // REMOVE handling is below
        }

        initAtrIfNeededLocked(collection, id, span);

        hooks.beforeUnlockInsert.accept(this, id); // testing hook

        unlock("standard");

        if (existing.isPresent() && existing.get().type == StagedMutationType.REMOVE) {
            // Use the doc of the remove to ensure CAS
            // It is ok to pass null for contentOfBody, since we are replacing a remove
            return createStagedReplace(operationId, existing.get().collection, existing.get().id, existing.get().cas,
                    existing.get().documentMetadata, existing.get().crc32, content, flagsToStage, null, flagsToStage, span, false,
                    expiry);
        } else {
            return createStagedInsert(operationId, collection, id, content, flagsToStage, span, Optional.empty(), expiry);
        }
    }

    private CoreTransactionGetResult insertWithQueryLocked(CollectionIdentifier collection, String id, byte[] content, int flags, SpanWrapper span) {
        requireNonBinaryContent(flags);
        ArrayNode params = Mapper.createArrayNode()
                .add(makeKeyspace(collection))
                .add(id)
                .addRawValue(new RawValue(new String(content, StandardCharsets.UTF_8)))
                .add(Mapper.createObjectNode());

        int sidx = queryStatementIdx.getAndIncrement();

        CoreQueryOptionsTransactions queryOptions = new CoreQueryOptionsTransactions();
        queryOptions.raw("args", params);

        // All KV ops are done with the scanConsistency set at the BEGIN WORK default, e.g. from
        // PerTransactionConfig & TransactionConfig
        try {
            CoreQueryResult result = queryWrapperBlockingLocked(sidx,
                    queryContext.queryContext,
                    "EXECUTE __insert",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_INSERT,
                    false,
                    true,
                    makeTxdata(),
                    params,
                    span,
                    false,
                    true);

            unlock("insertWithQueryLocked end");

            List<QueryChunkRow> rows = result.collectRows();

            if (rows.isEmpty()) {
                throw operationFailed(createError()
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
                    content,
                    flags,
                    transactionId(),
                    attemptId,
                    null, null, null, null,
                    cas,
                    Optional.empty());
        } catch (Exception err) {
            // Error has already gone through convertQueryError, and may be a TransactionOperationFailedException already
            if (err instanceof TransactionOperationFailedException) {
                throw err;
            }

            if (err instanceof DocumentExistsException) {
                throw err;
            }

            ErrorClass ec = classify(err);
            TransactionOperationFailedException out = operationFailed(createError().cause(err).build());
            span.recordExceptionAndSetErrorStatus(err);
            throw out;
        }
    }

    protected String randomAtrIdForVbucket(CoreTransactionAttemptContext self, Integer vbucketIdForDoc, int numAtrs) {
        return hooks.randomAtrIdForVbucket.apply(self)
                .orElse(ActiveTransactionRecordIds.randomAtrIdForVbucket(vbucketIdForDoc, numAtrs));
    }


    private void lock(String dbg) {
        if (threadSafetyEnabled) {
            if (lockDebugging) {
                logger().info(attemptId, "[LOCK] thread={} threadId{} waiting on lock for reason={}", Thread.currentThread().getName(), Thread.currentThread().getId(), dbg);
            }
            try {
                if (!mutex.tryLock(expiryRemainingMillis(), TimeUnit.MILLISECONDS)) {
                    throw operationFailed(createError()
                            .cause(new AttemptExpiredException("Attempt expired while waiting for lock at " + dbg))
                            .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                            .build());
                }
            } catch (InterruptedException e) {
                throw interrupted("locking " + dbg);
            }
            if (lockDebugging) {
                logger().info(attemptId, "[LOCK] thread={} threadId{} has acquired lock for reason={}", Thread.currentThread().getName(), Thread.currentThread().getId(), dbg);
            }
        }
    }

    private void unlock(String dbgExtra) {
        if (threadSafetyEnabled) {
            if (lockDebugging) {
                logger().info(attemptId, "[LOCK] thread={} threadId{} releasing lock for reason={}", Thread.currentThread().getName(), Thread.currentThread().getId(), dbgExtra);
            }
            // We have some double-unlocks to handle error paths, that will be superfluous in the standard golden path.
            if (mutex.isHeldByCurrentThread() && mutex.isLocked()) {
                try {
                    mutex.unlock();
                } catch (Throwable err) {
                    logger().info(attemptId, "[LOCK] Failed to unlock due to: {}", DebugUtil.dbg(err));
                    throw operationFailed(createError().cause(err).build());
                }
            } else {
                if (lockDebugging) {
                    logger().info(attemptId, "[LOCK] thread={} threadId{} superfluous unlock found for reason={}", Thread.currentThread().getName(), Thread.currentThread().getId(), dbgExtra);
                }
            }
        }
    }


    private boolean lockAndIncKVOps(String dbg) {
        lock(dbg);
        if (!kvOps.add(dbg)) {
            unlock(dbg + " - kvOps closed");
            return false;
        }

        return true;
    }

    private void waitForAllKVOpsThenLock(String dbg) {
        assertNotLocked(dbg);
        logger().info(attemptId, "waiting for {} KV ops finish for {}",
                kvOps.waitingCount(), dbg);
        if (threadSafetyEnabled) {
            kvOps.await(Duration.ofMillis(expiryRemainingMillis()));
        }
        lock(dbg);
        if (kvOps.waitingCount() > 0) {
            unlock(dbg + " still waiting for KV ops");
            waitForAllKVOpsThenLock(dbg + " still waiting for KV ops");
        }
    }

    private void waitForAllOpsThenDoUnderLock(String dbg,
                                              @Nullable SpanWrapper span,
                                              Runnable doUnderLock) {
        waitForAllOps(dbg);
        lock(dbg);
        try {
            doUnderLock.run();
        } catch (Exception err) {
            if (span != null) {
                span.span().status(RequestSpan.StatusCode.ERROR);
            }
            throw err;
        } finally {
            unlock("after doUnderLock");
        }
    }

    private void waitForAllOps(String dbg) {
        assertNotLocked(dbg);
        logger().info(attemptId, "waiting for {} KV ops in {}", kvOps.waitingCount(), dbg);
        if (threadSafetyEnabled) {
            kvOps.closeAndAwait(Duration.ofMillis(expiryRemainingMillis()));
        }
    }

    /**
     * Mutates the specified <code>doc</code> with new content, using the
     * document's last {@link CoreTransactionGetResult#cas()}.
     *
     * @param doc the doc to be mutated
     * @param content the content to replace the doc with
     * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
     * object is modified.
     */
    public CoreTransactionGetResult replace(CoreTransactionGetResult doc, byte[] content, int flags, @Nullable CoreExpiry expiry, SpanWrapper pspan) {
        SpanWrapperUtil.addOperationAttribute(tracer(), pspan, TracingIdentifiers.TRANSACTION_OP_REPLACE);
        return doKVOperation("replace " + DebugUtil.docId(doc), pspan, CoreTransactionAttemptContextHooks.HOOK_REPLACE, doc.collection(), doc.id(),
                (operationId, span) -> replaceInternalLocked(operationId, doc, content, flags, expiry, span));
    }

    public Mono<CoreTransactionGetResult> replaceReactive(CoreTransactionGetResult doc, byte[] content, int flags, @Nullable CoreExpiry expiry, SpanWrapper pspan) {
        return Mono.fromCallable(() -> replace(doc, content, flags, expiry, pspan))
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    /**
     * @deprecated in favor of {@link #replace(CoreTransactionGetResult, byte[], int, CoreExpiry, SpanWrapper)}
     * which takes an additional 'flags' argument.
     */
    @UsedBy(SPRING_DATA_COUCHBASE)
    @Deprecated
    public Mono<CoreTransactionGetResult> replace(CoreTransactionGetResult doc, byte[] content, SpanWrapper pspan) {
        return replaceReactive(doc, content, JSON_COMMON_FLAGS, null, pspan);
    }

    /**
     * Provide generic support functionality around KV operations (including those done in queryMode), including locking.
     * <p>
     * The callback is required to unlock the mutex iff it returns without error. This method will handle unlocking on errors.
     */
    private <T> T doKVOperation(String lockDebugOrig, SpanWrapper span, String stageName, @Nullable CollectionIdentifier docCollection, @Nullable String docId,
                                BiFunction<String, SpanWrapper, T> op) {
        String operationId = UUID.randomUUID().toString();
        // If two operations on the same doc are done concurrently it can be unclear, so include a partial of the operation id
        String lockDebug = lockDebugOrig + " - " + operationId.substring(0, TransactionLogEvent.CHARS_TO_LOG);
        SpanWrapperUtil.setAttributes(span, core.coreResources().requestTracerAndDecorator(), this, docCollection, docId);
        // We don't attach the opid to the span, it's too low cardinality to be useful

        if (!lockAndIncKVOps(lockDebug)) {
            TransactionOperationFailedException err = operationFailed(createError()
                    .cause(new IllegalStateException("Transaction is finalizing and cannot accept new operations"))
                    .doNotRollbackAttempt()
                    .build());
            span.finish(err);
            throw err;
        }

        try {
            TransactionOperationFailedException returnEarly = canPerformOperation(lockDebug);
            if (returnEarly != null) {
                throw returnEarly;
            }

            if (hasExpiredClientSide(stageName, Optional.ofNullable(docId))) {
                LOGGER.info(attemptId, "has expired in stage {}, setting expiry-overtime-mode", stageName);

                // Combo of setting this mode and throwing AttemptExpired will result in an attempt to
                // rollback, which will ignore expiries, and bail out if anything fails
                expiryOvertimeMode = true;

                throw operationFailed(createError()
                        .cause(new AttemptExpiredException("Attempt expired in stage " + stageName))
                        .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                        .build());
            }

            return op.apply(operationId, span);
        } catch (RuntimeException err) {
            span.setErrorStatus();
            throw err;
        } finally {
            // The callback is required to unlock the mutex iff it returns without error, but for safely, always unlock.
            if (!threadSafetyEnabled || mutex.isHeldByCurrentThread()) {
                unlock(lockDebug);
            }
            kvOps.done();
            span.finish();
        }
    }

    /**
     * Doesn't need everything from doKVOperation, as queryWrapper already centralises a lot of logic
     */
    public <T> T doQueryOperation(String lockDebugIn, String statement, @Nullable final CoreQueryOptions opts,
                                  @Nullable SpanWrapper pspan, BiFunction<Integer, SpanWrapper, T> op) {
        int sidx = queryStatementIdx.getAndIncrement();
        String lockDebug = lockDebugIn + " q" + sidx;
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_QUERY, pspan);

        boolean parametersUsed = opts != null && (opts.positionalParameters() != null || opts.namedParameters() != null);
        tip().provideQueryStatementIfSafe(TracingAttribute.STATEMENT, span.span(), statement, parametersUsed);

        // Query is done under lock, except for BEGIN WORK which needs to relock.
        lock(lockDebug);

        try {
            return op.apply(sidx, span);
        } catch (RuntimeException err) {
            span.setErrorStatus();
            throw err;
        } finally {
            unlock("doQueryOperation");
            span.finish();
        }
    }

    private CoreTransactionGetResult featureNotAvailableKvExpiryInQueryMode() {
        FeatureNotAvailableException err = new FeatureNotAvailableException("Expiry cannot be specified for KV operations if SQL++ statements are also involved in the same transaction");
        logger().error("Fast failing with " + err);
        throw operationFailed(createError()
                .cause(err)
                .build());
    }

    private CoreTransactionGetResult replaceInternalLocked(String operationId,
                                                           CoreTransactionGetResult doc,
                                                           byte[] content,
                                                           int flags,
                                                           @Nullable CoreExpiry expiry,
                                                           SpanWrapper pspan) {
        LOGGER.info(attemptId, "replace doc {}, operationId = {}", doc, operationId);

        if (queryModeLocked()) {
            if (expiry != null) {
                return featureNotAvailableKvExpiryInQueryMode();
            }
            return replaceWithQueryLocked(doc, content, flags, pspan);
        } else {
            return replaceWithKVLocked(operationId, doc, content, flags, expiry, pspan);
        }
    }

    private CoreTransactionGetResult replaceWithKVLocked(String operationId, CoreTransactionGetResult doc, byte[] content,
                                                         int flags, @Nullable CoreExpiry expiry, SpanWrapper pspan) {
        Optional<StagedMutation> existing = findStagedMutationLocked(doc);
        boolean mayNeedToWriteATR = state == AttemptState.NOT_STARTED;

        hooks.beforeUnlockReplace.accept(this, doc.id());

        unlock("standard");

        if (existing.isPresent()) {
            StagedMutation op = existing.get();

            if (op.type == StagedMutationType.REMOVE) {
                throw operationFailed(createError()
                        .cause(new DocumentNotFoundException(null))
                        .build());
            }

            // StagedMutationType.INSERT handling is below
        }

        checkAndHandleBlockingTxn(doc, pspan, ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_REPLACING, existing);

        initATRIfNeeded(mayNeedToWriteATR, doc.collection(), doc.id(), pspan);

        if (existing.isPresent() && existing.get().type == StagedMutationType.INSERT) {
            return createStagedInsert(operationId, doc.collection(), doc.id(), content, flags, pspan,
                    Optional.of(doc.cas()), expiry);
        } else {
            return createStagedReplace(operationId, doc.collection(), doc.id(), doc.cas(),
                    doc.documentMetadata(), doc.crc32OfGet(), content, flags, doc.contentAsBytes(),
                    doc.userFlags(), pspan, doc.links().isDeleted(), expiry);
        }
    }

    private void initAtrIfNeededLocked(CollectionIdentifier docCollection,
                                       String docId,
                                       SpanWrapper pspan) {
        assertLocked("initAtrIfNeededLocked");
        if (state == AttemptState.NOT_STARTED) {
            CollectionIdentifier atrCollection = selectAtrLocked(docCollection, docId);
            atrPendingLocked(atrCollection, pspan);
        }
    }

    private void initATRIfNeeded(boolean mayNeedToWriteATR,
                                 CollectionIdentifier docCollection,
                                 String docId,
                                 SpanWrapper pspan) {
        if (mayNeedToWriteATR) {
            doUnderLock("before ATR " + DebugUtil.docId(docCollection, docId),
                    () -> initAtrIfNeededLocked(docCollection, docId, pspan));
        }
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

        LOGGER.info(attemptId, "First mutated doc in txn is '{}' on vbucket {}, so using atr {}",
                DebugUtil.docId(docCollection, docId), vbucketIdForDoc, atr);

        return atrCollection.get();
    }

    private void requireNonBinaryContent(int userFlags) {
        if (CodecFlags.extractCommonFormatFlags(userFlags) == CodecFlags.CommonFlags.BINARY.ordinal()) {
            RuntimeException cause = new FeatureNotAvailableException("Binary documents are only supported in a KV-only transaction");
            throw operationFailed(createError().cause(cause).build());
        }
    }

    private CoreTransactionGetResult replaceWithQueryLocked(CoreTransactionGetResult doc, byte[] content, int flags, SpanWrapper span) {
        requireNonBinaryContent(flags);
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

        CoreQueryOptionsTransactions queryOptions = new CoreQueryOptionsTransactions();
        queryOptions.raw("args", params);

        try {
            CoreQueryResult result = queryWrapperBlockingLocked(sidx,
                    queryContext.queryContext,
                    "EXECUTE __update",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_REPLACE,
                    false,
                    true,
                    txData,
                    params,
                    span,
                    false,
                    true);

            unlock("replaceWithQueryLocked end");
            List<QueryChunkRow> rows = result.collectRows();

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
                    JSON_COMMON_FLAGS, // If using query, currently is must be JSON (binary not supported)
                    cas,
                    doc.collection(),
                    null,
                    Optional.empty(),
                    Optional.empty(),
                    crc32);
        } catch (Exception err) {
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder builder = createError().cause(err);
            span.recordExceptionAndSetErrorStatus(err);

            if (err instanceof TransactionOperationFailedException) {
                throw err;
            } else if (ec == FAIL_DOC_NOT_FOUND
                    || ec == FAIL_CAS_MISMATCH) {
                TransactionOperationFailedException out = operationFailed(builder.retryTransaction().build());
                throw out;
            } else {
                TransactionOperationFailedException out = operationFailed(builder.build());
                throw out;
            }
        }
    }

    private void removeWithQueryLocked(CoreTransactionGetResult doc, SpanWrapper span) {
        ObjectNode txData = makeTxdata();
        txData.put("scas", Long.toString(doc.cas()));
        doc.txnMeta().ifPresent(v -> txData.set("txnMeta", v));
        ArrayNode params = Mapper.createArrayNode()
                .add(makeKeyspace(doc.collection()))
                .add(doc.id())
                .add(Mapper.createObjectNode());

        int sidx = queryStatementIdx.getAndIncrement();

        CoreQueryOptionsTransactions queryOptions = new CoreQueryOptionsTransactions();
        queryOptions.raw("args", params);

        try {
            queryWrapperBlockingLocked(sidx,
                    queryContext.queryContext,
                    "EXECUTE __delete",
                    queryOptions,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_KV_REMOVE,
                    false,
                    true,
                    txData,
                    params,
                    span,
                    false,
                    true);
        } catch (Exception err) {
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder builder = createError().cause(err);
            span.recordExceptionAndSetErrorStatus(err);

            if (err instanceof TransactionOperationFailedException) {
                throw err;
            } else if (ec == FAIL_DOC_NOT_FOUND
                    || ec == FAIL_CAS_MISMATCH) {
                TransactionOperationFailedException out = operationFailed(builder.retryTransaction().build());
                throw out;
            } else {
                TransactionOperationFailedException out = operationFailed(builder.build());
                throw out;
            }
        } finally {
            unlock("removeWithQueryLocked end");
        }
    }

    private void forwardCompatibilityCheck(ForwardCompatibilityStage stage,
                                           Optional<ForwardCompatibility> fc) {
        try {
            ForwardCompatibility.check(core, stage, fc, logger(), overall.supported());
        } catch (Exception err) {
            TransactionOperationFailedException.Builder error = createError()
                    .cause(new ForwardCompatibilityFailureException());
            if (err instanceof ForwardCompatibilityRequiresRetryException) {
                error.retryTransaction();
            }
            throw operationFailed(error.build());
        }
    }

    private void checkATREntryForBlockingDocInternal(CoreTransactionGetResult doc,
                                                     CollectionIdentifier collection,
                                                     SpanWrapper span,
                                                     MeteringUnits.MeteringUnitsBuilder units) {

        try {
            // Produces 5 reads per second when blocked, and blocks up to one second
            BlockingRetryHandler retry = BlockingRetryHandler.builder(Duration.ofMillis(50), Duration.ofMillis(500))
                    .maxDuration(Duration.ofSeconds(1))
                    .build();
            do {
                retry.shouldRetry(false);
                checkExpiryPreCommitAndSetExpiryOvertimeMode("staging.check_atr_entry_blocking_doc", Optional.empty());

                hooks.beforeCheckATREntryForBlockingDoc.accept(this, doc.links().atrId().get());

                Optional<ActiveTransactionRecordEntry> atrEntry = ActiveTransactionRecord.findEntryForTransaction(core, collection, doc.links().atrId().get(),
                        doc.links().stagedAttemptId().get(),
                        config,
                        span,
                        logger(),
                        units,
                        null);
                if (atrEntry.isPresent()) {
                    ActiveTransactionRecordEntry ae = atrEntry.get();

                    LOGGER.info(attemptId, "fetched ATR entry for blocking txn: hasExpired={} entry={}",
                            ae.hasExpired(), ae);

                    forwardCompatibilityCheck(ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_READING_ATR, ae.forwardCompatibility());

                    switch (ae.state()) {
                        case COMPLETED:
                        case ROLLED_BACK:
                            LOGGER.info(attemptId, "ATR entry state of {} indicates we can proceed to overwrite",
                                    atrEntry.get().state());

                            return;
                        default:
                            LOGGER.info(attemptId, "still blocked by a valid transaction, retrying to unlock documents");
                            break;
                    }

                    retry.sleepForNextBackoffAndSetShouldRetry();
                } else {
                    LOGGER.info(attemptId,
                            "blocking txn {}'s entry has been removed indicating the txn expired, so proceeding " +
                                    "to overwrite",
                            doc.links().stagedAttemptId().get());

                    return;
                }
            } while (retry.shouldRetry());

            LOGGER.info(attemptId, "still blocked by a valid transaction, retrying to unlock documents");

            throw operationFailed(createError()
                    .retryTransaction()
                    .build());

        } catch (Exception err) {
            if (err instanceof DocumentNotFoundException) {
                LOGGER.info(attemptId, "blocking txn's ATR has been removed so proceeding to overwrite");
                return;
            } else {
                LOGGER.warn(attemptId, "got error in checkATREntryForBlockingDoc: {}", dbg(err));
            }

            throw operationFailed(createError()
                    .cause(err)
                    .retryTransaction()
                    .build());
        }
    }

    private void checkATREntryForBlockingDoc(CoreTransactionGetResult doc, SpanWrapper pspan) {
        CollectionIdentifier collection = doc.links().collection();
        MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

        try {
            checkATREntryForBlockingDocInternal(doc, collection, pspan, units);
        } finally {
            addUnits(units.build());
        }
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

    private RequestTracerAndDecorator tracer() {
        return core.context().coreResources().requestTracerAndDecorator();
    }

    private TracingDecorator tip() {
        return core.context().coreResources().tracingDecorator();
    }

    private byte[] serialize(Object in) {
        try {
            return Mapper.writer().writeValueAsBytes(in);
        } catch (JsonProcessingException e) {
            throw new DecodingFailureException(e);
        }
    }

    private void atrPendingLocked(CollectionIdentifier collection, SpanWrapper pspan) {
        assertLocked("atrPending");
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_PENDING, pspan);

        String prefix = "attempts." + attemptId;

        if (!atrId.isPresent()) throw new IllegalStateException("atrId not present");

        try {
            LOGGER.info(attemptId, "about to set ATR {} to Pending", getAtrDebug(collection, atrId));
            errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_PENDING, Optional.empty());

            hooks.beforeAtrPending.accept(this); // Testing hook

            SubdocMutateResponse result = TransactionKVHandler.mutateIn(core, collection, atrId.get(), kvTimeoutMutating(),
                    false, true, false, false, false, 0, BINARY_COMMON_FLAGS, durabilityLevel(), OptionsUtil.createClientContext("atrPending"), span,
                    Arrays.asList(
                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_TRANSACTION_ID, serialize(transactionId()), true, true, false, 0),
                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_STATUS, serialize(AttemptState.PENDING.name()), false, true, false, 1),
                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_START_TIMESTAMP, serialize("${Mutation.CAS}"), false, true, true, 2),
                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_EXPIRES_AFTER_MILLIS, serialize(expiryRemainingMillis()), false, true, false, 3),
                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_DURABILITY_LEVEL, serialize(DurabilityLevelUtil.convertDurabilityLevel(config.durabilityLevel())), false, true, false, 3),
                            new SubdocMutateRequest.Command(SubdocCommandType.SET_DOC, "", new byte[]{0}, false, false, false, 4)
                    ));

            // Testing hook
            hooks.afterAtrPending.accept(this);

            long elapsed = span.elapsedMicros();
            addUnits(result.flexibleExtras());
            LOGGER.info(attemptId, "set ATR {} to Pending in {}us{}", getAtrDebug(collection, atrId), elapsed,
                    DebugUtil.dbg(result.flexibleExtras()));
            setStateLocked(AttemptState.PENDING);
            overall.cleanup().addToCleanupSet(collection);

        } catch (Exception err) {
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder out = createError().cause(err);
            MeteringUnits units = addUnits(MeteringUnits.from(err));
            span.recordException(err);
            long elapsed = span.elapsedMicros();

            LOGGER.info(attemptId, "error while setting ATR {} to Pending{} in {}us: {}",
                    getAtrDebug(collection, atrId), DebugUtil.dbg(units), elapsed, dbg(err));

            if (expiryOvertimeMode) {
                mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_ATR_PENDING, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
            } else if (ec == FAIL_EXPIRY) {
                throw setExpiryOvertimeModeAndFail(err, CoreTransactionAttemptContextHooks.HOOK_ATR_PENDING);
            } else if (ec == FAIL_ATR_FULL) {
                throw operationFailed(out.cause(new ActiveTransactionRecordFullException(err)).build());
            } else if (ec == FAIL_AMBIGUOUS) {
                LOGGER.info(attemptId, "retrying the op on {} to resolve ambiguity", ec);
                sleep(DEFAULT_DELAY_RETRYING_OPERATION);
                atrPendingLocked(collection, span);
            } else if (ec == FAIL_PATH_ALREADY_EXISTS) {
                LOGGER.info(attemptId, "assuming this is caused by resolved ambiguity, and proceeding as though successful", ec);
                return;
            } else if (ec == FAIL_TRANSIENT) {
                LOGGER.info(attemptId, "transient error likely to be solved by retry", ec);
                throw operationFailed(out.retryTransaction().build());
            } else if (ec == FAIL_HARD) {
                throw operationFailed(out.doNotRollbackAttempt().build());
            } else {
                throw operationFailed(out.build());
            }
        } finally {
            span.finish();
        }
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            throw interrupted("sleep");
        }
    }

    private void setStateLocked(AttemptState newState) {
        assertLocked("setState " + newState);
        logger().info(attemptId, "changed state to {}", newState);
        state = newState;
    }


    /*
     * Stage a replace on a document, putting the staged mutation into the doc's xattrs.  The document's content is
     * left unchanged.
     * documentMetadata is optional to handle insert->replace case
     */
    private CoreTransactionGetResult
    createStagedReplace(String operationId,
                        CollectionIdentifier collection,
                        String id,
                        long cas,
                        Optional<DocumentMetadata> documentMetadata,
                        Optional<String> crc32OfGet,
                        byte[] contentToStage,
                        int userFlagsOfContentToStage,
                        byte[] contentOfExistingDocument,
                        int userFlagsOfExistingDocument,
                        SpanWrapper pspan,
                        boolean accessDeleted,
                        @Nullable CoreExpiry expiry) {
        assertNotLocked("createStagedReplace");
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_REPLACE_STAGE, pspan);

        try {
            boolean isBinary = CodecFlags.extractCommonFormatFlags(userFlagsOfContentToStage) == CodecFlags.CommonFlags.BINARY.ordinal();
            byte[] txn = createDocumentMetadata(OperationTypes.REPLACE, operationId, documentMetadata, userFlagsOfContentToStage, expiry);

            List<SubdocMutateRequest.Command> specList = new ArrayList<>();
            specList.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn", txn, true, true, false, specList.size()));
            specList.add(isBinary ? new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn.op.stgd", NEAR_EMPTY_BYTE_ARRAY, false, true, false, specList.size())
                    : new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn.op.bin", NEAR_EMPTY_BYTE_ARRAY, false, true, false, specList.size()));
            specList.add(isBinary ? new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn.op.stgd", null, false, true, false, specList.size())
                    : new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn.op.bin", null, false, true, false, specList.size()));
            specList.add(isBinary ? new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "txn.op.bin", contentToStage, false, true, false, true, specList.size())
                    : new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "txn.op.stgd", contentToStage, false, true, false, false, specList.size()));
            specList.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "txn.op.crc32", serialize("${Mutation.value_crc32c}"), false, true, true, specList.size()));

            hooks.beforeStagedReplace.accept(this, id); // test hook

            SubdocMutateResponse updatedDoc = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                    false, false, false, true, false, cas, userFlagsOfExistingDocument, durabilityLevel(), OptionsUtil.createClientContext("createStagedReplace"), span,
                    specList);

            LOGGER.info(attemptId, "about to replace doc {} with cas {}, accessDeleted={}, expiry {}",
                    DebugUtil.docId(collection, id), cas, accessDeleted, expiry);

            // Testing hook
            hooks.afterStagedReplaceComplete.accept(this, id);

            long elapsed = span.elapsedMicros();
            addUnits(updatedDoc.flexibleExtras());
            LOGGER.info(attemptId, "replaced doc {}{} got cas {}, in {}us",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(updatedDoc.flexibleExtras()), updatedDoc.cas(), elapsed);

            // Save the new CAS
            CoreTransactionGetResult out = createTransactionGetResult(operationId, collection, id,
                    contentToStage, userFlagsOfExistingDocument, contentToStage, userFlagsOfContentToStage, updatedDoc.cas(), documentMetadata, OperationTypes.REPLACE, crc32OfGet, Optional.ofNullable(expiry));
            boolean supports = supportsReplaceBodyWithXattr(collection.bucket());
            addStagedMutation(new StagedMutation(operationId, id, collection, updatedDoc.cas(), documentMetadata, crc32OfGet,
                    userFlagsOfExistingDocument, supports ? null : contentToStage, userFlagsOfContentToStage, StagedMutationType.REPLACE, expiry));
            return out;
        } catch (Exception err) {
            CoreTransactionGetResult result = handleErrorOnStagedMutation("replacing", collection, id, err, span, crc32OfGet,
                    (newCas) -> createStagedReplace(operationId, collection, id, newCas, documentMetadata,
                            crc32OfGet, contentToStage, userFlagsOfContentToStage, contentOfExistingDocument,
                            userFlagsOfExistingDocument, pspan, accessDeleted, expiry));
            span.recordException(err);
            return result;
        } finally {
            span.finish();
        }
    }

    /**
     * getWithKv: possibly created from StagedMutation, which may not have content. Could get chained to replace/remove later,
     * but these don't look at the staged content of the doc
     * createStagedReplace: existing doc, preserve metadata, has body
     * createStagedInsert: new doc, empty contents, all in links
     */
    private CoreTransactionGetResult createTransactionGetResult(String operationId,
                                                                CollectionIdentifier collection,
                                                                String id,
                                                                @Nullable byte[] bodyContent,
                                                                int currentUserFlags,
                                                                @Nullable byte[] stagedContent,
                                                                int stagedUserFlags,
                                                                long cas,
                                                                Optional<DocumentMetadata> documentMetadata,
                                                                String opType,
                                                                Optional<String> crc32OfFetch,
                                                                Optional<CoreExpiry> expiry) {
        boolean isBinary = CodecFlags.extractCommonFormatFlags(stagedUserFlags) == CodecFlags.CommonFlags.BINARY.ordinal();
        TransactionLinks links = new TransactionLinks(
                isBinary ? Optional.empty() : Optional.ofNullable(stagedContent),
                !isBinary ? Optional.empty() : Optional.ofNullable(stagedContent),
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
                Optional.of(operationId),
                Optional.of(stagedUserFlags),
                expiry
        );

        return new CoreTransactionGetResult(id,
                bodyContent,
                currentUserFlags,
                cas,
                collection,
                links,
                documentMetadata,
                Optional.empty(),
                crc32OfFetch
        );
    }

    private byte[] createDocumentMetadata(String opType,
                                          String operationId,
                                          Optional<DocumentMetadata> documentMetadata,
                                          int userFlagsToStage,
                                          @Nullable CoreExpiry expiry) {
        boolean isBinary = CodecFlags.extractCommonFormatFlags(userFlagsToStage) == CodecFlags.CommonFlags.BINARY.ordinal();

        ObjectNode op = Mapper.createObjectNode();
        op.put("type", opType);

        ObjectNode aux = Mapper.createObjectNode();
        aux.put("uf", userFlagsToStage);
        if (expiry != null) {
            expiry.when(
                    absolute -> {
                        aux.put("docexpiry", absolute.getEpochSecond());
                        logger().info(attemptId, "For operation {} specifying docexpiry of {}", operationId, expiry);
                    },
                    relative -> {
                        long epochSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + relative.getSeconds();
                        if (epochSecond > LATEST_VALID_EXPIRY_INSTANT.getEpochSecond()) {
                            throw InvalidArgumentException.fromMessage(
                                    "Requested expiry duration " + relative + " is too long; the final expiry time must be <= " + LATEST_VALID_EXPIRY_INSTANT
                            );
                        }
                        aux.put("docexpiry", epochSecond);
                        logger().info(attemptId, "For operation {} specifying docexpiry of {} from {} ", operationId, epochSecond, relative);
                    }, () -> {
                        // no-op
                    });
        }

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
        ret.set("aux", aux);

        ObjectNode restore = Mapper.createObjectNode();

        documentMetadata.map(DocumentMetadata::cas).ifPresent(v -> restore.put("CAS", v));
        documentMetadata.map(DocumentMetadata::exptime).ifPresent(v -> restore.put("exptime", v));
        documentMetadata.map(DocumentMetadata::revid).ifPresent(v -> restore.put("revid", v));

        if (restore.size() > 0) {
            ret.set("restore", restore);
        }

        if (isBinary && (opType.equals(OperationTypes.REPLACE) || opType.equals(OperationTypes.INSERT))) {
            ObjectNode fce = Mapper.createObjectNode()
                    .put("e", CoreTransactionsExtension.EXT_BINARY_SUPPORT.value())
                    .put("b", "f");
            ArrayNode fcea = Mapper.createArrayNode().add(fce);
            ObjectNode fc = Mapper.createObjectNode();
            fc.set(ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_INSERTING.value(), fcea);
            fc.set(ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_INSERTING_GET.value(), fcea);
            fc.set(ForwardCompatibilityStage.GETS.value(), fcea);
            fc.set(ForwardCompatibilityStage.CLEANUP_ENTRY.value(), fcea);
            ret.set("fc", fc);
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
    private void createStagedRemove(String operationId, CoreTransactionGetResult doc, long cas, SpanWrapper pspan, boolean accessDeleted) {
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), doc.collection(), doc.id(), TracingIdentifiers.TRANSACTION_OP_REMOVE_STAGE, pspan);

        try {
            LOGGER.info(attemptId, "about to remove doc {} with cas {}", DebugUtil.docId(doc), cas);

            byte[] txn = createDocumentMetadata(OperationTypes.REMOVE, operationId, doc.documentMetadata(), doc.userFlags(), null);

            hooks.beforeStagedRemove.accept(this, doc.id());

            SubdocMutateResponse response = TransactionKVHandler.mutateIn(core, doc.collection(), doc.id(), kvTimeoutMutating(),
                    false, false, false, accessDeleted, false, cas, doc.userFlags(), durabilityLevel(), OptionsUtil.createClientContext("createStagedReplace"), span,
                    Arrays.asList(
                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn", txn, true, true, false, 0),
                            new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, "txn.op.crc32", serialize("${Mutation.value_crc32c}"), false, true, true, 2)
                    ));

            hooks.afterStagedRemoveComplete.accept(this, doc.id());

            long elapsed = span.elapsedMicros();
            addUnits(response.flexibleExtras());
            LOGGER.info(attemptId, "staged remove of doc {}{} got cas {}, in {}us",
                    DebugUtil.docId(doc), DebugUtil.dbg(response.flexibleExtras()), response.cas(), elapsed);
            // Save so the staged mutation can be committed/rolled back with the correct CAS
            doc.cas(response.cas());
            addStagedMutation(new StagedMutation(operationId, doc.id(), doc.collection(), doc.cas(),
                    doc.documentMetadata(), doc.crc32OfGet(), doc.userFlags(), null, 0 /* unused */, StagedMutationType.REMOVE, null));
        } catch (Exception err) {
            handleErrorOnStagedMutation("removing", doc.collection(), doc.id(), err, span, doc.crc32OfGet(),
                    (newCas) -> {
                        createStagedRemove(operationId, doc, newCas, span, accessDeleted);
                        return null;
                    });
            span.recordException(err);
        } finally {
            span.finish();
        }
    }

    private void doUnderLock(String dbg, Runnable whileLocked) {
        lock(dbg);
        try {
            whileLocked.run();
        } finally {
            unlock("doUnderLock");
        }
    }

    private void addStagedMutation(StagedMutation sm) {
        doUnderLock("addStagedMutation " + DebugUtil.docId(sm.collection, sm.id),
                () -> {
                    removeStagedMutationLocked(sm.collection, sm.id);
                    stagedMutationsLocked.add(sm);
                });
    }

    private @Nullable CoreTransactionGetResult handleErrorOnStagedMutation(String stage, CollectionIdentifier collection, String id, Throwable err, SpanWrapper pspan,
                                                                           Optional<String> crc32FromGet, Function<Long, CoreTransactionGetResult> callback) {
        ErrorClass ec = classify(err);
        TransactionOperationFailedException.Builder out = createError().cause(err);
        MeteringUnits units = addUnits(MeteringUnits.from(err));

        LOGGER.info(attemptId, "error while {} doc {}{} in {}us: {}",
                stage, DebugUtil.docId(collection, id), DebugUtil.dbg(units), pspan.elapsedMicros(), dbg(err));

        if (expiryOvertimeMode) {
            LOGGER.warn(attemptId, "should not reach here in expiryOvertimeMode");
        }

        if (ec == FAIL_EXPIRY) {
            throw setExpiryOvertimeModeAndFail(err, stage);
        } else if (ec == FAIL_CAS_MISMATCH) {
            return handleDocChangedDuringStaging(pspan, id, collection, crc32FromGet, callback);
        } else if (ec == FAIL_DOC_NOT_FOUND) {
            throw operationFailed(createError().retryTransaction().build());
        } else if (ec == FAIL_AMBIGUOUS || ec == FAIL_TRANSIENT) {
            throw operationFailed(out.retryTransaction().build());
        } else if (ec == FAIL_HARD) {
            throw operationFailed(out.doNotRollbackAttempt().build());
        } else {
            throw operationFailed(out.build());
        }
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

    private CoreTransactionGetResult handleDocExistsDuringStagedInsert(String operationId,
                                                                       CollectionIdentifier collection,
                                                                       String id,
                                                                       byte[] content,
                                                                       int flags,
                                                                       SpanWrapper pspan,
                                                                       @Nullable CoreExpiry expiry) {
        String bp = "DocExists on " + DebugUtil.docId(collection, id) + ": ";
        MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

        Optional<Tuple2<CoreTransactionGetResult, CoreSubdocGetResult>> result;
        try {
            hooks.beforeGetDocInExistsDuringStagedInsert.accept(this, id); // testing hook

            LOGGER.info(attemptId, "{} getting doc (which may be a tombstone)", bp);

            result = DocumentGetter.justGetDoc(core, collection, id, kvTimeoutNonMutating(), pspan, true, logger(), units, false);
        } catch (Exception err) {
            addUnits(units.build());
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder e = createError().cause(err);

            LOGGER.warn(attemptId, "{} got error while getting doc: {}", bp, dbg(err));

            // FAIL_DOC_NOT_FOUND case is handled by the ifPresent() check below
            if (ec == FAIL_TRANSIENT || ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                e.retryTransaction();
            }

            throw operationFailed(e.build());
        }

        if (result.isPresent()) {
            Tuple2<CoreTransactionGetResult, CoreSubdocGetResult> results = result.get();
            CoreTransactionGetResult r = results.getT1();
            CoreSubdocGetResult lir = results.getT2();
            MeteringUnits built = addUnits(units.build());

            LOGGER.info(attemptId, "{} doc {} exists inTransaction={} isDeleted={}{}",
                    bp, DebugUtil.docId(collection, id), r.links(), lir.tombstone(), DebugUtil.dbg(built));

            forwardCompatibilityCheck(ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_INSERTING_GET, r.links().forwardCompatibility());

            if (lir.tombstone() && !r.links().isDocumentInTransaction()) {
                LOGGER.info(attemptId, "{} doc {} is a regular tombstone without txn metadata, proceeding to overwrite",
                        bp, DebugUtil.docId(collection, id));

                return createStagedInsert(operationId, collection, id, content, flags, pspan, Optional.of(r.cas()), expiry);
            } else if (!r.links().isDocumentInTransaction()) {
                LOGGER.info(attemptId, "{} doc {} exists but is not in txn, raising " +
                        "DocumentExistsException", bp, DebugUtil.docId(collection, id));

                throw new DocumentExistsException(ReducedKeyValueErrorContext.create(id));
            } else {
                if (r.links().stagedAttemptId().get().equals(attemptId)) {
                    // stagedOperationId must be present as this transaction is writing it
                    if (r.links().stagedOperationId().isPresent() && r.links().stagedOperationId().get().equals(operationId)) {
                        LOGGER.info(attemptId, "{} doc {} has the same operation id, must be a resolved ambiguity, proceeding",
                                bp, DebugUtil.docId(collection, id));

                        addStagedMutation(new StagedMutation(operationId, r.id(), r.collection(), r.cas(),
                                // Since we are resolving an ambiguous write, `flags` should match with r.links().stagedContentJsonOrBinary()
                                r.documentMetadata(), r.crc32OfGet(), flags, r.links().stagedContentJsonOrBinary().get(), flags, StagedMutationType.INSERT, expiry));
                        return r;
                    }

                    LOGGER.info(attemptId, "{} doc {} has the same attempt id but a different operation id, must be racing with a concurrent attempt to write the same doc",
                            bp, DebugUtil.docId(collection, id));

                    throw operationFailed(createError()
                            .cause(new ConcurrentOperationsDetectedOnSameDocumentException())
                            .build());
                }

                // BF-CBD-3787
                if (!r.links().op().get().equals(OperationTypes.INSERT)) {
                    LOGGER.info(attemptId, "{} doc {} is in a txn but is not a staged insert, raising " +
                            "DocumentExistsException", bp, DebugUtil.docId(collection, id));

                    throw new DocumentExistsException(ReducedKeyValueErrorContext.create(id));
                }

                // Will return Mono.empty if it's safe to overwrite, Mono.error otherwise
                else {
                    checkAndHandleBlockingTxn(r, pspan, ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_INSERTING, Optional.empty());

                    return overwriteStagedInsert(operationId, collection, id, content, flags, pspan, bp, r, lir, expiry);
                }
            }

        } else {
            LOGGER.info(attemptId, "{} completed get of {}, could not find, throwing to retry txn which " +
                    "should succeed now", bp, DebugUtil.docId(collection, id));
            throw operationFailed(createError()
                    .retryTransaction()
                    .build());
        }
    }

    private CoreTransactionGetResult overwriteStagedInsert(String operationId,
                                                           CollectionIdentifier collection,
                                                           String id,
                                                           byte[] content,
                                                           int flags,
                                                           SpanWrapper pspan,
                                                           String bp,
                                                           CoreTransactionGetResult r,
                                                           CoreSubdocGetResult lir,
                                                           @Nullable CoreExpiry expiry) {
        CbPreconditions.check(r.links().isDocumentInTransaction());
        CbPreconditions.check(r.links().op().get().equals(OperationTypes.INSERT));

        if (lir.tombstone()) {
            return createStagedInsert(operationId, collection, id, content, flags, pspan, Optional.of(r.cas()), expiry);
            }
            else {
            LOGGER.info(attemptId, "{} removing {} as it's a protocol 1.0 staged insert",
                    bp, DebugUtil.docId(collection, id));

            try {
                hooks.beforeOverwritingStagedInsertRemoval.accept(this, id);

                RemoveResponse v = TransactionKVHandler.remove(core, collection, id, kvTimeoutMutating(), lir.cas(), durabilityLevel(),
                        OptionsUtil.createClientContext("overwriteStagedInsert"), pspan);

                addUnits(v.flexibleExtras());

            } catch (Exception err) {
                MeteringUnits units = addUnits(MeteringUnits.from(err));

                LOGGER.warn(attemptId, "{} hit error {} while removing {}{}",
                        bp, DebugUtil.dbg(err), DebugUtil.docId(collection, id), DebugUtil.dbg(units));

                ErrorClass ec = classify(err);
                TransactionOperationFailedException.Builder out = createError().cause(err);

                if (ec == FAIL_DOC_NOT_FOUND || ec == FAIL_CAS_MISMATCH || ec == FAIL_TRANSIENT) {
                    out.retryTransaction();
                }

                throw operationFailed(out.build());
            }

            return createStagedInsert(operationId, collection, id, content, flags, pspan, Optional.empty(), expiry);
        }
    }

    private Boolean supportsReplaceBodyWithXattr(String bucketName) {
        BucketConfig bc = BucketConfigUtil.waitForBucketConfig(core, bucketName, Duration.of(expiryRemainingMillis(), ChronoUnit.MILLIS)).block();
        return bc.bucketCapabilities().contains(SUBDOC_REVIVE_DOCUMENT);
    }

    /*
     * Stage an insert on a document, putting the staged mutation into the doc's xattrs.  The document is created
     * with an empty body.
     */
    private CoreTransactionGetResult createStagedInsert(String operationId,
                                                        CollectionIdentifier collection,
                                                        String id,
                                                        byte[] content,
                                                        int flagsOfContentToStage,
                                                        SpanWrapper pspan,
                                                        Optional<Long> cas,
                                                        @Nullable CoreExpiry expiry) {
        assertNotLocked("createStagedInsert");
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), collection, id, TracingIdentifiers.TRANSACTION_OP_INSERT_STAGE, pspan);

        try {
            boolean isBinary = CodecFlags.extractCommonFormatFlags(flagsOfContentToStage) == CodecFlags.CommonFlags.BINARY.ordinal();

            byte[] txn = createDocumentMetadata(OperationTypes.INSERT, operationId, Optional.empty(), flagsOfContentToStage, expiry);

            List<SubdocMutateRequest.Command> specList = new ArrayList<>(3);
            specList.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn", txn, true, true, false, specList.size()));
            specList.add(isBinary ? new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn.op.bin", content, false, true, false, true, specList.size())
                    : new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn.op.stgd", content, false, true, false, specList.size()));
            specList.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, "txn.op.crc32", serialize("${Mutation.value_crc32c}"), false, true, true, specList.size()));

            LOGGER.info(attemptId, "about to insert staged doc {} as shadow document, cas={}, operationId={}, expiry={}",
                    DebugUtil.docId(collection, id), cas, operationId, expiry);
            errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_CREATE_STAGED_INSERT, Optional.of(id));

            hooks.beforeStagedInsert.accept(this, id); // testing hook

            SubdocMutateResponse response = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(), !cas.isPresent(),
                    false, false, true, true, false, cas.orElse(0L), flagsOfContentToStage, durabilityLevel(), OptionsUtil.createClientContext("createStagedInsert"), span, null,
                    specList);

            hooks.afterStagedInsertComplete.accept(this, id); // testing hook

            long elapsed = span.elapsedMicros();
            addUnits(response.flexibleExtras());
            LOGGER.info(attemptId, "inserted doc {}{} got cas {}, in {}us",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(response.flexibleExtras()), response.cas(), elapsed);

            CoreTransactionGetResult out = CoreTransactionGetResult.createFromInsert(collection,
                    id,
                    content,
                    flagsOfContentToStage,
                    transactionId(),
                    attemptId,
                    atrId.get(),
                    atrCollection.get().bucket(),
                    atrCollection.get().scope().get(),
                    atrCollection.get().collection().get(),
                    response.cas(),
                    Optional.ofNullable(expiry));

            boolean supports = supportsReplaceBodyWithXattr(collection.bucket());

            addStagedMutation(new StagedMutation(operationId, out.id(), out.collection(), out.cas(),
                    out.documentMetadata(), Optional.empty(), flagsOfContentToStage, supports ? null : content, flagsOfContentToStage, StagedMutationType.INSERT, expiry));
            return out;

        } catch (Exception err) {
            span.recordException(err);
            MeteringUnits units = addUnits(MeteringUnits.from(err));
            LOGGER.info(attemptId, "got err while staging insert of {}{}: {}",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(units), dbg(err));

            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder out = createError().cause(err);

            if (err instanceof FeatureNotAvailableException) {
                throw operationFailed(out.build());
            }
            else {
                if (expiryOvertimeMode) {
                    mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_CREATE_STAGED_INSERT, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                } else if (ec == FAIL_EXPIRY) {
                    throw setExpiryOvertimeModeAndFail(err, CoreTransactionAttemptContextHooks.HOOK_CREATE_STAGED_INSERT);
                } else if (ec == FAIL_AMBIGUOUS) {
                    sleep(DEFAULT_DELAY_RETRYING_OPERATION);
                    return createStagedInsert(operationId, collection, id, content, flagsOfContentToStage, span, cas, expiry);
                } else if (ec == FAIL_TRANSIENT) {
                    throw operationFailed(out.retryTransaction().build());
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(out.doNotRollbackAttempt().build());
                } else if (ec == FAIL_DOC_ALREADY_EXISTS
                        || ec == FAIL_CAS_MISMATCH) {
                    return handleDocExistsDuringStagedInsert(operationId, collection, id, content, flagsOfContentToStage, span, expiry);
                } else {
                    throw operationFailed(out.build());
                }
            }

            throw operationFailed(out.build());
        } finally {
            span.finish();
        }
    }


    /**
     * Removes the specified <code>doc</code>, using the document's last
     * {@link CoreTransactionGetResult#cas()}.
     * <p>
     *
     * @param doc - the doc to be removed
     */
    public void remove(CoreTransactionGetResult doc, SpanWrapper pspan) {
        SpanWrapperUtil.addOperationAttribute(tracer(), pspan, TracingIdentifiers.TRANSACTION_OP_REMOVE);
        doKVOperation("remove " + DebugUtil.docId(doc), pspan, CoreTransactionAttemptContextHooks.HOOK_REMOVE, doc.collection(), doc.id(),
                (operationId, span) -> {
                    removeInternalLocked(operationId, doc, span);
                    return null;
                });
    }

    public Mono<Void> removeReactive(CoreTransactionGetResult doc, SpanWrapper pspan) {
        return Mono.fromRunnable(() -> remove(doc, pspan)).then()
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    private void removeInternalLocked(String operationId, CoreTransactionGetResult doc, SpanWrapper span) {
        LOGGER.info(attemptId, "remove doc {}, operationId={}", DebugUtil.docId(doc), operationId);

        if (queryModeLocked()) {
            removeWithQueryLocked(doc, span);
        } else {
            removeWithKVLocked(operationId, doc, span);
        }
    }

    private void removeWithKVLocked(String operationId, CoreTransactionGetResult doc, SpanWrapper span) {
        boolean mayNeedToWriteATR = state == AttemptState.NOT_STARTED;
        Optional<StagedMutation> existing = findStagedMutationLocked(doc);

        hooks.beforeUnlockRemove.accept(this, doc.id()); // testing hook
        unlock("standard");

        if (existing.isPresent()) {
            StagedMutation op = existing.get();
            LOGGER.info(attemptId, "found previous write of {} as {} on remove", DebugUtil.docId(doc), op.type);

            if (op.type == StagedMutationType.REMOVE) {
                throw operationFailed(createError()
                        .cause(new DocumentNotFoundException(null))
                        .build());
            } else if (op.type == StagedMutationType.INSERT) {
                removeStagedInsert(doc, span);
                return;
            }
        }

        checkAndHandleBlockingTxn(doc, span, ForwardCompatibilityStage.WRITE_WRITE_CONFLICT_REMOVING, existing);

        initATRIfNeeded(mayNeedToWriteATR, doc.collection(), doc.id(), span);

        createStagedRemove(operationId, doc, doc.cas(), span, doc.links().isDeleted());
    }

    private void checkAndHandleBlockingTxn(CoreTransactionGetResult doc,
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
                            LOGGER.info(attemptId, "concurrent op race detected on doc {}: have read a document before a concurrent op wrote its stagedMutation", DebugUtil.docId(doc));

                            throw operationFailed(createError()
                                    .cause(new ConcurrentOperationsDetectedOnSameDocumentException())
                                    .build());
                        }
                    } else {
                        LOGGER.info(attemptId, "concurrent op race detected on doc {}: can see the KV result of another op, but stagedMutation not yet written", DebugUtil.docId(doc));

                        throw operationFailed(createError()
                                .cause(new ConcurrentOperationsDetectedOnSameDocumentException())
                                .build());
                    }
                }

                LOGGER.info(attemptId, "doc {} has been written by a different attempt in transaction, ok to continue",
                        DebugUtil.docId(doc));
            } else {
                if (doc.links().atrId().isPresent() && doc.links().atrBucketName().isPresent()) {
                    LOGGER.info(attemptId, "doc {} is in another txn {}, checking ATR "
                                    + "entry {}/{}/{} to see if blocked",
                            DebugUtil.docId(doc),
                            doc.links().stagedAttemptId().get(),
                            doc.links().atrBucketName().orElse(""),
                            doc.links().atrCollectionName().orElse(""), doc.links().atrId().orElse(""));

                    forwardCompatibilityCheck(stage, doc.links().forwardCompatibility());

                    checkATREntryForBlockingDoc(doc, pspan);
                } else {
                    LOGGER.info(attemptId, "doc {} is in another txn {}, cannot " +
                                    "check ATR entry - probably a bug, so proceeding to overwrite",
                            DebugUtil.docId(doc),
                            doc.links().stagedAttemptId().get());
                }
            }
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
                    LOGGER.trace(attemptId(), "Skipping addition of cleanup request in state {}", state());
                    break;
                default:
                    LOGGER.trace(attemptId(), "Adding cleanup request for {}/{}",
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
     */
    public void commit() {
        commitInternal();
    }

    void implicitCommit(boolean singleQueryTransactionMode) {
        // May have done an explicit commit already, or the attempt may have failed.
        if (hasStateBit(TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED)) {
            return;
        } else if (singleQueryTransactionMode) {
            return;
        } else {
            LOGGER.info(attemptId(), "doing implicit commit");
            commitInternal();
        }
    }

    void commitInternal() {
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_COMMIT, attemptSpan);
        assertNotLocked("commit");
        try {
            waitForAllOpsThenDoUnderLock("commit", span,
                    () -> commitInternalLocked(span));
        } finally {
            span.finish();
        }
    }

    private void commitInternalLocked(SpanWrapper span) {
        assertLocked("commitInternal");

        TransactionOperationFailedException returnEarly = canPerformCommit("commit");
        if (returnEarly != null) {
            logger().info(attemptId, "commit raising {}", DebugUtil.dbg(returnEarly));
            throw returnEarly;
        }

        setStateBits("commit", TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED | TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED, 0);

        if (queryModeLocked()) {
            commitWithQueryLocked(span);
        } else {
            LOGGER.info(attemptId, "commit {}", this);

            // Commit hasn't started yet, so if we've expired, probably better to make a single attempt to rollback
            // than to commit.
            checkExpiryPreCommitAndSetExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_BEFORE_COMMIT, Optional.empty());

            if (!atrCollection.isPresent() || !atrId.isPresent()) {
                // no mutation, no need to commit
                LOGGER.info(attemptId, "calling commit on attempt that's got no mutations, skipping");

                // Leave state as NOTHING_WRITTEN (or NOT_STARTED, as we have to call it in the Java
                // implementation).  A successful read-only transaction ends in NOTHING_WRITTEN.
            } else {
                commitActualLocked(span);
            }
        }
    }

    private void commitActualLocked(SpanWrapper span) {
        String prefix = "attempts." + attemptId;

        LOGGER.info(attemptId, "commitActualLocked with {} staged mutations", stagedMutationsLocked.size());

        ArrayList<SubdocMutateRequest.Command> specs = new ArrayList<>();
        specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_STATUS, serialize(AttemptState.COMMITTED.name()), false, true, false, 0));
        specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_START_COMMIT, serialize("${Mutation.CAS}"), false, true, true, 1));
        specs.addAll(addDocsToBuilder(specs.size()));
        specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, prefix + "." + TransactionFields.ATR_FIELD_COMMIT_ONLY_IF_NOT_ABORTED, serialize(0), false, true, false, specs.size()));

        AtomicReference<Long> overallStartTime = new AtomicReference<>(0l);

        atrCommitLocked(specs, overallStartTime, span);

        commitDocsLocked(span);

        atrCompleteLocked(prefix, overallStartTime, span);

        LOGGER.info(attemptId, "overall commit completed");
    }

    private void commitWithQueryLocked(SpanWrapper span) {
        int sidx = queryStatementIdx.getAndIncrement();

        try {
            queryWrapperBlockingLocked(sidx,
                    queryContext.queryContext,
                    "COMMIT",
                    null,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_COMMIT,
                    false,
                    // existingErrorCheck false as it's already been done in commitInternalLocked
                    false, null, null, span, false, true);

            // MB-42619 means that query does not return the actual transaction state.  We assume it's
            // COMPLETED but this isn't correct in some cases, e.g. a read-only transaction will leave
            // it NOT_STARTED.
            setStateLocked(AttemptState.COMPLETED);

        } catch (Exception err) {
            ErrorClass ec = classify(err);

            if (ec == FAIL_EXPIRY) {
                TransactionOperationFailedException e = operationFailed(createError()
                        .cause(err)
                        .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                        .doNotRollbackAttempt()
                        .build());
                throw e;
            } else if (ec == TRANSACTION_OPERATION_FAILED) {
                throw err;
            }

            TransactionOperationFailedException e = operationFailed(createError()
                    .cause(err)
                    .doNotRollbackAttempt()
                    .build());
            throw e;
        }
    }

    // The timing of this call is important.
    // Should be done before doOnNext, which tests often make throw an exception.
    // In fact, needs to be done without relying on any onNext signal.  What if the operation times out instead.
    private void checkExpiryDuringCommitOrRollbackLocked(String stage, Optional<String> id) {
        assertLockedByAnyThread("checkExpiryDuringCommitOrRollbackLocked in stage " + stage);
        if (!expiryOvertimeMode) {
            if (hasExpiredClientSide(stage, id)) {
                LOGGER.info(attemptId, "has expired in stage {}, entering expiry-overtime mode (one attempt to complete)",
                        stage);
                expiryOvertimeMode = true;
            }
        } else {
            LOGGER.info(attemptId, "ignoring expiry in stage {}, as in expiry-overtime mode", stage);
        }
    }

    private void atrCompleteLocked(String prefix, AtomicReference<Long> overallStartTime, SpanWrapper pspan) {
        assertLocked("atrComplete");
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_COMPLETE, pspan);

        LOGGER.info(attemptId, "about to remove ATR entry {}", getAtrDebug(atrCollection, atrId));

        try {
            if (!expiryOvertimeMode && hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_ATR_COMPLETE, Optional.empty())) {
                String msg = "has expired in stage atrComplete, but transaction has successfully completed so returning success";
                LOGGER.info(attemptId, msg);

                throw new AttemptExpiredException(msg);
            }

            hooks.beforeAtrComplete.accept(this); // testing hook

            SubdocMutateResponse response = TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                    false, false, false, false, false, 0, BINARY_COMMON_FLAGS, durabilityLevel(), OptionsUtil.createClientContext("atrComplete"), span,
                    Arrays.asList(
                            new SubdocMutateRequest.Command(SubdocCommandType.DELETE, prefix, null, false, true, false, 0)
                    ));

            hooks.afterAtrComplete.accept(this);  // Testing hook

            setStateLocked(AttemptState.COMPLETED);
            addUnits(response.flexibleExtras());
            long now = System.nanoTime();
            long elapsed = span.elapsedMicros();
            LOGGER.info(attemptId, "removed ATR {} in {}us{}, overall commit completed in {}us",
                    getAtrDebug(atrCollection, atrId), elapsed, DebugUtil.dbg(response.flexibleExtras()), TimeUnit.NANOSECONDS.toMicros(now - overallStartTime.get()));


        } catch (Exception err) {
            span.recordException(err);
            ErrorClass ec = classify(err);
            MeteringUnits units = addUnits(MeteringUnits.from(err));

            LOGGER.info(attemptId, "error '{}' ec={} while removing ATR {}{}", err, ec,
                    getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units));

            if (ec == FAIL_HARD) {
                throw operationFailed(createError()
                        .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT)
                        .doNotRollbackAttempt()
                        .build());
            } else {
                LOGGER.info(attemptId, "ignoring error during transaction tidyup, regarding as success");
            }
        } finally {
            span.finish();
        }
    }

    // [EXP-ROLLBACK] [EXP-COMMIT-OVERTIME]: have been trying to make one attempt to rollback (or finish commit) after
    // expiry, but something's failed.  Give up and raise AttemptExpired.  Do not attempt any further rollback.
    private <T> T mapErrorInOvertimeToExpired(boolean updateAppState, String stage, Throwable err, FinalErrorToRaise toRaise) {
        LOGGER.info(attemptId, "in expiry-overtime mode so changing error '{}' to raise {} in stage '{}'; no rollback will be tried",
                err, toRaise, stage);

        if (!expiryOvertimeMode) {
            LOGGER.warn(attemptId, "not in expiry-overtime mode handling error '{}' in stage {}, possibly a bug",
                    err, stage);
        }

        throw operationFailed(updateAppState, createError()
                .doNotRollbackAttempt()
                .raiseException(toRaise)
                .cause(new AttemptExpiredException(err)).build());
    }

    private void removeDocLocked(SpanWrapper span, CollectionIdentifier collection, String id, boolean ambiguityResolutionMode) {
        try {
            assertLockedByAnyThread("removeDoc");

            LOGGER.info(attemptId, "about to remove doc {}, ambiguityResolutionMode={}",
                    DebugUtil.docId(collection, id), ambiguityResolutionMode);

            // [EXP-COMMIT-OVERTIME]
            checkExpiryDuringCommitOrRollbackLocked(CoreTransactionAttemptContextHooks.HOOK_REMOVE_DOC, Optional.of(id));

            hooks.beforeDocRemoved.accept(this, id); // testing hook

            // A normal (non-subdoc) remove will also remove a doc's user xattrs too
            RemoveResponse mutationResult = TransactionKVHandler.remove(core,
                    collection,
                    id,
                    kvTimeoutNonMutating(),
                    0,
                    durabilityLevel(),
                    OptionsUtil.createClientContext("commitRemove"),
                    span);

            // Testing hook (goes before onErrorResume)
            hooks.afterDocRemovedPreRetry.accept(this, id);

            addUnits(mutationResult.flexibleExtras());
            LOGGER.info(attemptId, "commit - removed doc {}{}, mt = {}",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(mutationResult.flexibleExtras()), mutationResult.mutationToken());

        } catch (Exception err) {
            span.recordException(err);
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder e = createError()
                    .cause(err)
                    .doNotRollbackAttempt()
                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
            MeteringUnits units = addUnits(MeteringUnits.from(err));

            LOGGER.info("got error while removing doc {}{} in {}us: {}",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

            if (expiryOvertimeMode) {
                mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_REMOVE_DOC, err, FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
            } else if (ec == FAIL_AMBIGUOUS) {
                sleep(DEFAULT_DELAY_RETRYING_OPERATION);
                removeDocLocked(span, collection, id, true);
            } else if (ec == FAIL_DOC_NOT_FOUND) {
                throw operationFailed(e.build());
            } else if (ec == FAIL_HARD) {
                throw operationFailed(e.build());
            } else {
                throw operationFailed(e.build());
            }
        }

        // Testing hook
        hooks.afterDocRemovedPostRetry.accept(this, id);
    }

    private void commitDocsLocked(SpanWrapper span) {
        assertLocked("commitDocs");
        long start = System.nanoTime();

        CountDownLatch latch = new CountDownLatch(stagedMutationsLocked.size());
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        ExecutorService executor = core.context().environment().transactionsSchedulers().blockingExecutor();
        // Keep parallelism bounded.
        Semaphore semaphore = new Semaphore(UNSTAGING_PARALLELISM);

        for (StagedMutation mutation : stagedMutationsLocked) {
            executor.submit(() -> {
                try {
                    semaphore.acquire();
                    try {
                        // Note we intentionally don't short-circuit other operations on failure.  Commit has still occurred and we will try and accomplish as much of it possible now, leaving as little for
                        // cleanup as possible.
                        commitDocWrapperLocked(span, mutation);
                    } finally {
                        semaphore.release();
                    }
                } catch (RuntimeException err) {
                    failure.compareAndSet(null, err);
                } catch (Throwable err) {
                    failure.compareAndSet(null, new RuntimeException(err));
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw interrupted("commitDocsLocked");
        }

        if (failure.get() != null) {
            logger().warn(attemptId, "commit - error occurred {}", DebugUtil.dbg(failure.get()));
            span.recordException(failure.get());
            throw failure.get();
        }

        long elapsed = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        LOGGER.info(attemptId, "commit - all {} docs committed in {}us",
                stagedMutationsLocked.size(), elapsed);

        hooks.afterDocsCommitted.accept(this);
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

    private void commitDocWrapperLocked(SpanWrapper pspan,
                                        StagedMutation staged) {
        if (staged.type == StagedMutationType.REMOVE) {
            removeDocLocked(pspan, staged.collection, staged.id, false);
        } else {
            commitDocLocked(pspan, staged, staged.cas, staged.type == StagedMutationType.INSERT, false);
        }
    }

    private void commitDocLocked(SpanWrapper span,
                                 StagedMutation staged,
                                 long cas,
                                 boolean insertMode,
                                 boolean ambiguityResolutionMode) {
        String id = staged.id;
        CollectionIdentifier collection = staged.collection;

        try {
            assertLockedByAnyThread("commitDoc");

            LOGGER.info(attemptId, "commit - committing doc {}, cas={}, insertMode={}, ambiguity-resolution={} supportsReplaceBodyWithXattr={} binary={} userFlags={}",
                    DebugUtil.docId(collection, id), cas, insertMode, ambiguityResolutionMode, staged.supportsReplaceBodyWithXattr(), staged.isStagedBinary(), staged.stagedUserFlags);

            checkExpiryDuringCommitOrRollbackLocked(CoreTransactionAttemptContextHooks.HOOK_COMMIT_DOC, Optional.of(id));

            hooks.beforeDocCommitted.accept(this, id); // testing hook

            if (insertMode) {
                if (staged.supportsReplaceBodyWithXattr()) {
                    SubdocMutateResponse v = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(), false, false, true, true, false, false,
                            cas, staged.stagedUserFlags, durabilityLevel(), OptionsUtil.createClientContext("commitDocInsert"), span, staged.expiry, Arrays.asList(
                                    staged.isStagedBinary()
                                            ? new SubdocMutateRequest.Command(SubdocCommandType.REPLACE_BODY_WITH_XATTR, TransactionFields.STAGED_DATA_BINARY, null, false, true, false, true, 0)
                                            : new SubdocMutateRequest.Command(SubdocCommandType.REPLACE_BODY_WITH_XATTR, TransactionFields.STAGED_DATA_JSON, null, false, true, false, false, 0),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, null, false, true, false, 1)
                            ));
                    addUnits(v.flexibleExtras());
                    LOGGER.info(attemptId, "commit - committed doc insert swap body {} got cas {}{}", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                }
                else {
                    InsertResponse v = TransactionKVHandler.insert(core, collection, id, staged.content, staged.stagedUserFlags, kvTimeoutMutating(),
                            durabilityLevel(), OptionsUtil.createClientContext("commitDocInsert"), span, staged.expiry);
                    addUnits(v.flexibleExtras());
                    LOGGER.info(attemptId, "commit - committed doc insert {} got cas {}{}", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                }
            } else {
                if (staged.supportsReplaceBodyWithXattr()) {
                    SubdocMutateResponse v = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(), false, false, false, false, false, true,
                            cas, staged.stagedUserFlags, durabilityLevel(), OptionsUtil.createClientContext("commitDoc"), span, staged.expiry, Arrays.asList(
                                    staged.isStagedBinary()
                                            ? new SubdocMutateRequest.Command(SubdocCommandType.REPLACE_BODY_WITH_XATTR, TransactionFields.STAGED_DATA_BINARY, null, false, true, false, true, 0)
                                            : new SubdocMutateRequest.Command(SubdocCommandType.REPLACE_BODY_WITH_XATTR, TransactionFields.STAGED_DATA_JSON, null, false, true, false, false, 0),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, null, false, true, false, 1)
                            ));
                    addUnits(v.flexibleExtras());
                    LOGGER.info(attemptId, "commit - committed doc replace swap body {} got cas {}{}", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                }
                else {
                    SubdocMutateResponse v = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                            false, false, false, false, false, true, cas, staged.stagedUserFlags, durabilityLevel(),
                            OptionsUtil.createClientContext("commitDoc"), span, staged.expiry,
                            Arrays.asList(
                                    // Upsert this field to better handle illegal doc mutation.  E.g. run shadowDocSameTxnKVInsert without this,
                                    // fails at this point as path has been removed.  Could also handle with a spec change to handle that.
                                    new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, serialize(null), false, true, false, 0),
                                    new SubdocMutateRequest.Command(SubdocCommandType.DELETE, TransactionFields.TRANSACTION_INTERFACE_PREFIX_ONLY, null, false, true, false, 1),
                                    new SubdocMutateRequest.Command(SubdocCommandType.SET_DOC, "", staged.content, false, false, false, 2)
                            ));
                    addUnits(v.flexibleExtras());
                    LOGGER.info(attemptId, "commit - committed doc replace {} got cas {}{}", DebugUtil.docId(collection, id), v.cas(), DebugUtil.dbg(v.flexibleExtras()));
                }
            }

            hooks.afterDocCommittedBeforeSavingCAS.accept(this, id);

            // Do checkExpiryDuringCommitOrRollback before doOnNext (which tests often make throw)
            hooks.afterDocCommitted.accept(this, id);

        } catch (Exception err) {
            span.recordException(err);
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder e = createError().cause(err)
                    .doNotRollbackAttempt()
                    .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
            MeteringUnits units = addUnits(MeteringUnits.from(err));

            LOGGER.info(attemptId, "error while committing doc {}{} in {}us: {}",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

            if (expiryOvertimeMode) {
                mapErrorInOvertimeToExpired(true, CoreTransactionAttemptContextHooks.HOOK_COMMIT_DOC, err, FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT);
            } else if (ec == FAIL_AMBIGUOUS) {
                // TXNJ-136: The operation may or may not have succeeded.  Retry in ambiguity-resolution mode.
                LOGGER.warn(attemptId, "{} while committing doc {}: as op is ambiguously successful, retrying " +
                        "op in ambiguity-resolution mode", DebugUtil.dbg(err), DebugUtil.docId(collection, id));

                commitDocLocked(span, staged, cas, insertMode, true);
            } else if (ec == FAIL_CAS_MISMATCH) {
                handleDocChangedDuringCommit(span, staged, insertMode);
            } else if (ec == FAIL_DOC_NOT_FOUND) {
                handleDocMissingDuringCommit(span, staged);
            } else if (ec == FAIL_DOC_ALREADY_EXISTS) { // includes CannotReviveAliveDocumentException
                if (ambiguityResolutionMode) {
                    throw e.build();
                } else {
                    String msg = msgDocChangedUnexpectedly(collection, id);
                    LOGGER.warn(attemptId, msg);
                    LOGGER.eventBus().publish(new IllegalDocumentStateEvent(Event.Severity.WARN, msg, id));

                    if (staged.supportsReplaceBodyWithXattr()) {
                        // There's nothing can be done, the document data is lost
                        return;
                    }
                    else {
                        // Redo as a replace, which will of course fail on CAS.
                        sleep(DEFAULT_DELAY_RETRYING_OPERATION);
                        commitDocLocked(span, staged, cas, false, ambiguityResolutionMode);
                    }
                }
            } else if (ec == FAIL_HARD) {
                throw operationFailed(e.build());
            } else {
                throw operationFailed(e.build());
            }
        }
    }

    private void addUnits(CoreKvResponseMetadata meta) {
        meteringUnitsBuilder.add(meta);
    }

    private void addUnits(@Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
        meteringUnitsBuilder.add(flexibleExtras);
    }

    private MeteringUnits addUnits(@Nullable MeteringUnits units) {
        meteringUnitsBuilder.add(units);
        return units;
    }

    private void handleDocMissingDuringCommit(SpanWrapper pspan,
                                              StagedMutation staged) {
        String msg = msgDocRemovedUnexpectedly(staged.collection, staged.id, true);
        LOGGER.warn(attemptId, msg);
        LOGGER.eventBus().publish(new IllegalDocumentStateEvent(Event.Severity.WARN, msg, staged.id));
        sleep(DEFAULT_DELAY_RETRYING_OPERATION);
        commitDocLocked(pspan, staged, 0, true, false);
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
     * There are many times where this is unavailable, hence it being optional.  e.g. if CAS conflict
     * between get and replace, or if the get was with a version of query that doesn't return CRC, or if the
     * first op was an insert (no body to create a CRC32 from).
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

        LOGGER.info(attemptId, "handling doc changed during {} fetched doc {}, unclearIfBodyHasChanged = {}, bodyHasChanged = {}, inDifferentTransaction = {}, notInTransaction = {}, inSameTransaction = {} links = {} metadata = {} cas = {} crc32Then = {}, crc32Now = {}",
                stage, DebugUtil.docId(gr.collection(), gr.id()), unclearIfBodyHasChanged, bodyHasChanged,
                inDifferentTransaction, notInTransaction, out.inSameTransaction(), gr.links(), gr.documentMetadata(), gr.cas(), crc32Then, crc32Now);

        return out;
    }

    // No need to pass ambiguityResolutionMode - we are about to resolve the ambiguity
    private void handleDocChangedDuringCommit(SpanWrapper span,
                                              StagedMutation staged,
                                              boolean insertMode) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();

        do {
            retry.shouldRetry(false);

            String id = staged.id;
            CollectionIdentifier collection = staged.collection;
            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();

            Optional<CoreTransactionGetResult> doc = Optional.empty();
            try {
                LOGGER.info(attemptId, "commit - handling doc changed {}, insertMode={}",
                        DebugUtil.docId(collection, id), insertMode);
                if (hasExpiredClientSide(HOOK_COMMIT_DOC_CHANGED, Optional.of(staged.id))) {
                    LOGGER.info(attemptId, "has expired in stage {}", HOOK_COMMIT_DOC_CHANGED);
                    throw operationFailed(createError()
                            .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT)
                            .doNotRollbackAttempt()
                            .cause(new AttemptExpiredException("Attempt has expired in stage " + HOOK_COMMIT_DOC_CHANGED))
                            .build());
                }

                hooks.beforeDocChangedDuringCommit.accept(this, id); // testing hook
                doc = DocumentGetter.get(core, LOGGER, staged.collection, config, staged.id, attemptId, true, span, Optional.empty(), units, overall.supported(), false);
            } catch (Exception err) {
                ErrorClass ec = classify(err);
                MeteringUnits built = addUnits(units.build());
                span.recordException(err);
                LOGGER.info(attemptId, "commit - handling doc changed {}{}, got error {}",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(built), dbg(err));
                if (ec == TRANSACTION_OPERATION_FAILED) {
                    throw err;
                } else if (ec == FAIL_TRANSIENT) {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                    continue;
                } else {
                    throw operationFailed(createError()
                            .doNotRollbackAttempt()
                            .raiseException(FinalErrorToRaise.TRANSACTION_FAILED_POST_COMMIT)
                            .cause(err)
                            .build());
                }
                // FAIL_DOC_NOT_FOUND handled elsewhere in this function
            }

            addUnits(units.build());
            if (doc.isPresent()) {
                CoreTransactionGetResult gr = doc.get();
                DocChanged dc = getDocChanged(gr,
                        "commit",
                        // This will usually be present, from staging. But could have got an ERR_AMBIG while committing the doc, and it actually succeeded.  In which case this will be empty.
                        gr.links().crc32OfStaging(),
                        gr.crc32OfGet().get()        // this must be present as just fetched with $document
                );
                forwardCompatibilityCheck(ForwardCompatibilityStage.CAS_MISMATCH_DURING_COMMIT, gr.links().forwardCompatibility());


                if (dc.inDifferentTransaction || dc.notInTransaction) {
                    return;
                } else {
                    // Doc is still in same attempt
                    if (dc.bodyHasChanged) {
                        String msg = msgDocChangedUnexpectedly(staged.collection, staged.id);
                        LOGGER.warn(attemptId, msg);
                        LOGGER.eventBus().publish(new IllegalDocumentStateEvent(Event.Severity.WARN, msg, staged.id));
                    }
                    // Retry committing the doc, with the new CAS
                    sleep(DEFAULT_DELAY_RETRYING_OPERATION);
                    commitDocLocked(span, staged, gr.cas(), insertMode, false);
                }
            } else {
                handleDocMissingDuringCommit(span, staged);
            }
        } while (retry.shouldRetry());
    }

    private <T> T handleDocChangedDuringStaging(SpanWrapper span,
                                                String id,
                                                CollectionIdentifier collection,
                                                Optional<String> crc32FromGet,
                                                Function<Long, T> callback) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();
        do {
            retry.shouldRetry(false);
            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();
            LOGGER.info(attemptId, "handling doc changed during staging {}",
                    DebugUtil.docId(collection, id));

            try {
                throwIfExpired(id, HOOK_STAGING_DOC_CHANGED);
                hooks.beforeDocChangedDuringStaging.accept(this, id); // testing hook
                Optional<CoreTransactionGetResult> doc = DocumentGetter.get(core, LOGGER, collection, config, id, attemptId, true, span, Optional.empty(), units, overall.supported(), false);

                addUnits(units.build());
                if (doc.isPresent()) {
                    CoreTransactionGetResult gr = doc.get();
                    DocChanged dc = getDocChanged(gr,
                            "staging",
                            crc32FromGet,         // this may or may not be present - see getDocChanged for list
                            gr.crc32OfGet().get() // this must be present as doc just fetched
                    );
                    forwardCompatibilityCheck(ForwardCompatibilityStage.CAS_MISMATCH_DURING_STAGING, gr.links().forwardCompatibility());

                    if (dc.inDifferentTransaction) {
                        checkAndHandleBlockingTxn(gr, span, ForwardCompatibilityStage.CAS_MISMATCH_DURING_STAGING, Optional.empty());
                        retry.sleepForNextBackoffAndSetShouldRetry();
                        continue;
                    } else { // must be bodyHasChanged || notInTransaction || unclearIfBodyHasChanged
                        if (dc.bodyHasChanged || dc.unclearIfBodyHasChanged) {
                            throw operationFailed(createError()
                                    .retryTransaction()
                                    .build());
                        } else {
                            // Retry the operation, with the new CAS
                            sleep(DEFAULT_DELAY_RETRYING_OPERATION);
                            return callback.apply(gr.cas());
                        }
                    }
                } else {
                    throw operationFailed(createError()
                            .retryTransaction()
                            .cause(new DocumentNotFoundException(null))
                            .build());
                }
            } catch (Exception err) {

                MeteringUnits built = addUnits(units.build());
                ErrorClass ec = classify(err);
                LOGGER.info(attemptId, "handling doc changed during staging {}{}, got error {}",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(built), dbg(err));
                if (ec == TRANSACTION_OPERATION_FAILED) {
                    throw err;
                } else if (ec == FAIL_TRANSIENT) {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(createError()
                            .doNotRollbackAttempt()
                            .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                            .cause(err)
                            .build());
                } else {
                    throw operationFailed(createError()
                            .retryTransaction()
                            .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                            .cause(err)
                            .build());
                }
                // FAIL_DOC_NOT_FOUND handled elsewhere in this function
            }

        } while (retry.shouldRetry());

        throw operationFailed(createError().cause(new IllegalStateException("Internal bug, should not be able to reach here")).build());
    }

    private void throwIfExpired(String id, String stage) {
        if (hasExpiredClientSide(stage, Optional.of(id))) {
            LOGGER.info(attemptId, "has expired in stage {}", stage);
            throw operationFailed(createError()
                    .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                    .doNotRollbackAttempt()
                    .cause(new AttemptExpiredException("Attempt has expired in stage " + stage))
                    .build());
        }
    }

    private void throwIfExpired(String stage) {
        if (hasExpiredClientSide(stage, Optional.empty())) {
            LOGGER.info(attemptId, "has expired in stage {}", stage);
            throw operationFailed(createError()
                    .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                    .doNotRollbackAttempt()
                    .cause(new AttemptExpiredException("Attempt has expired in stage " + stage))
                    .build());
        }
    }

    private void handleDocChangedDuringRollback(SpanWrapper span,
                                                String id,
                                                CollectionIdentifier collection,
                                                Consumer<Long> callback) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();
        do {
            retry.shouldRetry(false);
            MeteringUnits.MeteringUnitsBuilder units = new MeteringUnits.MeteringUnitsBuilder();
            LOGGER.info(attemptId, "handling doc changed during rollback {}",
                    DebugUtil.docId(collection, id));
            Optional<CoreTransactionGetResult> doc;
            try {
                throwIfExpired(id, HOOK_ROLLBACK_DOC_CHANGED);
                hooks.beforeDocChangedDuringRollback.accept(this, id); // testing hook
                doc = DocumentGetter.get(core, LOGGER, collection, config, id, attemptId, true, span, Optional.empty(), units, overall.supported(), false);
            } catch (Exception err) {
                MeteringUnits built = addUnits(units.build());
                ErrorClass ec = classify(err);
                span.recordException(err);
                LOGGER.info(attemptId, "handling doc changed during rollback {}{}, got error {}",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(built), dbg(err));
                if (ec == TRANSACTION_OPERATION_FAILED) {
                    throw err;
                } else if (ec == FAIL_TRANSIENT) {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                    continue;
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(createError()
                            .doNotRollbackAttempt()
                            .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                            .cause(err)
                            .build());
                } else {
                    throw operationFailed(createError()
                            .doNotRollbackAttempt()
                            .raiseException(FinalErrorToRaise.TRANSACTION_FAILED)
                            .cause(err)
                            .build());
                }
                // FAIL_DOC_NOT_FOUND handled elsewhere in this function
            }

            addUnits(units.build());
            if (doc.isPresent()) {
                CoreTransactionGetResult gr = doc.get();
                DocChanged dc = getDocChanged(gr,
                        "rollback",
                        gr.links().crc32OfStaging(),   // this should be present from staging
                        gr.crc32OfGet().get()          // this must be present as doc just fetched
                );
                forwardCompatibilityCheck(ForwardCompatibilityStage.CAS_MISMATCH_DURING_ROLLBACK, gr.links().forwardCompatibility());

                if (dc.inDifferentTransaction || dc.notInTransaction) {
                    return;
                } else {
                    // In same attempt, body may or may not have changed
                    callback.accept(gr.cas());
                    return;
                }
            }
            retry.sleepForNextBackoffAndSetShouldRetry();
        } while (retry.shouldRetry());
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

    private void atrCommitAmbiguityResolutionLocked(AtomicReference<Long> overallStartTime,
                                                    SpanWrapper span) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();
        do {
            retry.shouldRetry(false);
            LOGGER.info(attemptId, "about to fetch status of ATR {} to resolve ambiguity, expiryOvertimeMode={}",
                    getAtrDebug(atrCollection, atrId), expiryOvertimeMode);
            overallStartTime.set(System.nanoTime());

            try {
                errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_COMMIT_AMBIGUITY_RESOLUTION, Optional.empty());

                hooks.beforeAtrCommitAmbiguityResolution.accept(this); // testing hook

                CoreSubdocGetResult result = TransactionKVHandler.lookupIn(core, atrCollection.get(), atrId.get(), kvTimeoutNonMutating(), false, OptionsUtil.createClientContext("atrCommitAmbiguityResolution"), span,
                        false,
                        Arrays.asList(
                                new SubdocGetRequest.Command(SubdocCommandType.GET, "attempts." + attemptId + "." + TransactionFields.ATR_FIELD_STATUS, true, 0)
                        ));

                String status = null;
                try {
                    status = Mapper.reader().readValue(result.field(0).value(), String.class);
                } catch (IOException e) {
                    LOGGER.info(attemptId, "failed to parse ATR {} status '{}'", getAtrDebug(atrCollection, atrId), new String(result.field(0).value()));
                    status = "UNKNOWN";
                }

                addUnits(result.meta());
                LOGGER.info(attemptId, "got status of ATR {}: '{}'", getAtrDebug(atrCollection, atrId), status);

                AttemptState state = AttemptState.convert(status);

                switch (state) {
                    case COMMITTED:
                        return;

                    case ABORTED:
                        throw operationFailed(createError()
                                .retryTransaction()
                                .build());

                    default:
                        throw operationFailed(createError()
                                .doNotRollbackAttempt()
                                .cause(new IllegalStateException("This transaction has been changed by another actor to be in unexpected state " + status))
                                .build());
                }
            } catch (Exception err) {
                ErrorClass ec = classify(err);
                TransactionOperationFailedException.Builder builder = createError().doNotRollbackAttempt().cause(err);
                MeteringUnits units = addUnits(MeteringUnits.from(err));
                span.recordException(err);

                if (err instanceof RetryAtrCommitException || ec == TRANSACTION_OPERATION_FAILED) {
                    throw err;
                }

                LOGGER.info(attemptId, "error while resolving ATR {} ambiguity{} in {}us: {}",
                        getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                if (ec == FAIL_EXPIRY) {
                    throw operationFailed(createError()
                            .doNotRollbackAttempt()
                            .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                            .cause(new AttemptExpiredException(err)).build());
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(builder
                            .doNotRollbackAttempt()
                            .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                            .build());
                } else if (ec == FAIL_TRANSIENT || ec == FAIL_OTHER) {
                    retry.sleepForFixedIntervalAndSetShouldRetry(DEFAULT_DELAY_RETRYING_OPERATION);
                } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                    throw operationFailed(createError()
                            .doNotRollbackAttempt()
                            .cause(new ActiveTransactionRecordEntryNotFoundException(atrId.get(), attemptId))
                            .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                            .build());
                } else if (ec == FAIL_DOC_NOT_FOUND) {
                    throw operationFailed(createError()
                            .doNotRollbackAttempt()
                            .cause(new ActiveTransactionRecordNotFoundException(atrId.get(), attemptId))
                            .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                            .build());
                } else {
                    throw operationFailed(builder.raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS).build());
                }
            }
        } while (retry.shouldRetry());
    }

    BlockingRetryHandler createRetryHandlerUntilExpiry() {
        return BlockingRetryHandler.builder(Duration.ofMillis(1), Duration.ofMillis(100))
                .jitter(0.5)
                .build();
    }

    private void atrCommitLocked(List<SubdocMutateRequest.Command> specs,
                                 AtomicReference<Long> overallStartTime,
                                 SpanWrapper pspan) {

        assertLocked("atrCommit");
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TRANSACTION_OP_ATR_COMMIT, pspan);

        try {
            AtomicBoolean ambiguityResolutionMode = new AtomicBoolean(false);
            BlockingRetryHandler retry = createRetryHandlerUntilExpiry();
            do {
                retry.shouldRetry(false);

                try {
                    LOGGER.info(attemptId, "about to set ATR {} to Committed, expiryOvertimeMode={}, ambiguityResolutionMode={}",
                            getAtrDebug(atrCollection, atrId), expiryOvertimeMode, ambiguityResolutionMode);
                    overallStartTime.set(System.nanoTime());

                    errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_COMMIT, Optional.empty());

                    hooks.beforeAtrCommit.accept(this); // testing hook

                    SubdocMutateResponse response = TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                            false, false, false, false, false, 0, BINARY_COMMON_FLAGS, durabilityLevel(), OptionsUtil.createClientContext("atrCommit"), span, specs);

                    // Testing hook
                    hooks.afterAtrCommit.accept(this);

                    setStateLocked(AttemptState.COMMITTED);
                    addUnits(response.flexibleExtras());
                    LOGGER.info(attemptId, "set ATR {} to Committed{} in {}us", getAtrDebug(atrCollection, atrId),
                            DebugUtil.dbg(response.flexibleExtras()), span.elapsedMicros());

                } catch (Exception err) {
                    ErrorClass ec = classify(err);
                    TransactionOperationFailedException.Builder builder = createError().cause(err);
                    MeteringUnits units = addUnits(MeteringUnits.from(err));
                    span.recordException(err);

                    LOGGER.info(attemptId, "error while setting ATR {} to Committed{} in {}us: {}",
                            getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                    if (ec == FAIL_EXPIRY) {
                        FinalErrorToRaise toRaise = ambiguityResolutionMode.get() ? FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS : FinalErrorToRaise.TRANSACTION_EXPIRED;
                        throw operationFailed(createError()
                                .doNotRollbackAttempt()
                                .raiseException(toRaise)
                                .cause(new AttemptExpiredException(err)).build());
                    } else if (ec == FAIL_AMBIGUOUS) {
                        ambiguityResolutionMode.set(true);
                        retry.sleepForNextBackoffAndSetShouldRetry();
                    } else if (ec == FAIL_HARD) {
                        if (ambiguityResolutionMode.get()) {
                            throw operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                    .cause(err).build());
                        } else {
                            throw operationFailed(builder.doNotRollbackAttempt().build());
                        }
                    } else if (ec == FAIL_TRANSIENT) {
                        if (ambiguityResolutionMode.get()) {
                            retry.sleepForNextBackoffAndSetShouldRetry();
                        } else {
                            throw operationFailed(builder.retryTransaction().build());
                        }
                    } else if (ec == FAIL_PATH_ALREADY_EXISTS) {
                        try {
                            atrCommitAmbiguityResolutionLocked(overallStartTime, pspan);
                        } catch (Exception e) {
                            if (e instanceof RetryAtrCommitException) {
                                ambiguityResolutionMode.set(false);
                                retry.sleepForNextBackoffAndSetShouldRetry();
                            } else {
                                throw e;
                            }
                        }
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
                            throw operationFailed(createError()
                                    .doNotRollbackAttempt()
                                    .raiseException(FinalErrorToRaise.TRANSACTION_COMMIT_AMBIGUOUS)
                                    .cause(cause).build());
                        } else {
                            throw operationFailed(builder.cause(cause)
                                    .rollbackAttempt(rollback)
                                    .build());
                        }
                    }
                }
            } while (retry.shouldRetry());
        } finally {
            span.finish();
        }
    }

    private void setExpiryOvertimeMode(String stage) {
        LOGGER.info(attemptId, "moving to expiry-overtime-mode in stage {}", stage);
        expiryOvertimeMode = true;
    }

    private RuntimeException setExpiryOvertimeModeAndFail(Throwable err, String stage) {
        LOGGER.info(attemptId, "moving to expiry-overtime-mode in stage {}, and raising error", stage);
        expiryOvertimeMode = true;

        // This error should cause a rollback and then a TransactionExpired to be raised to app
        // But if any problem happens, then mapErrorInOvertimeToExpire logic should capture it, and also cause a
        // TransactionExpired to be raised to app
        return operationFailed(createError()
                .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                .cause(new AttemptExpiredException(err)).build());
    }

    /**
     * Rolls back the transaction.  All staged replaces, inserts and removals will be removed.  The transaction will not
     * be retried, so this will be the final attempt.
     */
    public void rollback() {
        waitForAllOpsThenDoUnderLock("app-rollback", attemptSpan,
                () -> rollbackInternalLocked(true));
    }

    void rollbackAuto() {
        waitForAllOpsThenDoUnderLock("auto-rollback", attemptSpan,
                () -> rollbackInternalLocked(false));
    }

    /**
     * Rolls back the transaction.
     *
     * @param isAppRollback whether this is an app-rollback or auto-rollback
     */
    private void rollbackInternalLocked(boolean isAppRollback) {
        SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), null, null, TracingIdentifiers.TRANSACTION_OP_ROLLBACK, attemptSpan);

        try {
            TransactionOperationFailedException returnEarly = canPerformRollback("rollbackInternal", isAppRollback);
            if (returnEarly != null) {
                logger().info(attemptId, "rollback raising {}", DebugUtil.dbg(returnEarly));
                throw returnEarly;
            }

            int sb = TRANSACTION_STATE_BIT_COMMIT_NOT_ALLOWED | TRANSACTION_STATE_BIT_APP_ROLLBACK_NOT_ALLOWED;
            setStateBits("rollback-" + (isAppRollback ? "app" : "auto"), sb, 0);

            // In queryMode we always ROLLBACK, as there is possibly delta table state to cleanup, and there may be an
            // ATR - we don't know
            if (state == AttemptState.NOT_STARTED && !queryModeUnlocked()) {
                LOGGER.info(attemptId, "told to auto-rollback but in NOT_STARTED state, so nothing to do - skipping rollback");
                return;
            }

            if (queryModeLocked()) {
                rollbackQueryLocked(isAppRollback, span);
            } else {
                rollbackWithKVLocked(isAppRollback, span);
            }
        } finally {
            span.finish();
        }
    }

    private void rollbackWithKVLocked(boolean isAppRollback, SpanWrapper span) {
        assertLocked("rollbackWithKV");
        LOGGER.info(attemptId, "rollback {} expiryOvertimeMode={} isAppRollback={}",
                this, expiryOvertimeMode, isAppRollback);

        // [EXP-ROLLBACK] - ignore expiries on singleAttempt, assumes we're doing this as already expired
        if (!expiryOvertimeMode && hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_ROLLBACK, Optional.empty())) {
            LOGGER.info(attemptId, "has expired before rollback, entering expiry-overtime mode");
            // Make one attempt to complete the rollback anyway
            expiryOvertimeMode = true;
        }

        if (!atrCollection.isPresent() || !atrId.isPresent()) {
            // no mutation, no need to rollback
            LOGGER.info(attemptId, "Calling rollback when it's had no mutations, so nothing to do");
        } else {
            rollbackWithKVActual(isAppRollback, span);
        }
    }

    private void rollbackWithKVActual(boolean isAppRollback, SpanWrapper span) {
        String prefix = "attempts." + attemptId;

        try {
            atrAbortLocked(prefix, span, isAppRollback, false);

            rollbackDocsLocked(isAppRollback, span);

            atrRollbackCompleteLocked(isAppRollback, prefix, span);

            // [RETRY-ERR-ROLLBACK]
        } catch (Exception err) {
            if (err instanceof ActiveTransactionRecordNotFoundException) {
                LOGGER.info(attemptId, "ActiveTransactionRecordNotFound indicates that nothing needs " +
                        "to be done for this rollback: treating as successful rollback");
            } else {
                throw err; // propagate
            }
        }
    }

    private void rollbackQueryLocked(boolean appRollback, SpanWrapper span) {
        assertLocked("rollbackQuery");

        int statementIdx = queryStatementIdx.getAndIncrement();

        try {
            queryWrapperBlockingLocked(statementIdx,
                    queryContext.queryContext,
                    "ROLLBACK",
                    null,
                    CoreTransactionAttemptContextHooks.HOOK_QUERY_ROLLBACK,
                    false, false, null, null, span, false, appRollback);

            setStateLocked(AttemptState.ROLLED_BACK);
        } catch (Exception err) {
            span.recordExceptionAndSetErrorStatus(err);

            if (err instanceof TransactionOperationFailedException) {
                throw err;
            }

            if (err instanceof AttemptNotFoundOnQueryException) {
                // Indicates that query has already rolled back this attempt.
                return;
            } else {
                TransactionOperationFailedException e = operationFailed(createError()
                        .cause(err)
                        .doNotRollbackAttempt()
                        .build());
                throw e;
            }
        }
    }

    private void atrRollbackCompleteLocked(boolean isAppRollback, String prefix, SpanWrapper pspan) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();
        do {
            retry.shouldRetry(false);
            assertLocked("atrRollbackComplete");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_ROLLBACK, pspan);

            try {
                LOGGER.info(attemptId, "removing ATR {} as rollback complete", getAtrDebug(atrCollection, atrId));

                errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_ROLLBACK_COMPLETE, Optional.empty());

                hooks.beforeAtrRolledBack.accept(this);

                SubdocMutateResponse response = TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                        false, false, false, false, false, 0, BINARY_COMMON_FLAGS, durabilityLevel(), OptionsUtil.createClientContext(CoreTransactionAttemptContextHooks.HOOK_ATR_ROLLBACK_COMPLETE), span,
                        Arrays.asList(
                                new SubdocMutateRequest.Command(SubdocCommandType.DELETE, prefix, null, false, true, false, 0)
                        ));

                hooks.afterAtrRolledBack.accept(this);

                setStateLocked(AttemptState.ROLLED_BACK);
                long elapsed = span.elapsedMicros();
                addUnits(response.flexibleExtras());
                LOGGER.info(attemptId, "rollback - atr rolled back{} in {}us", DebugUtil.dbg(response.flexibleExtras()), elapsed);
            } catch (Exception err) {
                MeteringUnits units = addUnits(MeteringUnits.from(err));
                span.recordException(err);

                LOGGER.info(attemptId, "error while marking ATR {} as rollback complete{} in {}us: {}",
                        getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                ErrorClass ec = classify(err);
                TransactionOperationFailedException.Builder error = createError().doNotRollbackAttempt();

                if (expiryOvertimeMode) {
                    mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_ATR_ROLLBACK_COMPLETE, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                } else if (ec == FAIL_EXPIRY) {
                    throw operationFailed(isAppRollback, createError()
                            .doNotRollbackAttempt()
                            .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                            .build());
                } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND
                        || ec == FAIL_DOC_NOT_FOUND) {
                    // Possibly the ATRs have been deleted and/or recreated, possibly we retried on a FAIL_AMBIGUOUS,
                    // either way, the entry has been removed.
                    return;
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(isAppRollback, error.build());
                } else {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                }
            } finally {
                span.finish();
            }
        } while (retry.shouldRetry());
    }

    private void rollbackDocsLocked(boolean isAppRollback, SpanWrapper span) {
        assertLocked("rollbackDocs");

        CountDownLatch latch = new CountDownLatch(stagedMutationsLocked.size());
        AtomicReference<RuntimeException> failure = new AtomicReference<>();
        ExecutorService executor = core.context().environment().transactionsSchedulers().blockingExecutor();
        Semaphore semaphore = new Semaphore(UNSTAGING_PARALLELISM);

        for (StagedMutation mutation : stagedMutationsLocked) {
            executor.submit(() -> {
                try {
                    semaphore.acquire();
                    try {
                        switch (mutation.type) {
                            case INSERT:
                                rollbackStagedInsertLocked(isAppRollback, span, mutation.collection, mutation.id, mutation.cas);
                                break;
                            default:
                                rollbackStagedReplaceOrRemoveLocked(isAppRollback, span, mutation.collection, mutation.id, mutation.cas, mutation.currentUserFlags);
                                break;
                        }
                    } finally {
                        semaphore.release();
                    }
                } catch (RuntimeException err) {
                    failure.compareAndSet(null, err);
                } catch (Throwable err) {
                    failure.compareAndSet(null, new RuntimeException(err));
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw interrupted("rollbackDocsLocked");
        }

        if (failure.get() != null) {
            throw failure.get();
        }

        LOGGER.info(attemptId, "rollback - docs rolled back");
    }

    private void rollbackStagedReplaceOrRemoveLocked(boolean isAppRollback, SpanWrapper span, CollectionIdentifier collection, String id, long cas, int userFlags) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();
        do {
            retry.shouldRetry(false);
            try {
                LOGGER.info(attemptId, "rolling back doc {} with cas {} by removing staged mutation",
                        DebugUtil.docId(collection, id), cas);
                errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC, Optional.of(id));

                hooks.beforeDocRolledBack.accept(this, id); // testing hook

                SubdocMutateResponse updatedDoc = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                        false, false, false, false, false, cas, userFlags, durabilityLevel(), OptionsUtil.createClientContext("rollbackDoc"), span,
                        Arrays.asList(
                                new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn", null, false, true, false, 0)
                        ));

                hooks.afterRollbackReplaceOrRemove.accept(this, id); // Testing hook

                addUnits(updatedDoc.flexibleExtras());
                LOGGER.info(attemptId, "rolled back doc {}{}, got cas {} and mt {}",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(updatedDoc.flexibleExtras()), updatedDoc.cas(), updatedDoc.mutationToken());

            } catch (Exception err) {
                ErrorClass ec = classify(err);
                TransactionOperationFailedException.Builder builder = createError().doNotRollbackAttempt().cause(err);
                MeteringUnits units = addUnits(MeteringUnits.from(err));
                span.recordException(err);

                logger().info(attemptId, "got error while rolling back doc {}{} in {}us: {}",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                if (expiryOvertimeMode) {
                    mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                } else if (ec == FAIL_EXPIRY) {
                    setExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ROLLBACK_DOC);
                    retry.sleepForNextBackoffAndSetShouldRetry();
                } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                    LOGGER.info(attemptId,
                            "got PATH_NOT_FOUND while cleaning up staged doc {}, it must have already been "
                                    + "rolled back, continuing",
                            DebugUtil.docId(collection, id));
                    return;
                } else if (ec == FAIL_DOC_NOT_FOUND) {
                    // Ultimately rollback has happened here
                    return;
                } else if (ec == FAIL_CAS_MISMATCH) {
                    handleDocChangedDuringRollback(span, id, collection,
                            (newCas) -> rollbackStagedReplaceOrRemoveLocked(isAppRollback, span, collection, id, newCas, userFlags));
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(isAppRollback, builder.doNotRollbackAttempt().build());
                } else {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                }
            }
        } while (retry.shouldRetry());
    }

    private void rollbackStagedInsertLocked(boolean isAppRollback,
                                            SpanWrapper span,
                                            CollectionIdentifier collection,
                                            String id,
                                            long cas) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();
        do {
            retry.shouldRetry(false);
            try {
                LOGGER.info(attemptId, "rolling back staged insert {} with cas {}",
                        DebugUtil.docId(collection, id), cas);
                errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_DELETE_INSERTED, Optional.of(id));

                hooks.beforeRollbackDeleteInserted.accept(this, id);

                SubdocMutateResponse updatedDoc = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                        false, false, false, true, false, false, cas, 0, durabilityLevel(), OptionsUtil.createClientContext("rollbackStagedInsert"), span, null,
                        Arrays.asList(
                                new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn", null, false, true, false, 0)
                        ));

                // Testing hook
                hooks.afterRollbackDeleteInserted.accept(this, id);

                addUnits(updatedDoc.flexibleExtras());
                LOGGER.info(attemptId, "deleted inserted doc {}{}, mt {}",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(updatedDoc.flexibleExtras()), updatedDoc.mutationToken());
            } catch (Exception err) {
                ErrorClass ec = classify(err);
                TransactionOperationFailedException.Builder builder = createError().cause(err);
                MeteringUnits units = addUnits(MeteringUnits.from(err));
                span.recordException(err);

                LOGGER.info(attemptId, "error while rolling back inserted doc {}{} in {}us: {}",
                        DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

                if (expiryOvertimeMode) {
                    mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_REMOVE_DOC, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                } else if (ec == FAIL_EXPIRY) {
                    setExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_REMOVE);
                    retry.sleepForNextBackoffAndSetShouldRetry();
                } else if (ec == FAIL_DOC_NOT_FOUND
                        || ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                    LOGGER.info(attemptId,
                            "got {} while removing staged insert doc {}, it must "
                                    + "have already been rolled back, continuing",
                            ec, DebugUtil.docId(collection, id));
                    return;
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(isAppRollback, builder.doNotRollbackAttempt().build());
                } else if (ec == FAIL_CAS_MISMATCH) {
                    handleDocChangedDuringRollback(span, id, collection,
                            (newCas) -> rollbackStagedInsertLocked(isAppRollback, span, collection, id, newCas));
                } else {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                }
            }
        } while (retry.shouldRetry());
    }

    /**
     * Very similar to rollbackStagedInsert, but with slightly different requirements as it's not performed
     * during rollback.
     */
    private void removeStagedInsert(CoreTransactionGetResult doc, SpanWrapper span) {
        assertNotLocked("removeStagedInsert");
        CollectionIdentifier collection = doc.collection();
        String id = doc.id();

        try {
            LOGGER.info(attemptId, "removing staged insert {} with cas {}",
                    DebugUtil.docId(collection, id), doc.cas());

            if (hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_REMOVE_STAGED_INSERT, Optional.of(id))) {
                throw operationFailed(createError()
                        .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                        .doNotRollbackAttempt()
                        .cause(new AttemptExpiredException("Attempt has expired in stage " + CoreTransactionAttemptContextHooks.HOOK_REMOVE_STAGED_INSERT))
                        .build());
            }

            hooks.beforeRemoveStagedInsert.accept(this, id);

            SubdocMutateResponse updatedDoc = TransactionKVHandler.mutateIn(core, collection, id, kvTimeoutMutating(),
                    false, false, false, true, false, false, doc.cas(), doc.userFlags(), durabilityLevel(), OptionsUtil.createClientContext("removeStagedInsert"), span, null,
                    Arrays.asList(
                            new SubdocMutateRequest.Command(SubdocCommandType.DELETE, "txn", null, false, true, false, 0)
                    ));

            hooks.afterRemoveStagedInsert.accept(this, id);

            addUnits(updatedDoc.flexibleExtras());

            // Save so the subsequent KV ops can be done with the correct CAS
            doc.cas(updatedDoc.cas());
            long elapsed = span.elapsedMicros();
            LOGGER.info(attemptId, "removed staged insert from doc {} in {}us", DebugUtil.docId(collection, id), elapsed);

            doUnderLock("removeStagedInsert " + DebugUtil.docId(collection, id),
                    () -> removeStagedMutationLocked(doc.collection(), doc.id()));
        } catch (Exception err) {
            ErrorClass ec = classify(err);
            TransactionOperationFailedException.Builder builder = createError()
                    .retryTransaction()
                    .cause(err);
            MeteringUnits units = addUnits(MeteringUnits.from(err));
            span.recordException(err);

            LOGGER.info(attemptId, "error while removing staged insert doc {}{} in {}us: {}",
                    DebugUtil.docId(collection, id), DebugUtil.dbg(units), span.elapsedMicros(), dbg(err));

            if (ec == TRANSACTION_OPERATION_FAILED) {
                throw err;
            } else if (ec == FAIL_HARD) {
                throw operationFailed(builder.doNotRollbackAttempt().build());
            } else {
                throw operationFailed(builder.build());
            }
        }
    }

    private void atrAbortLocked(String prefix,
                                SpanWrapper pspan,
                                boolean isAppRollback,
                                boolean ambiguityResolutionMode) {
        BlockingRetryHandler retry = createRetryHandlerUntilExpiry();

        do {
            retry.shouldRetry(false);
            assertLocked("atrAbort");
            SpanWrapper span = SpanWrapperUtil.createOp(this, tracer(), atrCollection.orElse(null), atrId.orElse(null), TracingIdentifiers.TRANSACTION_OP_ATR_ABORT, pspan);

            ArrayList<SubdocMutateRequest.Command> specs = new ArrayList<>();

            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_STATUS, serialize(AttemptState.ABORTED.name()), false, true, false, 0));
            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, prefix + "." + TransactionFields.ATR_FIELD_TIMESTAMP_ROLLBACK_START, serialize("${Mutation.CAS}"), false, true, true, 1));

            specs.addAll(addDocsToBuilder(specs.size()));

            try {
                LOGGER.info(attemptId, "aborting ATR {} isAppRollback={} ambiguityResolutionMode={}",
                        getAtrDebug(atrCollection, atrId), isAppRollback, ambiguityResolutionMode);
                errorIfExpiredAndNotInExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_ABORT, Optional.empty());

                hooks.beforeAtrAborted.accept(this); // testing hook

                SubdocMutateResponse response = TransactionKVHandler.mutateIn(core, atrCollection.get(), atrId.get(), kvTimeoutMutating(),
                        false, false, false, false, false, 0, BINARY_COMMON_FLAGS, durabilityLevel(), OptionsUtil.createClientContext("atrAbort"), span, specs);

                // Debug hook
                hooks.afterAtrAborted.accept(this);

                setStateLocked(AttemptState.ABORTED);
                addUnits(response.flexibleExtras());
                LOGGER.info(attemptId, "aborted ATR {}{} in {}us", getAtrDebug(atrCollection, atrId),
                        DebugUtil.dbg(response.flexibleExtras()), span.elapsedMicros());
            } catch (Exception err) {
                ErrorClass ec = classify(err);
                TransactionOperationFailedException.Builder builder = createError().cause(err).doNotRollbackAttempt();
                MeteringUnits units = addUnits(MeteringUnits.from(err));
                span.recordException(err);

                LOGGER.info(attemptId, "error {} while aborting ATR {}{}",
                        DebugUtil.dbg(err), getAtrDebug(atrCollection, atrId), DebugUtil.dbg(units));

                if (expiryOvertimeMode) {
                    mapErrorInOvertimeToExpired(isAppRollback, CoreTransactionAttemptContextHooks.HOOK_ATR_ABORT, err, FinalErrorToRaise.TRANSACTION_EXPIRED);
                } else if (ec == FAIL_EXPIRY) {
                    setExpiryOvertimeMode(CoreTransactionAttemptContextHooks.HOOK_ATR_ABORT);
                    retry.sleepForNextBackoffAndSetShouldRetry();
                } else if (ec == ErrorClass.FAIL_PATH_NOT_FOUND) {
                    throw operationFailed(isAppRollback, builder.cause(new ActiveTransactionRecordEntryNotFoundException(atrId.get(), attemptId)).build());
                } else if (ec == FAIL_DOC_NOT_FOUND) {
                    throw operationFailed(isAppRollback, builder.cause(new ActiveTransactionRecordNotFoundException(atrId.get(), attemptId)).build());
                } else if (ec == FAIL_ATR_FULL) {
                    throw operationFailed(isAppRollback, builder.cause(new ActiveTransactionRecordFullException(err)).build());
                } else if (ec == FAIL_HARD) {
                    throw operationFailed(isAppRollback, builder.doNotRollbackAttempt().build());
                } else {
                    retry.sleepForNextBackoffAndSetShouldRetry();
                }
            } finally {
                span.finish();
            }
        } while (retry.shouldRetry());
    }

    private void assertLocked(String dbg) {
        if (threadSafetyEnabled && !mutex.isHeldByCurrentThread()) {
            throw new IllegalStateException("Internal bug hit: mutex must be locked by current thread in " + dbg + " but isn't");
        }
    }

    // Used where we are in a threadpool
    private void assertLockedByAnyThread(String dbg) {
        if (threadSafetyEnabled && !mutex.isLocked()) {
            throw new IllegalStateException("Internal bug hit: mutex must be locked t " + dbg + " but isn't");
        }
    }

    private void assertNotLocked(String dbg) {
        if (threadSafetyEnabled && mutex.isHeldByCurrentThread()) {
            throw new IllegalStateException("Internal bug hit: mutex must not be held by thread " + Thread.currentThread() + " in " + dbg + " but isn't");
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
                    logger().info(attemptId, "failing operation {} as not allowed to commit (probably as previous operations have failed)", dbg);
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
     * <p>
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
    private ClassicCoreReactiveQueryResult queryInternal(final int sidx,
                                                         @Nullable final CoreQueryContext qc,
                                                         final String statement,
                                                         final CoreQueryOptionsTransactions options,
                                                         @Nullable final SpanWrapper pspan,
                                                         final boolean tximplicit) {
        hooks.beforeQuery.accept(this, statement);


        // The metrics are invaluable for debugging, override user's setting
        options.put("metrics", BooleanNode.getTrue());

        if (tximplicit) {
            options.put("tximplicit", BooleanNode.getTrue());
        }

        if (tximplicit) {

            // Workaround for MB-50914 on older server versions
            if (options.scanConsistency() == null) {
                options.put("scan_consistency", TextNode.valueOf("request_plus"));
            }

            applyQueryOptions(config, options, expiryRemainingMillis());
        }

        logger().info(attemptId, "q{} using query params {}", sidx, options.toString());

        // This is thread-safe as we're under lock.
        NodeIdentifier target = queryContext == null ? null : queryContext.queryTarget;

        // It has to be a ClassicCoreReactiveQueryResult so the cast is safe: we go down a different path entirely with Protostellar.
        ClassicCoreReactiveQueryResult result = (ClassicCoreReactiveQueryResult) queryOps.queryReactive(statement, options, qc, target, null).block();

        if (queryContext == null) {
            queryContext = new TransactionQueryContext(result.lastDispatchedTo(), qc);
            logger().info(attemptId, "q{} got query node id {}", sidx, RedactableArgument.redactMeta(queryContext.queryTarget));
        }

        hooks.afterQuery.accept(this, statement);

        return result;
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
     * <p>
     * Though the implementation is largely de-reactorised, this low-level function continues with ClassicCoreReactiveQueryResult
     * as that supports streaming & backpressure, which is needed for single query transactions.
     */
    public ClassicCoreReactiveQueryResult queryWrapperLocked(final int sidx,
                                                     @Nullable CoreQueryContext qc,
                                                     final String statement,
                                                     @Nullable final CoreQueryOptions options,
                                                     final String hookPoint,
                                                     final boolean isBeginWork,
                                                     final boolean existingErrorCheck,
                                                     @Nullable final ObjectNode txdata,
                                                     @Nullable final ArrayNode params,
                                                     @Nullable final SpanWrapper span,
                                                     final boolean tximplicit,
                                                     final boolean updateInternalState) {
        assertLocked("queryWrapper q" + sidx);

        logger().debug(attemptId, "q{}: '{}' params={} txdata={} tximplicit={}", sidx,
                RedactableArgument.redactUser(statement), RedactableArgument.redactUser(params), RedactableArgument.redactUser(txdata), tximplicit);

        if (!tximplicit && !queryModeLocked() && !isBeginWork) {
            // Thread-safety 7.6: need to unlock (to avoid deadlocks), wait for all KV ops, lock
            beginWorkIfNeeded(sidx, qc, statement, span);
        }

        // Not strictly speaking a copy, but does the same job - we're not modifying the original.
        CoreQueryOptionsTransactions optionsCopy = new CoreQueryOptionsTransactions(options);

        if (txdata != null) {
            optionsCopy.raw("txdata", txdata);
        }

        queryInternalPreLocked(sidx, statement, hookPoint, existingErrorCheck);

        if (!tximplicit && !isBeginWork) {
            optionsCopy.raw("txid", TextNode.valueOf(attemptId));
        }
        return queryInternal(sidx, qc, statement, optionsCopy, span, tximplicit);
    }

    // As per CBD-4594, we discovered after release of the API that it's not (easily) possible to support
    // streaming from ctx.query(), as errors will not be handled.  For example gocbcore could
    // report that the transaction should retry, and this would be missed and a generic fast-fail
    // reported at COMMIT time instead.  So, buffer the rows internally so errors are definitely
    // caught.
    private CoreQueryResult queryWrapperBlockingLocked(final int sidx,
                                                       @Nullable final CoreQueryContext qc,
                                                       final String statement,
                                                       @Nullable final CoreQueryOptions options,
                                                       final String hookPoint,
                                                       final boolean isBeginWork,
                                                       final boolean existingErrorCheck,
                                                       @Nullable final ObjectNode txdata,
                                                       @Nullable final ArrayNode params,
                                                       @Nullable final SpanWrapper span,
                                                       final boolean tximplicit,
                                                       final boolean updateInternalState) {
        try {
            ClassicCoreReactiveQueryResult result = queryWrapperLocked(sidx,
                    qc,
                    statement,
                    options,
                    hookPoint,
                    isBeginWork,
                    existingErrorCheck,
                    txdata,
                    params,
                    span,
                    tximplicit,
                    updateInternalState);

            List<QueryChunkRow> rows = result.rows().collectList().block();
            CoreQueryMetaData metaData = result.metaData().block();

            long elapsed = span == null ? -1 : span.elapsedMicros();

            logger().info(attemptId, "q{} returned with metrics {} after {}us", sidx, metaData.metrics().get(), elapsed);

            if (metaData.status() == CoreQueryStatus.FATAL) {
                throw operationFailed(updateInternalState, createError().build());
            } else {
                // Convert to a blocking query result
                return new CoreQueryResult() {
                    @Override
                    public Stream<QueryChunkRow> rows() {
                        return rows.stream();
                    }

                    @Override
                    public CoreQueryMetaData metaData() {
                        return metaData;
                    }

                    @Override
                    public NodeIdentifier lastDispatchedTo() {
                        return result.lastDispatchedTo();
                    }
                };
            }
        } catch (Exception err) {
            RuntimeException converted = convertQueryError(sidx, err, updateInternalState);
            long elapsed = span == null ? -1 : span.elapsedMicros();

            logger().warn(attemptId, "q{} got error {} after {}us, converted from {}", sidx, dbg(converted),
                    elapsed, dbg(err));

            if (converted != null) {
                throw converted;
            }

            throw err;
        }
    }

    private void beginWorkIfNeeded(int sidx,
                                   @Nullable final CoreQueryContext qc,
                                   String statement,
                                   SpanWrapper span) {
        // Thread-safety 7.6: need to unlock (to avoid deadlocks), wait for all KV ops, lock
        hooks.beforeUnlockQuery.accept(this, statement); // test hook

        unlock("before BEGIN WORK q" + sidx);

        waitForAllKVOpsThenLock("queryWrapper q" + sidx);

        boolean stillNeedsBeginWork = !queryModeLocked();
        LOGGER.info(attemptId, "q{} after reacquiring lock stillNeedsBeginWork={}", sidx, stillNeedsBeginWork);
        if (!queryModeLocked()) {
            queryBeginWorkLocked(qc, span);
        }
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

    private static CoreQueryOptionsTransactions applyQueryOptions(CoreMergedTransactionConfig config, CoreQueryOptionsTransactions options, long txtimeout) {
        // The BEGIN WORK scan consistency determines the default for all queries in the transaction.
        String scanConsistency = null;

        if (config.scanConsistency().isPresent()) {
            scanConsistency = config.scanConsistency().get();
        }

        // If not set, will default (in query) to REQUEST_PLUS
        if (scanConsistency != null) {
            options.put("scan_consistency", TextNode.valueOf(scanConsistency));
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

        options.put("durability_level", TextNode.valueOf(durabilityLevelString));
        options.put("txtimeout", TextNode.valueOf(txtimeout + "ms"));
        config.metadataCollection().ifPresent(metadataCollection -> {
            options.put("atrcollection",
                    TextNode.valueOf(String.format("`%s`.`%s`.`%s`",
                            metadataCollection.bucket(),
                            metadataCollection.scope().orElse(DEFAULT_SCOPE),
                            metadataCollection.collection().orElse(DEFAULT_COLLECTION))));
        });
        options.put("numatrs", IntNode.valueOf(config.numAtrs()));

        return options;
    }

    private void queryBeginWorkLocked(@Nullable CoreQueryContext qc,
                                      final SpanWrapper span) {
        assertLocked("queryBeginWork");

        stagedMutationsLocked.forEach(sm -> {
            if (sm.isStagedBinary()) {
                RuntimeException cause = new FeatureNotAvailableException("Binary documents are only supported in a KV-only transaction");
                throw operationFailed(createError().cause(cause).build());
            }

            if (sm.expiry != null) {
                RuntimeException cause = new FeatureNotAvailableException("Expiry cannot be set on documents if query is involved in the same transaction");
                throw operationFailed(createError().cause(cause).build());
            }
        });

        ObjectNode txdata = makeQueryTxDataLocked();

        CoreQueryOptionsTransactions options = new CoreQueryOptionsTransactions();
        applyQueryOptions(config, options, expiryRemainingMillis());

        String statement = "BEGIN WORK";
        int statementIdx = queryStatementIdx.getAndIncrement();

        queryWrapperBlockingLocked(statementIdx,
                qc,
                statement,
                options,
                CoreTransactionAttemptContextHooks.HOOK_QUERY_BEGIN_WORK,
                true,
                true,
                txdata,
                null,
                span,
                false,
                true);

        assertLocked("beginWork");
        // Query/gocbcore will maintain the mutations now
        stagedMutationsLocked.clear();
        if (queryContext == null) {
            throw operationFailed(TransactionOperationFailedException.Builder.createError()
                    .cause(new IllegalAccessError("Internal error: Must have a queryTarget after BEGIN WORK"))
                    .build());
        }
    }

    /**
     * Used by AttemptContext, buffers all query rows in-memory.
     */

    public CoreQueryResult queryBlocking(final String statement,
                                         @Nullable CoreQueryContext qc,
                                         @Nullable final CoreQueryOptions options,
                                         final boolean tximplicit) {
        return doQueryOperation("query blocking",
                statement,
                options,
                attemptSpan,
                (sidx, span) -> {
                    if (tximplicit) {
                        tip().provideAttr(TracingAttribute.TRANSACTION_SINGLE_QUERY, span.span(), true);
                    }
                    return queryWrapperBlockingLocked(sidx, qc, statement, options, CoreTransactionAttemptContextHooks.HOOK_QUERY, false, true, null, null, span, tximplicit, true);
                });
    }

    public Mono<CoreQueryResult> queryReactive(final String statement,
                                               @Nullable CoreQueryContext qc,
                                               @Nullable final CoreQueryOptions options,
                                               final boolean tximplicit) {
        return Mono.fromCallable(() -> queryBlocking(statement, qc, options, tximplicit))
                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerBlocking());
    }

    /**
     * This pre-amble will be called before literally any query.
     *
     * @param existingErrorCheck exists so we can call ROLLBACK even in the face of there being an existing error
     */
    void queryInternalPreLocked(final int sidx, final String statement, final String hookPoint, final boolean existingErrorCheck) {
        assertLocked("queryInternalPre");

        if (existingErrorCheck) {
            TransactionOperationFailedException returnEarly = canPerformOperation("queryInternalPre " + sidx);
            if (returnEarly != null) {
                throw returnEarly;
            }
        }

        long remaining = expiryRemainingMillis();
        boolean expiresSoon = remaining < EXPIRY_THRESHOLD;

        // Still call hasExpiredClientSide, so we can inject expiry from tests
        if (hasExpiredClientSide(hookPoint, Optional.of(statement)) || expiresSoon) {
            logger().info(attemptId, "transaction has expired in stage '{}' remaining={} threshold={}",
                    hookPoint, remaining, EXPIRY_THRESHOLD);

            throw operationFailed(createError()
                    .raiseException(FinalErrorToRaise.TRANSACTION_EXPIRED)
                    .doNotRollbackAttempt()
                    .build());
        }

        return;
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
    private void addCleanup(@Nullable CoreTransactionsCleanup cleanup) {
        CleanupRequest cleanupRequest = createCleanupRequestIfNeeded(cleanup);
        if (cleanupRequest != null && cleanup != null) {
            cleanup.add(cleanupRequest);
        }
    }

    abstract static class LambdaEndNext { }

    static class LambdaEndRetry extends LambdaEndNext { }

    static class LambdaEndFailed extends LambdaEndNext {
        public final Throwable error;

        public LambdaEndFailed(Throwable error) { this.error = error; }
    }

    static class LambdaEndSuccess extends LambdaEndNext {
    }

    @Stability.Internal
    LambdaEndNext lambdaEnd(@Nullable CoreTransactionsCleanup cleanup, @Nullable Throwable err, boolean singleQueryTransactionMode) {
        int sb = stateBits.get();
        boolean shouldNotRollback = (sb & TRANSACTION_STATE_BIT_SHOULD_NOT_ROLLBACK) != 0;
        int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
        FinalErrorToRaise finalError = FinalErrorToRaise.values()[maskedFinalError];
        boolean rollbackNeeded = finalError != FinalErrorToRaise.TRANSACTION_SUCCESS && !shouldNotRollback && !singleQueryTransactionMode;

        LOGGER.info(attemptId, "reached post-lambda in {}us, shouldNotRollback={} finalError={} rollbackNeeded={}, err (only cause of this will be used)={} tximplicit={}{}",
                TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startTimeClient.toNanos()), shouldNotRollback,
                finalError, rollbackNeeded, err, singleQueryTransactionMode,
                // Don't display the aggregated units if queryMode as it won't be accurate
                queryModeUnlocked() ? "" : meteringUnitsBuilder.toString());

        core.transactionsContext().counters().attempts().incrementBy(1);

        try {
            if (rollbackNeeded) {
                try {
                    rollbackAuto();
                } catch (Exception e) {
                    // If rollback fails, want to raise original error as cause, but with retry=false.
                    overall.LOGGER.info(attemptId, "rollback failed with {}. Original error will be raised as cause, and retry should be disabled", DebugUtil.dbg(e));
                    setStateBits("lambdaEnd", TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY, 0);
                }
            }

            // Only want to add the attempt after doing the rollback, so the attempt has the correct state (hopefully
            // ROLLED_BACK)
            addCleanup(cleanup);

        } finally {
            if (err != null) {
                attemptSpan.finishWithErrorStatus();
            } else {
                attemptSpan.finish();
            }
        }

        return retryIsRequired(err);
    }

    private LambdaEndNext retryIsRequired(Throwable err) {
        int sb = stateBits.get();
        boolean shouldNotRetry = (sb & TRANSACTION_STATE_BIT_SHOULD_NOT_RETRY) != 0;
        int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
        FinalErrorToRaise finalError = FinalErrorToRaise.values()[maskedFinalError];
        boolean retryNeeded = finalError != FinalErrorToRaise.TRANSACTION_SUCCESS && !shouldNotRetry;

        if (retryNeeded) {
            if (hasExpiredClientSide(CoreTransactionAttemptContextHooks.HOOK_BEFORE_RETRY, Optional.empty())) {
                // This will set state bits
                TransactionOperationFailedException e = operationFailed(createError()
                        .doNotRollbackAttempt()
                        .raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED)
                        .build());
                return new LambdaEndFailed(e);
            }
        }

        LOGGER.info(attemptId, "reached end of lambda post-rollback (if needed), shouldNotRetry={} finalError={} retryNeeded={}",
                shouldNotRetry, finalError, retryNeeded);

        if (retryNeeded) {
            return new LambdaEndRetry();
        }

        if (err != null) {
            return new LambdaEndFailed(err);
        }

        return new LambdaEndSuccess();
    }

    @Stability.Internal
    CoreTransactionResult transactionEnd(@Nullable Throwable err, boolean singleQueryTransactionMode) {
        boolean unstagingComplete = state == AttemptState.COMPLETED;

        CoreTransactionResult result = new CoreTransactionResult(overall.LOGGER,
                Duration.ofNanos(System.nanoTime() - overall.startTimeClient()),
                overall.transactionId(),
                unstagingComplete);

        int sb = stateBits.get();
        int maskedFinalError = (sb & STATE_BITS_MASK_FINAL_ERROR) >> STATE_BITS_POSITION_FINAL_ERROR;
        FinalErrorToRaise finalError = FinalErrorToRaise.values()[maskedFinalError];

        LOGGER.info(attemptId, "reached end of transaction, toRaise={}, err={}", finalError, DebugUtil.dbg(err));

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
            LOGGER.info(attemptId, "raising final error {} based on state bits {} masked {} tximplicit {}",
                    ret, sb, maskedFinalError, singleQueryTransactionMode);

            throw (RuntimeException) ret;
        }

        return result;
    }

    @Stability.Internal
    Throwable convertToOperationFailedIfNeeded(Throwable e, boolean singleQueryTransactionMode) {
        // If it's an TransactionOperationFailedException, the error originator has already chosen the error handling behaviour.  All
        // transaction internals will only raise this.
        if (e instanceof TransactionOperationFailedException) {
            return (TransactionOperationFailedException) e;
        } else if (e instanceof WrappedTransactionOperationFailedException) {
            return ((WrappedTransactionOperationFailedException) e).wrapped();
        } else if (singleQueryTransactionMode) {
            logger().info(attemptId(), "Caught exception from application's lambda {}, not converting", DebugUtil.dbg(e));
            return e;
        } else {
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
            logger().info(attemptId(), "Caught exception from application's lambda {}, converted it to {}",
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
