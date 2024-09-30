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
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.transaction.CoreTransactionsReactive;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.error.internal.ErrorClass;
import com.couchbase.client.core.transaction.support.OptionsUtil;
import com.couchbase.client.core.transaction.support.SpanWrapper;
import com.couchbase.client.core.transaction.support.SpanWrapperUtil;
import com.couchbase.client.core.transaction.util.DebugUtil;
import com.couchbase.client.core.transaction.util.TransactionKVHandler;
import com.couchbase.client.core.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents the ClientRecord doc, a single document that contains an entry for every client (app) current
 * participating
 * in the cleanup of 'lost' transactions.
 * <p>
 * ClientRecord isn't as contended as it appears.  It's only read and written to by each client once per cleanup window
 * (default for this is 60 seconds).  It does remain a single point of failure, but with a sensible number of replicas
 * this is unlikely to be a problem.
 * <p>
 * All writes are non-durable.  If a write is rolled back then it's not critical, it will just take a little longer to
 * find lost txns.
 *
 * @author Graham Pople
 */
@Stability.Internal
public class ClientRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreTransactionsCleanup.CATEGORY_CLIENT_RECORD);

    private final Core core;

    public static final String CLIENT_RECORD_DOC_ID = "_txn:client-record";
    private static final String FIELD_HEARTBEAT = "heartbeat_ms";
    private static final String FIELD_EXPIRES = "expires_ms";
    private static final String FIELD_NUM_ATRS = "num_atrs";
    private static final String FIELD_HOST = "host";
    private static final String FIELD_IMPLEMENTATION = "implementation";
    private static final String FIELD_VERSION = "version";
    private static final String FIELD_PROCESS_ID = "process_id";
    public static final String FIELD_RECORDS = "records";
    public static final String FIELD_CLIENTS = "clients";
    public static final String FIELD_OVERRIDE = "override";
    public static final String FIELD_OVERRIDE_ENABLED = "enabled";
    public static final String FIELD_OVERRIDE_EXPIRES = "expires";
    private static final int SAFETY_MARGIN_EXPIRY_MILLIS = 20000;
    private static final Duration TIMEOUT = Duration.ofMillis(500);
    private static final Duration BACKOFF_START = Duration.ofMillis(10);
    private static final Duration BACKOFF_END = Duration.ofMillis(250);

    public ClientRecord(Core core) {
        this.core = Objects.requireNonNull(core);
    }

    public Flux<Void> removeClientFromClientRecord(String clientUuid, Set<CollectionIdentifier> cleanupSet) {
        return removeClientFromClientRecord(clientUuid, TIMEOUT, cleanupSet);
    }

    /**
     * Called on shutdown to cleanly remove a client from the client-record.
     */
    public Flux<Void> removeClientFromClientRecord(String clientUuid, Duration timeout, Set<CollectionIdentifier> collections) {
        return Flux.fromIterable(collections)

                .subscribeOn(core.context().environment().transactionsSchedulers().schedulerCleanup())

                .doOnNext(v -> LOGGER.info("{} removing from client record on collection {}", clientUuid, RedactableArgument.redactUser(v)))

                .concatMap(collection -> beforeRemoveClient(this) // testing hook

                        // Use default timeout+durability here.  App is probably shutting down, let's not
                        // hold it up.  Not mission critical if record doesn't get removed, it will get
                        // cleaned up when it expires.
                        .then(TransactionKVHandler.mutateIn(core, collection, CLIENT_RECORD_DOC_ID, mutatingTimeout(),
                                false, false, false,
                                false, false, 0, CodecFlags.BINARY_COMMON_FLAGS,
                                Optional.empty(), OptionsUtil.createClientContext("Cleaner::removeClientFromCleanupSet"), null, Arrays.asList(
                                        new SubdocMutateRequest.Command(SubdocCommandType.DELETE, FIELD_RECORDS + "." + FIELD_CLIENTS + "." + clientUuid, Bytes.EMPTY_BYTE_ARRAY, false, true, false, 0)
                                )))

                        .onErrorResume(err -> {
                            switch (ErrorClass.classify(err)) {
                                case FAIL_DOC_NOT_FOUND:
                                    LOGGER.info("{}/{} remove skipped as client record does not exist",
                                            RedactableArgument.redactUser(collection), clientUuid);
                                    return Mono.empty();
                                case FAIL_PATH_NOT_FOUND:
                                    LOGGER.info("{}/{} remove skipped as client record entry does not exist",
                                            RedactableArgument.redactUser(collection), clientUuid);
                                    return Mono.empty();
                                default:
                                    LOGGER.info("{}/{} got error while removing client from client record: {}",
                                            RedactableArgument.redactUser(collection), clientUuid, DebugUtil.dbg(err));
                                    return Mono.error(err);
                            }
                        })

                        // TXNJ-96
                        .retryWhen(Retry.any()
                                .exponentialBackoff(BACKOFF_START, BACKOFF_END)
                                .doOnRetry(v -> {
                                    LOGGER.info("{}/{} retrying removing client from record on error {}",
                                            RedactableArgument.redactUser(collection), clientUuid, DebugUtil.dbg(v.exception()));
                                })
                                .toReactorRetry())

                        .timeout(timeout)

                        .doOnNext(v -> {
                            LOGGER.info("{}/{} removed from client record",
                                    RedactableArgument.redactUser(collection), clientUuid);
                        })

                        .doOnError(err -> {
                            LOGGER.info("got error while removing client record '{}'", String.valueOf(err));
                        })
                        .then());
    }

    private Duration mutatingTimeout() {
        return core.context().environment().timeoutConfig().kvDurableTimeout();
    }

    private Duration nonMutatingTimeout() {
        return core.context().environment().timeoutConfig().kvTimeout();
    }

    public static ClientRecordDetails parseClientRecord(CoreSubdocGetResult clientRecord, String clientUuid) {
        try {
            JsonNode records = Mapper.reader().readValue(clientRecord.field(0).value(), JsonNode.class);
            JsonNode hlcRaw = Mapper.reader().readValue(clientRecord.field(1).value(), JsonNode.class);
            ActiveTransactionRecord.ParsedHLC parsedHLC = new ActiveTransactionRecord.ParsedHLC(hlcRaw);
            JsonNode clients = records.get("clients");

            final List<String> expiredClientIds = new ArrayList<>();
            final List<String> activeClientIds = new ArrayList<>();

            Iterator<String> iterator = clients.fieldNames();
            while (iterator.hasNext()) {
                String otherClientId = iterator.next();
                JsonNode cl = clients.get(otherClientId);
                final long casMillis = parsedHLC.nowInNanos() / 1_000_000;

                long heartbeatMillis = ActiveTransactionRecord.parseMutationCAS(cl.get(FIELD_HEARTBEAT).textValue());
                int expiresMsecs = cl.get(FIELD_EXPIRES).intValue();
                long expiredPeriod = casMillis - heartbeatMillis;
                boolean hasExpired = expiredPeriod >= expiresMsecs;
                // Don't count this client as expired, it's just about to re-add itself
                boolean out = hasExpired && !(otherClientId.equals(clientUuid));
                if (out) expiredClientIds.add(otherClientId);
                else activeClientIds.add(otherClientId);
            }

            if (!activeClientIds.contains(clientUuid)) {
                // This client is just about to add itself
                activeClientIds.add(clientUuid);
            }

            // The clients are stored as a map, but if we sort them we get an array and hence
            // know this client's index in it.
            List<String> sortedActiveClientIds = activeClientIds.stream().sorted().collect(Collectors.toList());

            int indexOfThisClient = sortedActiveClientIds.indexOf(clientUuid);
            int numExpiredClients = expiredClientIds.size();
            int numActiveClients = sortedActiveClientIds.size();
            int numExistingClients = numExpiredClients + numActiveClients;
            boolean alreadyContainsClient = clients.has(clientUuid);

            boolean overrideEnabled = false;
            long overrideExpiresCas = 0;

            JsonNode override = records.get("override");

            if (override != null) {
                overrideEnabled = override.get(FIELD_OVERRIDE_ENABLED).asBoolean();
                overrideExpiresCas = override.get(FIELD_OVERRIDE_EXPIRES).asLong();
            }

            return new ClientRecordDetails(numActiveClients,
                    indexOfThisClient,
                    !alreadyContainsClient,
                    expiredClientIds,
                    numExistingClients,
                    numExpiredClients,
                    overrideEnabled,
                    overrideExpiresCas,
                    parsedHLC.nowInNanos());
        } catch (IOException e) {
            throw new DecodingFailureException(e);
        }
    }

    public Mono<CoreSubdocGetResult> getClientRecord(CollectionIdentifier collection, @Nullable SpanWrapper span) {
        return TransactionKVHandler.lookupIn(core, collection, CLIENT_RECORD_DOC_ID, nonMutatingTimeout(), false, OptionsUtil.createClientContext("ClientRecord::getClientRecord"), span,
                false,
                Arrays.asList(
                        new SubdocGetRequest.Command(SubdocCommandType.GET, FIELD_RECORDS, true, 0),
                        new SubdocGetRequest.Command(SubdocCommandType.GET, "$vbucket.HLC", true, 1)
                ));
    }

    private RequestTracer tracer() {
        return core.context().coreResources().requestTracer();
    }

    /*
     * Handles all the logic to do with a client checking the client record, adding or updating itself, pruning expired
     * clients, and finding which ATRs it should be checking.
     */
    public Mono<ClientRecordDetails> processClient(String clientUuid,
                                                   CollectionIdentifier collection,
                                                   CoreTransactionsConfig config,
                                                   @Nullable SpanWrapper pspan) {
        return Mono.defer(() -> {
            SpanWrapper span = SpanWrapperUtil.createOp(null, tracer(), collection, CLIENT_RECORD_DOC_ID, TracingIdentifiers.TRANSACTION_CLEANUP_CLIENT, pspan)
                    .attribute(TracingIdentifiers.ATTR_TRANSACTION_CLEANUP_CLIENT_ID, clientUuid);

            String bp = collection.bucket() + "/" + collection.scope().orElse("-") + "/" + collection.collection().orElse("-") + "/" + clientUuid;

            return beforeGetRecord(this)

                    .then(getClientRecord(collection, span))

                    .flatMap(clientRecord -> {
                        ClientRecordDetails cr = parseClientRecord(clientRecord, clientUuid);

                        LOGGER.debug("{} found {} existing clients including this ({} active, {} " +
                                        "expired), included this={}, index of this={}, override={enabled={},expires={},now={},active={}}",
                                bp, cr.numExistingClients(), cr.numActiveClients(), cr.numExpiredClients(),
                                !cr.clientIsNew(), cr.indexOfThisClient(),
                                cr.overrideEnabled(), cr.overrideExpires(), cr.casNow(), cr.overrideActive());

                        if (cr.overrideActive()) {
                            // Nothing to do: an external process has taken over cleanup, for now.
                            return Mono.just(cr);
                        } else {
                            ArrayList<SubdocMutateRequest.Command> specs = new ArrayList<>();

                            String field = FIELD_RECORDS + "." + FIELD_CLIENTS + "." + clientUuid;

                            String host = "unavailable";
                            try {
                                host = InetAddress.getLocalHost().getHostAddress();
                            } catch (Throwable e) {
                            }

                            long pid = 0;
                            String name = ManagementFactory.getRuntimeMXBean().getName();
                            try {
                                pid = Long.parseLong(name.split("@")[0]);
                            } catch (Throwable err) {
                                LOGGER.debug("Discarding error {} while trying to parse PID {}", err.getMessage(), name);
                            }

                            // Either update existing record or add new one
                            byte[] toWrite = Mapper.encodeAsBytes(Mapper.createObjectNode()
                                    .put(FIELD_EXPIRES, config.cleanupConfig().cleanupWindow().toMillis() + SAFETY_MARGIN_EXPIRY_MILLIS)
                                    .put(FIELD_NUM_ATRS, config.numAtrs())
                                    .put(FIELD_IMPLEMENTATION, "java")
                                    .put(FIELD_VERSION, CoreTransactionsReactive.class.getPackage().getImplementationVersion())
                                    .put(FIELD_HOST, host)
                                    .put(FIELD_PROCESS_ID, pid));
                            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, field, toWrite, true, true, false, 0));
                            try {
                                specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DICT_UPSERT, field + "." + FIELD_HEARTBEAT, Mapper.writer().writeValueAsBytes("${Mutation.CAS}"), false, true, true, 1));
                            } catch (JsonProcessingException e) {
                                throw new EncodingFailureException(e);
                            }

                            // Subdoc supports a max of 16 fields, fill up the rest with removing expired clients
                            // Any remaining expired clients will be handled on another run or by another client
                            cr.expiredClientIds().stream()
                                    .limit(SubdocMutateRequest.SUBDOC_MAX_FIELDS - specs.size() - 1)
                                    .forEach(expiredClientId -> {
                                        LOGGER.debug("{} removing expired client {}",
                                                bp, expiredClientId);

                                        specs.add(new SubdocMutateRequest.Command(SubdocCommandType.DELETE, FIELD_RECORDS + "." + FIELD_CLIENTS + "." + expiredClientId, null, false, true, false, specs.size()));
                                    });

                            specs.add(new SubdocMutateRequest.Command(SubdocCommandType.SET_DOC, "", new byte[]{0}, false, false, false, specs.size()));

                            return beforeUpdateRecord(this) // testing hook

                                    // Use default timeout+durability, as this update is not mission critical.
                                    .then(TransactionKVHandler.mutateIn(core, collection, CLIENT_RECORD_DOC_ID, mutatingTimeout(),
                                            false, false, false, false, false, 0, CodecFlags.BINARY_COMMON_FLAGS,
                                            Optional.empty(), OptionsUtil.createClientContext("ClientRecord::processClient"), span, specs))

                                    .thenReturn(cr);
                        }
                    })

                    .onErrorResume(err -> {
                        ErrorClass ec = ErrorClass.classify(err);

                        LOGGER.debug("{} got error processing client record: {}", bp, DebugUtil.dbg(err));

                        if (ec == ErrorClass.FAIL_DOC_NOT_FOUND) {
                            return createClientRecord(clientUuid, collection, span)
                                    .then(processClient(clientUuid, collection, config, pspan));
                        } else if ((err instanceof CouchbaseException)
                                && ((CouchbaseException) err).context() != null
                                && ((CouchbaseException) err).context().responseStatus() == ResponseStatus.NO_ACCESS) {
                            // This will catch both read and write access failures
                            // These aren't logged as it'll get spammy, since the thread is tried every periodically
                            return Mono.error(new AccessErrorException());
                        } else {
                            // Any other errors, propagate through for perBucketThread to handle
                            return Mono.error(err);
                        }
                    })

                    .doOnError(err -> span.finish(err))
                    .doOnTerminate(() -> span.finish());
        });
    }

    private Mono<Void> createClientRecord(String clientUuid,
                                          CollectionIdentifier collection,
                                          SpanWrapper pspan) {
        String bp = collection.bucket() + "/" + collection.scope().orElse("-") + "/" + collection.collection().orElse("-") + "/" + clientUuid;

        return beforeCreateRecord(this) // testing hook

                .then(TransactionKVHandler.mutateIn(core, collection, CLIENT_RECORD_DOC_ID, mutatingTimeout(),
                        true, false, false, false, false, 0, CodecFlags.BINARY_COMMON_FLAGS,
                        Optional.empty(), OptionsUtil.createClientContext("ClientRecord::createClientRecord"), pspan, Arrays.asList(
                                new SubdocMutateRequest.Command(SubdocCommandType.DICT_ADD, FIELD_RECORDS + "." + FIELD_CLIENTS, "{}".getBytes(StandardCharsets.UTF_8), false, true, false, 0),
                                new SubdocMutateRequest.Command(SubdocCommandType.SET_DOC, "", new byte[]{0}, false, false, false, 1))))

                .doOnSubscribe(v -> {
                    LOGGER.debug("{} found client record does not exist, creating and retrying", bp);
                })

                .onErrorResume(e -> {
                    if (ErrorClass.FAIL_DOC_ALREADY_EXISTS == ErrorClass.classify(e)) {
                        LOGGER.debug("{} found client record exists after retry, another client " +
                                "must have created it, continuing", bp);
                        return Mono.empty();
                    } else if ((e instanceof CouchbaseException)
                            && ((CouchbaseException) e).context().responseStatus() == ResponseStatus.NO_ACCESS) {
                        return Mono.error(new AccessErrorException());
                    } else {
                        LOGGER.info("got error while creating client record '{}'", String.valueOf(e));
                        return Mono.error(e);
                    }
                })

                .then();
    }

    // Testing hooks
    protected Mono<Integer> beforeCreateRecord(ClientRecord self) {
        return Mono.just(1);
    }

    protected Mono<Integer> beforeRemoveClient(ClientRecord self) {
        return Mono.just(1);
    }

    @Deprecated // No longer called as of TXNJ-274
    protected Mono<Integer> beforeUpdateCAS(ClientRecord self) {
        return Mono.just(1);
    }

    protected Mono<Integer> beforeGetRecord(ClientRecord self) {
        return Mono.just(1);
    }

    protected Mono<Integer> beforeUpdateRecord(ClientRecord self) {
        return Mono.just(1);
    }
}
