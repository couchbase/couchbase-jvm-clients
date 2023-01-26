/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.core.logging.RedactionLevel;
// [start:3.3.0]
import com.couchbase.client.core.transaction.cleanup.TransactionsCleaner;
import com.couchbase.client.core.transaction.cleanup.ClientRecord;
import com.couchbase.client.core.transaction.cleanup.ClientRecordDetails;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecordEntry;
import com.couchbase.client.core.transaction.components.ActiveTransactionRecord;
import com.couchbase.client.core.transaction.config.CoreMergedTransactionConfig;
import com.couchbase.client.core.transaction.forwards.Extension;
import com.couchbase.client.core.transaction.forwards.Supported;
import com.couchbase.client.core.cnc.events.transaction.TransactionCleanupAttemptEvent;
import com.couchbase.client.core.transaction.log.CoreTransactionLogger;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.protocol.transactions.CleanupSet;
import com.couchbase.client.protocol.transactions.CleanupSetFetchRequest;
import com.couchbase.client.protocol.transactions.CleanupSetFetchResponse;
import com.couchbase.client.protocol.transactions.ClientRecordProcessRequest;
import com.couchbase.client.protocol.transactions.ClientRecordProcessResponse;
import com.couchbase.client.protocol.transactions.TransactionCleanupAttempt;
import com.couchbase.client.protocol.transactions.TransactionCleanupRequest;
import com.couchbase.client.protocol.transactions.TransactionCreateRequest;
import com.couchbase.client.protocol.transactions.TransactionResult;
import com.couchbase.client.protocol.transactions.TransactionSingleQueryRequest;
import com.couchbase.client.protocol.transactions.TransactionSingleQueryResponse;
import com.couchbase.client.protocol.transactions.TransactionStreamDriverToPerformer;
import com.couchbase.client.protocol.transactions.TransactionStreamPerformerToDriver;
import com.couchbase.transactions.SingleQueryTransactionExecutor;
import com.couchbase.twoway.TwoWayTransactionBlocking;
import com.couchbase.twoway.TwoWayTransactionMarshaller;
import com.couchbase.twoway.TwoWayTransactionReactive;
import com.couchbase.utils.ResultsUtil;
import com.couchbase.utils.HooksUtil;
// [end:3.3.0]
import com.couchbase.client.protocol.observability.SpanCreateRequest;
import com.couchbase.client.protocol.observability.SpanCreateResponse;
import com.couchbase.client.protocol.observability.SpanFinishRequest;
import com.couchbase.client.protocol.observability.SpanFinishResponse;
import com.couchbase.client.protocol.performer.Caps;
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor;
import com.couchbase.client.protocol.shared.Collection;
import com.couchbase.client.protocol.performer.Caps;
import com.couchbase.client.protocol.shared.Collection;
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor;
import com.couchbase.client.performer.core.CorePerformer;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.protocol.performer.PerformerCapsFetchResponse;
import com.couchbase.client.protocol.shared.API;
import com.couchbase.client.protocol.shared.ClusterConnectionCloseRequest;
import com.couchbase.client.protocol.shared.ClusterConnectionCloseResponse;
import com.couchbase.client.protocol.shared.ClusterConnectionCreateRequest;
import com.couchbase.client.protocol.shared.ClusterConnectionCreateResponse;
import com.couchbase.client.protocol.shared.DisconnectConnectionsRequest;
import com.couchbase.client.protocol.shared.DisconnectConnectionsResponse;
import com.couchbase.client.protocol.shared.EchoRequest;
import com.couchbase.client.protocol.shared.EchoResponse;
import com.couchbase.utils.Capabilities;
import com.couchbase.utils.ClusterConnection;
import com.couchbase.utils.OptionsUtil;
import com.couchbase.utils.VersionUtil;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Hooks;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;

public class PerformerService extends CorePerformer {
    private static final Logger logger = LoggerFactory.getLogger(PerformerService.class);
    private static final ConcurrentHashMap<String, ClusterConnection> clusterConnections = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, RequestSpan> spans = new ConcurrentHashMap<>();

    // Allows capturing various errors so we can notify the driver of problems.
    public static AtomicReference<String> globalError = new AtomicReference<>();

    @Override
    protected SdkCommandExecutor executor(com.couchbase.client.protocol.run.Workloads workloads, Counters counters, API api) {
        var connection = clusterConnections.get(workloads.getClusterConnectionId());
        return api == API.DEFAULT
                ? new JavaSdkCommandExecutor(connection, counters, spans)
                : new ReactiveJavaSdkCommandExecutor(connection, counters, spans);
    }

    @Override
    protected TransactionCommandExecutor transactionsExecutor(com.couchbase.client.protocol.run.Workloads workloads, Counters counters) {
        // [start:3.3.0]
        var connection = clusterConnections.get(workloads.getClusterConnectionId());
        return new JavaTransactionCommandExecutor(connection, counters, spans);
        // [end:3.3.0]
        // [start:<3.3.0]
        /*
        return null;
        // [end:<3.3.0]
         */
    }

    @Override
    protected void customisePerformerCaps(PerformerCapsFetchResponse.Builder response) {
        response.addAllSdkImplementationCaps(Capabilities.sdkImplementationCaps());
        response.setLibraryVersion(VersionUtil.introspectSDKVersion());

        // [start:3.3.0]
        for (Extension ext : Extension.SUPPORTED) {
            try {
                var pc = com.couchbase.client.protocol.transactions.Caps.valueOf(ext.name());
                response.addTransactionImplementationsCaps(pc);
            } catch (IllegalArgumentException err) {
                // FIT and Java have used slightly different names for this
                if (ext.name().equals("EXT_CUSTOM_METADATA")) {
                    response.addTransactionImplementationsCaps(com.couchbase.client.protocol.transactions.Caps.EXT_CUSTOM_METADATA_COLLECTION);
                } else {
                    logger.warn("Could not find FIT extension for " + ext.name());
                }
            }
        }

        var supported = new Supported();
        var protocolVersion = supported.protocolMajor + "." + supported.protocolMinor;

        response.setTransactionsProtocolVersion(protocolVersion);

        logger.info("Performer implements protocol {} with caps {}",
            protocolVersion, response.getPerformerCapsList());
        response.addPerformerCaps(Caps.TRANSACTIONS_WORKLOAD_1);
        response.addPerformerCaps(Caps.TRANSACTIONS_SUPPORT_1);
        // [end:3.3.0]
        response.addSupportedApis(API.ASYNC);
        response.addPerformerCaps(Caps.CLUSTER_CONFIG_1);
        // Some observability options blocks changed name here
        // [start:3.2.0]
        response.addPerformerCaps(Caps.OBSERVABILITY_1);
        // [end:3.2.0]
        response.setPerformerUserAgent("java-sdk");
    }

    @Override
    public void clusterConnectionCreate(ClusterConnectionCreateRequest request,
                                        StreamObserver<ClusterConnectionCreateResponse> responseObserver) {
        try {
            var clusterConnectionId = request.getClusterConnectionId();
            // Need this callback as we have to configure hooks to do something with a Cluster that we haven't created yet.
            Supplier<ClusterConnection> getCluster = () -> clusterConnections.get(clusterConnectionId);
            var onClusterConnectionClose = new ArrayList<Runnable>();

            request.getTunablesMap().forEach((k, v) -> {
                logger.info("Setting cluster-level tunable {}={}", k, v);
                if (v != null) {
                    System.setProperty(k, v);
                }
            });

            onClusterConnectionClose.add(() -> {
                request.getTunablesMap().forEach((k, v) -> {
                    logger.info("Clearing cluster-level tunable {}", k);
                    if (v != null) {
                        System.clearProperty(k);
                    }
                });
            });

            var clusterEnvironment = OptionsUtil.convertClusterConfig(request, getCluster, onClusterConnectionClose);

            var connection = new ClusterConnection(request.getClusterHostname(),
                    request.getClusterUsername(),
                    request.getClusterPassword(),
                    clusterEnvironment,
                    onClusterConnectionClose);
            clusterConnections.put(clusterConnectionId, connection);
            logger.info("Created cluster connection {} for user {}, now have {}",
                    clusterConnectionId, request.getClusterUsername(), clusterConnections.size());

            // Fine to have a default and a per-test connection open, any more suggests a leak
            logger.info("Dumping {} cluster connections for resource leak troubleshooting:", clusterConnections.size());
            clusterConnections.forEach((key, value) -> logger.info("Cluster connection {} {}", key, value.username));

            responseObserver.onNext(ClusterConnectionCreateResponse.newBuilder()
                    .setClusterConnectionCount(clusterConnections.size())
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException err) {
            logger.error("Operation failed during clusterConnectionCreate due to : " + err);
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }

    public void clusterConnectionClose(ClusterConnectionCloseRequest request,
                                       StreamObserver<ClusterConnectionCloseResponse> responseObserver) {
        var cc = clusterConnections.get(request.getClusterConnectionId());
        cc.close();
        clusterConnections.remove(request.getClusterConnectionId());
        responseObserver.onNext(ClusterConnectionCloseResponse.newBuilder()
                .setClusterConnectionCount(clusterConnections.size())
                .build());
        responseObserver.onCompleted();
    }

    // [start:3.3.0]
    @Override
    public void transactionCreate(TransactionCreateRequest request,
                                  StreamObserver<TransactionResult> responseObserver) {
        try {
            ClusterConnection connection = getClusterConnection(request.getClusterConnectionId());

            logger.info("Starting transaction on cluster connection {} created for user {}",
                    request.getClusterConnectionId(), connection.username);

            TransactionResult response;
            if (request.getApi() == API.DEFAULT) {
                response = TwoWayTransactionBlocking.run(connection, request, (TransactionCommandExecutor) null, false, spans);
            }
            else {
                response = TwoWayTransactionReactive.run(connection, request, spans);
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException err) {
            logger.error("Operation failed during transactionCreate due to :  " + err);
            err.printStackTrace();
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }
    // [end:3.3.0]

    @Override
    public  void echo(EchoRequest request , StreamObserver<EchoResponse> responseObserver){
        try {
            logger.info("================ {} : {} ================ ", request.getTestName(), request.getMessage());
            responseObserver.onNext(EchoResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (RuntimeException err) {
            logger.error("Echo of Test {} for message {} failed : {} " +request.getTestName(),request.getMessage(), err);
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }

    @Override
    public void disconnectConnections(DisconnectConnectionsRequest request, StreamObserver<DisconnectConnectionsResponse> responseObserver) {
        try {
            logger.info("Closing all {} connections from performer to cluster", clusterConnections.size());

            clusterConnections.forEach((key, value) -> value.close());
            clusterConnections.clear();

            responseObserver.onNext(DisconnectConnectionsResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (RuntimeException err) {
            logger.error("Operation failed while closing cluster connections : " + err);
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }

    // [start:3.3.0]
    @Override
    public StreamObserver<TransactionStreamDriverToPerformer> transactionStream(
            StreamObserver<TransactionStreamPerformerToDriver> toTest) {
        var marshaller = new TwoWayTransactionMarshaller(clusterConnections, spans);

        return marshaller.run(toTest);
    }
    // [end:3.3.0]

    private static CollectionIdentifier collectionIdentifierFor(com.couchbase.client.protocol.transactions.DocId doc) {
        return new CollectionIdentifier(doc.getBucketName(), Optional.of(doc.getScopeName()), Optional.of(doc.getCollectionName()));
    }

    // [start:3.3.0]
    @Override
    public void transactionCleanup(TransactionCleanupRequest request,
                                   StreamObserver<TransactionCleanupAttempt> responseObserver) {
        try {
            logger.info("Starting transaction cleanup attempt");
            // Only the KV timeout is used from this
            var config = TransactionsConfig.builder().build();
            var connection = getClusterConnection(request.getClusterConnectionId());
            var collection = collectionIdentifierFor(request.getAtr());
            connection.waitUntilReady(collection);
            var cleanupHooks = HooksUtil.configureCleanupHooks(request.getHookList(), () -> connection);
            var cleaner = new TransactionsCleaner(connection.core(), cleanupHooks);
            var logger = new CoreTransactionLogger(null, "");
            var merged = new CoreMergedTransactionConfig(config);

            Optional<ActiveTransactionRecordEntry> atrEntry = ActiveTransactionRecord.findEntryForTransaction(connection.core(),
                            collection,
                            request.getAtr().getDocId(),
                            request.getAttemptId(),
                            merged,
                            null,
                            logger)
                    .block();

            TransactionCleanupAttempt response;
            TransactionCleanupAttemptEvent result = null;

            if (atrEntry.isPresent()) {
                result = cleaner.cleanupATREntry(collection,
                        request.getAtrId(),
                        request.getAttemptId(),
                        atrEntry.get(),
                        false)
                        .block();
            }

            if (result != null) {
                response = ResultsUtil.mapCleanupAttempt(result, atrEntry);
            }
            else {
                // Can happen if 2+ cleanups are being executed concurrently
                response = TransactionCleanupAttempt.newBuilder()
                        .setSuccess(false)
                        .setAtr(request.getAtr())
                        .setAttemptId(request.getAttemptId())
                        .addLogs("Failed at performer to get ATR entry before running cleanupATREntry")
                        .build();
            }

            logger.info("Finished transaction cleanup attempt, success={}", response.getSuccess());

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException err) {
            logger.error("Operation failed during transactionCleanup due to : " + err);
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }

    @Override
    public void clientRecordProcess(ClientRecordProcessRequest request,
                                    StreamObserver<ClientRecordProcessResponse> responseObserver) {
        try {
            logger.info("Starting client record process attempt");

            var config = TransactionsConfig.builder().build();
            ClusterConnection connection = getClusterConnection(request.getClusterConnectionId());

            var collection = new CollectionIdentifier(request.getBucketName(),
                    Optional.of(request.getScopeName()),
                    Optional.of(request.getCollectionName()));

            connection.waitUntilReady(collection);

            ClientRecord cr = HooksUtil.configureClientRecordHooks(request.getHookList(), connection);

            ClientRecordProcessResponse.Builder response = ClientRecordProcessResponse.newBuilder();

            try {
                ClientRecordDetails result = cr.processClient(request.getClientUuid(),
                                collection,
                                config,
                                null)
                        .block();

                response.setSuccess(true)
                        .setNumActiveClients(result.numActiveClients())
                        .setIndexOfThisClient(result.indexOfThisClient())
                        .addAllExpiredClientIds(result.expiredClientIds())
                        .setNumExistingClients(result.numExistingClients())
                        .setNumExpiredClients(result.numExpiredClients())
                        .setOverrideActive(result.overrideActive())
                        .setOverrideEnabled(result.overrideEnabled())
                        .setOverrideExpires(result.overrideExpires())
                        .setCasNowNanos(result.casNow())
                        .setClientUuid(request.getClientUuid())
                        .build();
            }
            catch (RuntimeException err) {
                response.setSuccess(false);
            }

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RuntimeException err) {
            logger.error("Operation failed during clientRecordProcess due to : " + err);
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }

    @Override
    public void transactionSingleQuery(TransactionSingleQueryRequest request,
                                       StreamObserver<TransactionSingleQueryResponse> responseObserver) {
        try {
            var connection = getClusterConnection(request.getClusterConnectionId());

            logger.info("Performing single query transaction on cluster connection {} (user {})",
                    request.getClusterConnectionId(),
                    connection.username);

            TransactionSingleQueryResponse ret = SingleQueryTransactionExecutor.execute(request, connection, spans);

            responseObserver.onNext(ret);
            responseObserver.onCompleted();
        } catch (Throwable err) {
            logger.error("Operation failed during transactionSingleQuery due to : " + err.toString());
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }

    public void cleanupSetFetch(CleanupSetFetchRequest request, StreamObserver<CleanupSetFetchResponse> responseObserver) {
        try {
            var connection = getClusterConnection(request.getClusterConnectionId());

            var cleanupSet = connection.core().transactionsCleanup().cleanupSet().stream()
                    .map(cs -> Collection.newBuilder()
                            .setBucketName(cs.bucket())
                            .setScopeName(cs.scope().orElse(DEFAULT_SCOPE))
                            .setCollectionName(cs.collection().orElse(DEFAULT_COLLECTION))
                            .build())
                    .collect(Collectors.toList());

            responseObserver.onNext(CleanupSetFetchResponse.newBuilder()
                            .setCleanupSet(CleanupSet.newBuilder()
                                    .addAllCleanupSet(cleanupSet))
                    .build());
            responseObserver.onCompleted();
        } catch (Throwable err) {
            logger.error("Operation failed during cleanupSetFetch due to {}", err.toString());
            responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
        }
    }
    // [end:3.3.0]

    @Override
    public void spanCreate(SpanCreateRequest request, StreamObserver<SpanCreateResponse> responseObserver) {
        var parent = request.hasParentSpanId()
                ? spans.get(request.getParentSpanId())
                : null;
        var span = getClusterConnection(request.getClusterConnectionId())
                .cluster()
                .environment()
                .requestTracer()
                .requestSpan(request.getName(), parent);
        // RequestSpan interface finalised here
        // [start:3.1.6]
        request.getAttributesMap().forEach((k, v) -> {
            if (v.hasValueBoolean()) {
                span.attribute(k, v.getValueBoolean());
            }
            else if (v.hasValueLong()) {
                span.attribute(k, v.getValueLong());
            }
            else if (v.hasValueString()) {
                span.attribute(k, v.getValueString());
            }
            else throw new UnsupportedOperationException();
        });
        // [end:3.1.6]
        spans.put(request.getId(), span);
        responseObserver.onNext(SpanCreateResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void spanFinish(SpanFinishRequest request, StreamObserver<SpanFinishResponse> responseObserver) {
        // [start:3.1.6]
        spans.get(request.getId()).end();
        // [end:3.1.6]
        spans.remove(request.getId());
        responseObserver.onNext(SpanFinishResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 8060;

        // Better reactor stack traces for low cost
        // Unfortunately we cannot have this without pulling in reactor-tools, which can then pill in an incompatible
        // reactor-core when we are building old versions of the SDK.
        // ReactorDebugAgent.init();

        // Control ultra-verbose logging
        System.setProperty("com.couchbase.transactions.debug.lock", "true");
        System.setProperty("com.couchbase.transactions.debug.monoBridge", "false");

        // Setup global error handlers
        Hooks.onErrorDropped(err -> {
            globalError.set("Hook dropped (raised async so could have been in an earlier test): " + err + " cause: " + (err.getCause() != null ? err.getCause().getMessage() : "-"));
            logger.warn(err.toString());
            for (var ex : err.getStackTrace()) {
                logger.warn(ex.toString());
            }
        });

        // Blockhound is disabled as it causes an immediate runtime error on Jenkins
//        BlockHound
//                .builder()
//                .blockingMethodCallback(blockingMethod -> {
//                    globalError.set("Blocking method detected: " + blockingMethod);
//                })
//                .install();

        //Need to send parameters in format : port=8060 version=1.1.0 loglevel=all:Info
        for(String parameter : args) {
            switch (parameter.split("=")[0]) {
                case "port":
                    port= Integer.parseInt(parameter.split("=")[1]);
                    break;
                default:
                    logger.warn("Undefined input: {}. Ignoring it",parameter);
            }
        }

        // Force that log redaction has been enabled
        LogRedaction.setRedactionLevel(RedactionLevel.PARTIAL);

        Server server = ServerBuilder.forPort(port)
                .addService(new PerformerService())
                .build();
        server.start();
        logger.info("Server Started at {}", server.getPort());
        server.awaitTermination();
    }


    public static ClusterConnection getClusterConnection(@Nullable String clusterConnectionId) {
        return clusterConnections.get(clusterConnectionId);
    }
}