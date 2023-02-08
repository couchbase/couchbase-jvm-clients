/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
// [start:3.4.1]
import com.couchbase.client.java.kv.ScanResult;
// [end:3.4.1]
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.management.collection.query.Command;
import com.couchbase.client.protocol.shared.Exception;
import com.couchbase.stream.FluxStreamer;
import com.couchbase.utils.ClusterConnection;
import com.google.protobuf.ByteString;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.couchbase.JavaSdkCommandExecutor.*;
import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN;


/**
 * SdkOperation performs each requested SDK operation
 */
public class ReactiveJavaSdkCommandExecutor extends SdkCommandExecutor {
    private final ClusterConnection connection;
    private final ConcurrentHashMap<String, RequestSpan> spans;

    public ReactiveJavaSdkCommandExecutor(ClusterConnection connection, Counters counters, ConcurrentHashMap<String, RequestSpan> spans) {
        super(counters);
        this.connection = connection;
        this.spans = spans;
    }

    @Override
    protected com.couchbase.client.protocol.run.Result performOperation(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        return performOperationReactive(op, perRun).block();
    }

    private Mono<Result> performOperationReactive(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        return Mono.defer(() -> {
            var result = com.couchbase.client.protocol.run.Result.newBuilder();

            if (op.hasInsert()) {
                var request = op.getInsert();
                var collection = connection.collection(request.getLocation()).reactive();
                var content = content(request.getContent());
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.insert(docId, content);
                else mr = collection.insert(docId, content, options);
                return mr.map(r -> {
                    result.setElapsedNanos(System.nanoTime() - start);
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasGet()) {
                var request = op.getGet();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<GetResult> gr;
                if (options == null) gr = collection.get(docId);
                else gr = collection.get(docId, options);
                return gr.map(r -> {
                    result.setElapsedNanos(System.nanoTime() - start);
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasRemove()) {
                var request = op.getRemove();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.remove(docId);
                else mr = collection.remove(docId, options);
                result.setElapsedNanos(System.nanoTime() - start);
                return mr.map(r -> {
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasReplace()) {
                var request = op.getReplace();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                var content = content(request.getContent());
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.replace(docId, content);
                else mr = collection.replace(docId, content, options);
                return mr.map(r -> {
                    result.setElapsedNanos(System.nanoTime() - start);
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            } else if (op.hasUpsert()) {
                var request = op.getUpsert();
                var collection = connection.collection(request.getLocation()).reactive();
                var docId = getDocId(request.getLocation());
                var options = createOptions(request, spans);
                var content = content(request.getContent());
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Mono<MutationResult> mr;
                if (options == null) mr = collection.upsert(docId, content);
                else mr = collection.upsert(docId, content, options);
                result.setElapsedNanos(System.nanoTime() - start);
                return mr.map(r -> {
                    if (op.getReturnResult()) populateResult(result, r);
                    else setSuccess(result);
                    return result.build();
                });
            // [start:3.4.1]
            } else if (op.hasRangeScan()) {
                var request = op.getRangeScan();
                var collection = connection.collection(request.getCollection()).reactive();
                var options = createOptions(request, spans);
                var scanType = convertScanType(request);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Flux<ScanResult> results;
                if (options != null) results = collection.scan(scanType, options);
                else results = collection.scan(scanType);
                result.setElapsedNanos(System.nanoTime() - start);
                var streamer = new FluxStreamer<ScanResult>(results, perRun, request.getStreamConfig().getStreamId(), request.getStreamConfig(),
                        (ScanResult r) -> processScanResult(request, r),
                        (Throwable err) -> convertException(err));
                perRun.streamerOwner().addAndStart(streamer);
                result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                        .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                                .setType(STREAM_KV_RANGE_SCAN)
                                .setStreamId(streamer.streamId())));
                return Mono.just(result.build());
                // [end:3.4.1]
            } else if (op.hasCreatePrimaryIndex()) {
                var request = op.getCreatePrimaryIndex();
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                if (options == null) connection.cluster().reactive().queryIndexes().createPrimaryIndex(request.getBucketName()).block();
                else connection.cluster().reactive().queryIndexes().createPrimaryIndex(request.getBucketName(), options).block();
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return Mono.just(result.build());
            } else if (op.hasCreateIndex()) {
                var request = op.getCreateIndex();
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                var fields = request.getFieldsList().stream().map(f -> f.toString()).collect(Collectors.toSet());
                if (options == null) connection.cluster().reactive().queryIndexes().createIndex(request.getBucketName(), request.getIndexName(), fields).block();
                else connection.cluster().reactive().queryIndexes().createIndex(request.getBucketName(),request.getIndexName(), fields, options).block();
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return Mono.just(result.build());
            } else if (op.hasGetAllIndexes()) {
                var request = op.getGetAllIndexes();
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                Flux<QueryIndex> indexes;
                if (options == null) indexes = connection.cluster().reactive().queryIndexes().getAllIndexes(request.getBucketName());
                else  indexes = connection.cluster().reactive().queryIndexes().getAllIndexes(request.getBucketName(), options);
                result.setElapsedNanos(System.nanoTime() - start);
                if (op.getReturnResult()) populateResult(result, Objects.requireNonNull(indexes.collectList().block(Duration.ofSeconds(10))));
                else setSuccess(result);
                return Mono.just(result.build());
            } else if (op.hasDropPrimaryIndex()) {
                var request = op.getDropPrimaryIndex();
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                if (options == null) connection.cluster().reactive().queryIndexes().dropPrimaryIndex(request.getBucketName()).block();
                else connection.cluster().reactive().queryIndexes().dropPrimaryIndex(request.getBucketName(), options).block();
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return Mono.just(result.build());
            } else if (op.hasDropIndex()) {
                var request = op.getDropIndex();
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                if (options == null) connection.cluster().reactive().queryIndexes().dropIndex(request.getBucketName(), request.getIndexName()).block();
                else connection.cluster().queryIndexes().reactive().dropIndex(request.getBucketName(), request.getIndexName(), options).block();
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return Mono.just(result.build());
            } else if (op.hasWatchIndexes()) {
                var request = op.getWatchIndexes();
                var options = createOptions(request);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                if (options == null) connection.cluster().reactive().queryIndexes().watchIndexes(request.getBucketName(), request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs())).block();
                else connection.cluster().reactive().queryIndexes().watchIndexes(request.getBucketName(), request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()), options).block();
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return Mono.just(result.build());
            } else if (op.hasBuildDeferredIndexes()) {
                var request = op.getBuildDeferredIndexes();
                var options = createOptions(request, spans);
                result.setInitiated(getTimeNow());
                long start = System.nanoTime();
                if (options == null) connection.cluster().reactive().queryIndexes().buildDeferredIndexes(request.getBucketName()).block();
                else connection.cluster().reactive().queryIndexes().buildDeferredIndexes(request.getBucketName(), options).block();
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return Mono.just(result.build());
            // [start:3.4.3]
            } else if (op.hasCollectionQueryIndexManager()) {
                com.couchbase.client.protocol.sdk.management.collection.query.Command command = op.getCollectionQueryIndexManager();

                return handleCollectionQueryIndexManagerCommand(op, result, command);
            // [end:3.4.3]
            } else {
                return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
            }
        });
    }

    // [start:3.4.3]
    private Mono<Result> handleCollectionQueryIndexManagerCommand(com.couchbase.client.protocol.sdk.Command op, Result.Builder result, Command command) {
        if (command.hasCreatePrimaryIndex()) {
            var request = command.getCreatePrimaryIndex();
            var collection = connection.collection(request.getCollection()).reactive();
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Mono<Void> res;
            if (options == null) res = collection.queryIndexes().createPrimaryIndex();
            else res = collection.queryIndexes().createPrimaryIndex(options);
            return res.then(Mono.fromCallable(() -> {
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return result.build();
            }));
        } else if (command.hasCreateIndex()) {
            var request = command.getCreateIndex();
            var collection = connection.collection(request.getCollection()).reactive();
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            var fields = new HashSet<>(request.getFieldsList());
            Mono<Void> res;
            if (options == null) res = collection.queryIndexes().createIndex(request.getIndexName(), fields);
            else res = collection.queryIndexes().createIndex(request.getIndexName(), fields, options);
            return res.then(Mono.fromCallable(() -> {
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return result.build();
            }));
        } else if (command.hasGetAllIndexes()) {
            var request = command.getGetAllIndexes();
            var collection = connection.collection(request.getCollection()).reactive();
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Flux<QueryIndex> indexes;
            if (options == null) indexes = collection.queryIndexes().getAllIndexes();
            else indexes = collection.queryIndexes().getAllIndexes(options);
            return indexes.collectList().map(i -> {
                result.setElapsedNanos(System.nanoTime() - start);
                if (op.getReturnResult()) populateResult(result, i);
                else setSuccess(result);
                return result.build();
            });
        } else if (command.hasDropPrimaryIndex()) {
            var request = command.getDropPrimaryIndex();
            var collection = connection.collection(request.getCollection()).reactive();
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Mono<Void> res;
            if (options == null) res = collection.queryIndexes().dropPrimaryIndex();
            else res = collection.queryIndexes().dropPrimaryIndex(options);
            return res.then(Mono.fromCallable(() -> {
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return result.build();
            }));
        } else if (command.hasDropIndex()) {
            var request = command.getDropIndex();
            var collection = connection.collection(request.getCollection()).reactive();
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Mono<Void> res;
            if (options == null) res = collection.queryIndexes().dropIndex(request.getIndexName());
            else res = collection.queryIndexes().dropIndex(request.getIndexName(), options);
            return res.then(Mono.fromCallable(() -> {
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return result.build();
            }));
        } else if (command.hasWatchIndexes()) {
            var request = command.getWatchIndexes();
            var collection = connection.collection(request.getCollection()).reactive();
            var options = createOptions(request);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Mono<Void> res;
            if (options == null) res = collection.queryIndexes().watchIndexes(request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()));
            else res = collection.queryIndexes().watchIndexes(request.getIndexNamesList().stream().toList(), Duration.ofMillis(request.getTimeoutMsecs()), options);
            return res.then(Mono.fromCallable(() -> {
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return result.build();
            }));
        } else if (command.hasBuildDeferredIndexes()) {
            var request = command.getBuildDeferredIndexes();
            var collection = connection.collection(request.getCollection()).reactive();
            var options = createOptions(request, spans);
            result.setInitiated(getTimeNow());
            long start = System.nanoTime();
            Mono<Void> res;
            if (options == null) res = collection.queryIndexes().buildDeferredIndexes();
            else res = collection.queryIndexes().buildDeferredIndexes(options);
            return res.then(Mono.fromCallable(() -> {
                result.setElapsedNanos(System.nanoTime() - start);
                setSuccess(result);
                return result.build();
            }));
        }
        else {
            return Mono.error(new UnsupportedOperationException());
        }
    }
    // [end:3.4.3]

    @Override
    protected Exception convertException(Throwable raw) {
        return convertExceptionShared(raw);
    }
}
