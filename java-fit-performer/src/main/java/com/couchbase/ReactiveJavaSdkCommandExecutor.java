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
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.shared.Exception;
import com.couchbase.query.QueryIndexManagerHelper;
import com.couchbase.search.SearchHelper;
import com.couchbase.stream.FluxStreamer;
import com.couchbase.utils.ClusterConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.JavaSdkCommandExecutor.content;
import static com.couchbase.JavaSdkCommandExecutor.convertExceptionShared;
import static com.couchbase.JavaSdkCommandExecutor.convertScanType;
import static com.couchbase.JavaSdkCommandExecutor.createOptions;
import static com.couchbase.JavaSdkCommandExecutor.populateResult;
import static com.couchbase.JavaSdkCommandExecutor.processScanResult;
import static com.couchbase.JavaSdkCommandExecutor.setSuccess;
import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.client.protocol.streams.Type.STREAM_KV_RANGE_SCAN;
import static com.couchbase.search.SearchHelper.handleSearchReactive;


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
    protected void performOperation(com.couchbase.client.protocol.sdk.Command op, PerRun perRun) {
        performOperationReactive(op, perRun)
                // A few operations (such as streaming) will intentionally not return a result, so they guarantee
                // the stream.Created is sent first.
                .doOnNext(result -> perRun.resultsStream().enqueue(result))
                .block();
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
            } else if (op.hasClusterCommand()) {
                var clc = op.getClusterCommand();
                var cluster = connection.cluster().reactive();

                if (clc.hasQueryIndexManager()) {
                    return QueryIndexManagerHelper.handleClusterQueryIndexManagerReactive(cluster, spans, op, result);
                } else if (clc.hasSearch()) {
                    // Streaming, so intentionally does not return a result.
                    return handleSearchReactive(connection.cluster(), null, spans, clc.getSearch(), perRun)
                            .ofType(Result.class);
                } else if (clc.hasSearchIndexManager()) {
                    // Skipping testing the reactive API as this is tertiary functionality, and the reactive API wraps the
                    // same underlying logic as the blocking API.
                    return Mono.fromSupplier(() -> SearchHelper.handleClusterSearchIndexManager(connection.cluster(), spans, op));
                } else throw new UnsupportedOperationException();
            } else if (op.hasScopeCommand()) {
                var slc = op.getScopeCommand();
                var scope = connection.cluster().bucket(slc.getScope().getBucketName()).scope(slc.getScope().getScopeName());

                if (slc.hasSearch()) {
                    return handleSearchReactive(connection.cluster(), scope, spans, slc.getSearch(), perRun)
                            .ofType(Result.class);
                    // [start:3.4.4]
                } else if (slc.hasSearchIndexManager()) {
                    return Mono.fromSupplier(() -> SearchHelper.handleScopeSearchIndexManager(scope, spans, op));
                    // [end:3.4.4]
                } else throw new UnsupportedOperationException();
            } else if (op.hasCollectionCommand()) {
                var clc = op.getCollectionCommand();
                var collection = connection.cluster()
                        .bucket(clc.getCollection().getBucketName())
                        .scope(clc.getCollection().getScopeName())
                        .collection(clc.getCollection().getCollectionName())
                        .reactive();

                if (clc.hasQueryIndexManager()) {
                    return QueryIndexManagerHelper.handleCollectionQueryIndexManagerReactive(collection, spans, op, result);
                }
                else throw new UnsupportedOperationException();
            } else {
                return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
            }
        });
    }

    @Override
    protected Exception convertException(Throwable raw) {
        return convertExceptionShared(raw);
    }
}
