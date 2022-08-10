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
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.PerRun;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.shared.Exception;
import com.couchbase.stream.FluxStreamer;
import com.couchbase.utils.ClusterConnection;
import com.google.protobuf.ByteString;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;

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
                        (ScanResult r) -> {
                            return processScanResult(request, r);
                        });
                perRun.streamerOwner().addAndStart(streamer);
                result.setStream(com.couchbase.client.protocol.streams.Signal.newBuilder()
                        .setCreated(com.couchbase.client.protocol.streams.Created.newBuilder()
                                .setType(STREAM_KV_RANGE_SCAN)
                                .setStreamId(streamer.streamId())));
                return Mono.just(result.build());
            // [end:3.4.1]
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