/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.performer.core;

import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor;
import com.couchbase.client.performer.core.metrics.MetricsReporter;
import com.couchbase.client.performer.core.perf.Counters;
import com.couchbase.client.performer.core.perf.HorizontalScalingThread;
import com.couchbase.client.performer.core.perf.WorkloadStreamingThread;
import com.couchbase.client.performer.core.perf.WorkloadsRunner;
import com.couchbase.client.protocol.PerformerServiceGrpc;
import com.couchbase.client.protocol.performer.Caps;
import com.couchbase.client.protocol.performer.PerformerCapsFetchRequest;
import com.couchbase.client.protocol.performer.PerformerCapsFetchResponse;
import com.couchbase.client.protocol.shared.API;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

abstract public class CorePerformer extends PerformerServiceGrpc.PerformerServiceImplBase {
    abstract protected SdkCommandExecutor executor(com.couchbase.client.protocol.run.Workloads workloads, Counters counters);

    // Can return null if the performer does not support transactions.
    abstract protected @Nullable TransactionCommandExecutor transactionsExecutor(com.couchbase.client.protocol.run.Workloads workloads, Counters counters);

    abstract protected void customisePerformerCaps(PerformerCapsFetchResponse.Builder response);
    private final Logger logger = LoggerFactory.getLogger(CorePerformer.class);

    @Override
    public void performerCapsFetch(PerformerCapsFetchRequest request, StreamObserver<PerformerCapsFetchResponse> responseObserver) {
        var builder = PerformerCapsFetchResponse.newBuilder()
                .addSupportedApis(API.DEFAULT) // blocking only for now
                .addPerformerCaps(Caps.GRPC_TESTING)
                .addPerformerCaps(Caps.KV_SUPPORT_1);

        customisePerformerCaps(builder);

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void run(com.couchbase.client.protocol.run.Request request,
                                     StreamObserver<com.couchbase.client.protocol.run.Result> responseObserver) {
        try {
            if (!request.hasWorkloads()) {
                throw new UnsupportedOperationException("Not workloads");
            }

            var counters = new Counters();
            var executor = executor(request.getWorkloads(), counters);
            var transactionsExecutor = transactionsExecutor(request.getWorkloads(), counters);

            var writer = new WorkloadStreamingThread(responseObserver, request.getConfig());
            writer.start();

            MetricsReporter metrics = null;
            if (request.hasConfig()
                    && request.getConfig().hasStreamingConfig()
                    && request.getConfig().getStreamingConfig().getEnableMetrics()) {
                metrics = new MetricsReporter(writer);
                metrics.start();
            }

            WorkloadsRunner.run(request.getWorkloads(),
                    (x) -> writer.enqueue(x),
                    (x) -> new HorizontalScalingThread(x, executor, transactionsExecutor),
                    counters);

            if (metrics != null) {
                metrics.interrupt();
                metrics.join();
            }

            writer.interrupt();
            writer.join();

            responseObserver.onCompleted();
        }
        catch (UnsupportedOperationException err) {
            responseObserver.onError(Status.UNIMPLEMENTED.withDescription(err.toString()).asException());
        } catch (RuntimeException | InterruptedException err) {
            responseObserver.onError(Status.UNKNOWN.withDescription(err.toString()).asException());
        }
    }
}
