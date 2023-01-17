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
package com.couchbase.client.performer.core.perf;

import com.couchbase.client.protocol.run.BatchedResult;
import com.couchbase.client.protocol.run.Config;
import com.couchbase.client.protocol.run.Result;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Streams back a workload result to the driver.
 * This was done because the response observer on the driver is not thread safe so couldn't handle multiple messages
 * at the same time.
 */
public class WorkloadStreamingThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(WorkloadStreamingThread.class);
    private final ServerCallStreamObserver<Result> responseObserver;
    private final ConcurrentLinkedQueue<Result> writeQueue = new ConcurrentLinkedQueue<>();
    private final Config config;
    private final GrpcPerformanceMeasureThread grpcPerformance = new GrpcPerformanceMeasureThread();

    public WorkloadStreamingThread(StreamObserver<Result> responseObserver, Config config) {
        super("perf-workload-streaming");
        this.responseObserver = (ServerCallStreamObserver<Result>) responseObserver;
        this.config = config;
        this.grpcPerformance.start();
    }

    public void enqueue(Result result) {
        if (isInterrupted()) {
            grpcPerformance.ignored();
            return;
        }
        writeQueue.add(result);
        grpcPerformance.enqueued();
    }

    @Override
    public void run() {
        try {
            while (!isInterrupted()) {
                flush();
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException ignored) {
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Error sending performance data to driver", e);
            // Important to tell the driver something has gone badly wrong otherwise it'll hang
            responseObserver.onError(Status.ABORTED.withDescription(e.toString()).asException());
        }

        logger.info("Writer thread has been stopped, performing final flush (currently {} on queue)", writeQueue.size());

        flush();

        grpcPerformance.interrupt();
    }

    private void flush() {
        if (config.hasStreamingConfig()) {
            var sc = config.getStreamingConfig();
            if (sc.hasBatchSize()) {
                flushBatch(sc.getBatchSize());
            } else {
                flushIndividual(!sc.getFlowControl());
            }
        }
        else {
            flushIndividual(true);
        }
    }

    private void flushIndividual(boolean asFastAsPossible) {
        while (!writeQueue.isEmpty()) {

            if (asFastAsPossible || responseObserver.isReady()) {

                var next = writeQueue.poll();
                if (next != null) {
                    try {
                        responseObserver.onNext(next);
                    } catch (RuntimeException err) {
                        logger.warn("Failed to write {}: {}", next, err.toString());
                        // Important to tell the driver something has gone badly wrong otherwise it'll hang
                        responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
                    }

                    grpcPerformance.sentOne();
                } else {
                    logger.warn("Got null element from queue");
                }
            }
        }
    }

    private void flushBatch(int opsInBatch) {
        boolean asFastAsPossible = config.hasStreamingConfig() ? !config.getStreamingConfig().getFlowControl() : true;

        while (!writeQueue.isEmpty()) {
            boolean canSend = asFastAsPossible || responseObserver.isReady();

            if (canSend) {
                var batch = new ArrayList<Result>(opsInBatch);

                for (int i = 0; i < opsInBatch; i++) {
                    var next = writeQueue.poll();
                    if (next != null) {
                        batch.add(next);
                    } else {
                        break;
                    }
                }

                try {
                    responseObserver.onNext(Result.newBuilder()
                            .setBatched(BatchedResult.newBuilder()
                                    .addAllResult(batch))
                            .build());
                    grpcPerformance.sentBatch(batch.size(), opsInBatch);
                } catch (RuntimeException err) {
                    logger.warn("Failed to write batch: {}", err.toString());
                    // Important to tell the driver something has gone badly wrong otherwise it'll hang
                    responseObserver.onError(Status.ABORTED.withDescription(err.toString()).asException());
                }
            }
            else {
                try {
                    // Wait a brief period for the responseObserver to be ready.
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    // Unclear where the interruption is coming from on CI, but better to end gracefully with a warning than die
                    logger.warn("Interrupted while waiting for responseObserver to be ready");
                }
            }
        }
    }
}
