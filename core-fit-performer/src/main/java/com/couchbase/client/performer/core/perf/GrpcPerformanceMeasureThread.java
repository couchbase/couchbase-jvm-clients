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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Measures the performance of GRPC.
 */
public class GrpcPerformanceMeasureThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(GrpcPerformanceMeasureThread.class);
    private final AtomicInteger enqueued = new AtomicInteger();
    private final AtomicInteger ignored = new AtomicInteger();
    private final AtomicInteger sentSingle = new AtomicInteger();
    private final AtomicInteger sentBatchComplete = new AtomicInteger();
    private final AtomicInteger sentBatchIncomplete = new AtomicInteger();
    private static final double CHECK_EVERY_X_SECONDS = 5.0;

    public GrpcPerformanceMeasureThread() {
        super("perf-grpc-measure");
    }

    @Override
    public void run() {
        logger.info("GRPC performance monitoring thread started");
        long start = System.nanoTime();
        long sentSingleTotal = 0;
        long sentBatchTotal = 0;
        long enqueuedTotal = 0;

        try {
            while (!isInterrupted()) {
                try {
                    Thread.sleep((int) CHECK_EVERY_X_SECONDS * 1000);
                } catch (InterruptedException e) {
                    break;
                }

                double totalTimeSecs = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

                // Not getting these atomically, won't matter too much
                var sentSingleFrozen = sentSingle.get();
                var sentBatchCompleteFrozen = sentBatchComplete.get();
                var sentBatchIncompleteFrozen = sentBatchIncomplete.get();
                var enqueuedFrozen = enqueued.get();
                var ignoredFrozen = ignored.get();

                sentSingleTotal += sentSingleFrozen;
                sentBatchTotal += sentBatchTotal;
                enqueuedTotal += enqueuedFrozen;

                // "~" since we do lose a few operations here and there, due to not wanting to make this class totally
                // atomic and hence possibly impact performance
                logger.info("Performer sent in last {} seconds: ~{} batches ({} complete + {} incomplete), throughput ~{} ops/sec.  And enqueued ~{} ops/sec{}.  In total it has sent ~{} ops, with overall throughput: ~{} ops/sec, and enqueued ~{}, leaving ~{} on queue",
                        CHECK_EVERY_X_SECONDS,
                        sentBatchCompleteFrozen + sentBatchIncompleteFrozen,
                        sentBatchCompleteFrozen,
                        sentBatchIncompleteFrozen,
                        (int) (sentSingle.get() / CHECK_EVERY_X_SECONDS),
                        (int) (enqueuedFrozen / CHECK_EVERY_X_SECONDS),
                        ignoredFrozen == 0 ? "" : " and ignored " + ignoredFrozen,
                        sentSingleTotal,
                        (int) (sentSingleTotal / totalTimeSecs),
                        enqueuedTotal,
                        enqueuedTotal - sentSingleTotal);

                // Technically we lose a few operations here
                sentSingle.set(0);
                sentBatchIncomplete.set(0);
                sentBatchComplete.set(0);
                enqueued.set(0);
                ignored.set(0);
            }
        } finally {
            logger.info("GRPC performance monitoring thread stopped");
        }
    }

    public void sentOne() {
        sentSingle.incrementAndGet();
    }

    public void sentBatch(int batchSize, int maxBatchSize) {
        sentSingle.addAndGet(batchSize);
        if (batchSize == maxBatchSize) {
            sentBatchComplete.incrementAndGet();
        }
        else {
            sentBatchIncomplete.incrementAndGet();
        }
    }

    public void enqueued() {
        enqueued.incrementAndGet();
    }

    public void ignored() {
        ignored.incrementAndGet();
    }
}
