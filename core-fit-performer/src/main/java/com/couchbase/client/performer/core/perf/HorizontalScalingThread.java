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

import com.couchbase.client.performer.core.bounds.BoundsExecutor;
import com.couchbase.client.performer.core.bounds.BoundsCounterBased;
import com.couchbase.client.performer.core.bounds.BoundsForTime;
import com.couchbase.client.performer.core.commands.SdkCommandExecutor;
import com.couchbase.client.performer.core.commands.TransactionCommandExecutor;
import com.couchbase.client.protocol.shared.Bounds;
import com.couchbase.client.protocol.transactions.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class HorizontalScalingThread extends Thread {
    private final Logger logger;
    private final SdkCommandExecutor sdkCommandExecutor;
    private final @Nullable TransactionCommandExecutor transactionsCommandExecutor;
    private final PerHorizontalScaling per;
    AtomicInteger operationsSuccessful = new AtomicInteger(0);
    AtomicInteger operationsFailed = new AtomicInteger(0);

    public HorizontalScalingThread(PerHorizontalScaling per,
                                   SdkCommandExecutor sdkCommandExecutor,
                                   @Nullable TransactionCommandExecutor transactionsCommandExecutor) {
        super("perf-runner");
        logger = LoggerFactory.getLogger("runner-" + per.runnerIndex());
        this.sdkCommandExecutor = sdkCommandExecutor;
        this.per = per;
        this.transactionsCommandExecutor = transactionsCommandExecutor;
    }

    private BoundsExecutor getBounds(boolean hasBounds, Bounds bounds) {
        if (!hasBounds) {
            return new BoundsCounterBased(new AtomicInteger(1));
        }

        if (bounds.hasCounter()) {
            var counter = per.counters().getCounter(bounds.getCounter());
            return new BoundsCounterBased(counter);
        }
        else if (bounds.hasForTime()) {
            return new BoundsForTime(Duration.ofSeconds(bounds.getForTime().getSeconds()));
        }
        else {
            throw new UnsupportedOperationException("Unknown bounds type");
        }
    }

    private void executeSdkWorkload(com.couchbase.client.protocol.sdk.Workload workload) {
        var bounds = getBounds(workload.hasBounds(), workload.getBounds());

        long executed = 0;
        while (bounds.canExecute()) {
            var nextCommand = workload.getCommand((int) (executed % workload.getCommandCount()));
            ++ executed;
            var result = sdkCommandExecutor.run(nextCommand);
            per.consumer().accept(result);
            if (result.hasSdk()) {
                if (result.getSdk().getSuccess()) {
                    operationsSuccessful.incrementAndGet();
                } else {
                    operationsFailed.incrementAndGet();
                }
            }
        }
    }

    private void executeTransactionWorkload(com.couchbase.client.protocol.transactions.Workload workload) {
        var bounds = getBounds(workload.hasBounds(), workload.getBounds());

        long executed = 0;
        while (bounds.canExecute()) {
            var nextCommand = workload.getCommand((int) (executed % workload.getCommandCount()));
            ++ executed;
            var result = transactionsCommandExecutor.run(nextCommand, workload.getPerformanceMode());
            per.consumer().accept(result);
            if (result.hasTransaction()) {
                if (result.getTransaction().getException() == TransactionException.NO_EXCEPTION_THROWN) {
                    operationsSuccessful.incrementAndGet();
                } else {
                    operationsFailed.incrementAndGet();
                }
            }
        }
    }

    private void executeGrpcWorkload(com.couchbase.client.protocol.meta.Workload workload) {
        var bounds = getBounds(workload.hasBounds(), workload.getBounds());

        while (bounds.canExecute()) {
            if (!workload.getCommand().hasPing()) {
                throw new UnsupportedOperationException("Unknown GRPC command type");
            }

            per.consumer().accept(com.couchbase.client.protocol.run.Result.newBuilder()
                    .setGrpc(com.couchbase.client.protocol.meta.Result.getDefaultInstance())
                    .build());

            operationsSuccessful.incrementAndGet();
        }
    }

    @Override
    public void run() {

        try {
            logger.info("Runner thread has started, will run {} workloads", per.perThread().getWorkloadsCount());

            for (var workload : per.perThread().getWorkloadsList()) {
                if (workload.hasSdk()) {
                    executeSdkWorkload(workload.getSdk());
                } else if (workload.hasTransaction()) {
                    executeTransactionWorkload(workload.getTransaction());
                } else if (workload.hasGrpc()) {
                    executeGrpcWorkload(workload.getGrpc());
                }
                else {
                    throw new UnsupportedOperationException();
                }
            }
        }
        catch (Throwable err) {
            logger.error("Runner thread died with {}", err.toString());
            System.exit(-1);
        }

        logger.info("Finished after {} successful operations and {} failed",
                operationsSuccessful, operationsFailed);
    }
}
