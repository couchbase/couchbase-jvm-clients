/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.performer.core.commands;

import com.couchbase.client.protocol.transactions.CommandBatch;
import com.couchbase.client.protocol.transactions.TransactionCommand;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class BatchExecutor {
    public record CommandInBatch(long threadIdx, TransactionCommand command) {}

    public static void performCommandBatchBlocking(Logger logger, CommandBatch request, Consumer<CommandInBatch> call) {
        logger.info("Running {} operations, concurrency={}", request.getCommandsCount(), request.getParallelism());

        var threads = new ArrayList<Thread>(request.getParallelism());
        var thrownError = new AtomicReference<RuntimeException>();
        var commandIdx = new AtomicInteger();

        for (int threadIdx = 0; threadIdx < request.getParallelism(); threadIdx++) {
            final Long x = (long) threadIdx;

            var thread = new Thread(() -> {
                logger.info("{} thread started", x);
                while (true) {
                    var nextCommandIdx = commandIdx.getAndIncrement();
                    if (nextCommandIdx < request.getCommandsCount()) {
                        var command = request.getCommands(nextCommandIdx);
                        try {
                            call.accept(new CommandInBatch(x, command));
                            logger.info("{} A parallel op {} has finished", x, command.getCommandCase());
                        } catch (Throwable err) {
                            logger.info("{} A parallel op {} has errored with {}", x, command.getCommandCase(), err.toString());
                            // Only store the first thrown error
                            if (err instanceof RuntimeException) {
                                thrownError.compareAndSet(null, (RuntimeException) err);
                            } else {
                                thrownError.compareAndSet(null, new RuntimeException(err));
                            }
                            break;
                        }
                    }
                    else {
                        break;
                    }
                }
                logger.info("{} thread finished", x);
            });

            threads.add(thread);
        }

        threads.forEach(Thread::start);
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            if (thrownError.get() != null) {
                logger.info("Rethrowing error {}", thrownError.get().toString());
                throw thrownError.get();
            }
        });

        if (thrownError.get() == null) {
            logger.info("Reached end of operations with nothing throwing");
        }
    }

}
