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
package com.couchbase.client.core.transaction.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.transaction.AttemptExpiredException;
import com.couchbase.client.core.error.transaction.TransactionOperationFailedException;
import com.couchbase.client.core.transaction.AccessorUtil;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.Builder.createError;

/**
 * Provides locking functionality in line with Go's WaitGroups, in a reactive compatible way.
 */
@Stability.Internal
public class ReactiveWaitGroup {
    public static class Waiter {
        public final Sinks.One<Void> notifier = Sinks.one();
        public final String dbg;

        public Waiter(String dbg) {
            this.dbg = Objects.requireNonNull(dbg);
        }
    }

    private final CoreTransactionAttemptContext ctx;
    private final ArrayList<Waiter> waiting = new ArrayList<>();
    private final boolean debugMode;

    public ReactiveWaitGroup(CoreTransactionAttemptContext ctx, boolean debugMode) {
        this.debugMode = debugMode;
        this.ctx = Objects.requireNonNull(ctx);
    }

    public synchronized int waitingCount() {
        return waiting.size();
    }

    public Mono<Waiter> add(String dbg) {
        return Mono.defer(() -> {
            Waiter waiter = new Waiter(dbg);

            synchronized (this) {
                waiting.add(waiter);
                if (debugMode) {
                    ctx.logger().info(ctx.attemptId(), "WG: adding [{}], {} now in waiting", dbg, waiting.size());
                }
            }

            return Mono.just(waiter);
        });
    }

    public Mono<Void> done(Waiter waiter) {
        return Mono.defer(() -> {
            Sinks.One<Void> notifier = null;

            synchronized (this) {
                if (!waiting.remove(waiter)) {
                    // We allow this, to permit a defensive form of programming where the waiter is removed both when it's meant to be, and on any error
                    // Can also get here if a concurrent op is being cancelled
                    if (debugMode) {
                        ctx.logger().info(ctx.attemptId(), "WG: wanted to remove [{}] from waiters but it's not in there", waiter.dbg);
                    }
                } else {
                    if (debugMode) {
                        ctx.logger().info(ctx.attemptId(), "WG: [{}] is done, {} now in waiting", waiter.dbg, waiting.size());
                    }
                    notifier = waiter.notifier;
                }
            }

            // Signal the awaiter, making sure to do it outside synchronization for safety.
            if (notifier != null) {
                notifier.tryEmitEmpty().orThrow();
            }

            return Mono.empty();
        });
    }

    public Mono<Void> await(Duration timeout) {
        return Mono.defer(() -> {
            List<Waiter> waiters;
            synchronized (this) {
                // We don't track if we're already waiting as a) it's legal to have multiple awaiters, and b) this
                // can happen as we can have multiple failing ops waiting for the other ops before continuing (but after
                // removing themselves from wait group).

                waiters = new ArrayList<>(waiting);
            }

            // Making sure to do this outside synchronization for safety.
            return Flux.merge(waiters.stream()
                            .map(v -> v.notifier.asMono())
                            .collect(Collectors.toList()))

                    .timeout(timeout)
                    .publishOn(ctx.scheduler()) // timeout is on parallel
                    .onErrorResume(err -> {
                        if (err instanceof TimeoutException) {
                            String msg = String.format("Attempt expired while waiting for %d - %s", waiters.size(),
                                    waiters.stream().map(v -> v.dbg).collect(Collectors.joining(",")));
                            if (debugMode) {
                                ctx.logger().info(ctx.attemptId(), msg);
                            }
                            return Mono.error(AccessorUtil.operationFailed(ctx, createError()
                                    .raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED)
                                    .doNotRollbackAttempt()
                                    .cause(new AttemptExpiredException(msg, err)).build()));
                        } else {
                            return Mono.error(err);
                        }
                    })
                    .then();
        });
    }
}
