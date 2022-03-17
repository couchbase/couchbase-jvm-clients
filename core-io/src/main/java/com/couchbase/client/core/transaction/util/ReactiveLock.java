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
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static com.couchbase.client.core.error.transaction.TransactionOperationFailedException.Builder.createError;

/**
 * A mutex pessimistic lock, compatible with reactive.
 */
@Stability.Internal
public class ReactiveLock {
    public static class Waiter {
        private final Sinks.One<Waiter> notifier = Sinks.one();
        public final String dbg;

        public Waiter(String dbg) {
            this.dbg = Objects.requireNonNull(dbg);
        }
    }

    private final CoreTransactionAttemptContext ctx;
    private final ArrayList<Waiter> waiting = new ArrayList<>();
    private final boolean debugMode;
    private @Nullable
    Waiter lockedBy = null;
    private final boolean debugAsSingleThreaded = Boolean.parseBoolean(System.getProperty("com.couchbase.transactions.lockDebugAsSingleThreaded", "false"));

    public ReactiveLock(CoreTransactionAttemptContext ctx, boolean debugMode) {
        this.debugMode = debugMode;
        this.ctx = Objects.requireNonNull(ctx);
    }

    /**
     * if the lock is unlocked, lock it, and continue
     * else if the lock is locked, join the list of things waiting for it to be unlocked
     */
    public Mono<Waiter> lock(String dbg, Duration timeout) {
        return Mono.defer(() -> {
            Waiter waiter = new Waiter(dbg);

            synchronized (this) {
                if (debugAsSingleThreaded && isLocked()) {
                    String msg = "LOCK: Internal bug: [" + dbg + "] needs to lock mutex, which is already locked";
                    ctx.logger().info(ctx.attemptId(), msg);
                    throw new IllegalStateException(msg);
                }

                if (lockedBy == null) {
                    if (debugMode) {
                        ctx.logger().info(ctx.attemptId(), String.format("LOCK: [%s] is locking, %d waiting", waiter.dbg, waiting.size()));
                    }
                    lockedBy = waiter;
                    return Mono.just(waiter);
                } else if (lockedBy == waiter) {
                    String msg = String.format("LOCK: internal bug [%s] wants a lock currently held by itself", dbg);
                    ctx.logger().info(ctx.attemptId(), msg);
                    throw new IllegalStateException(msg);
                } else {
                    if (debugMode) {
                        ctx.logger().info(ctx.attemptId(), String.format("LOCK: [%s] will wait for lock currently held by [%s], %d other waiters",
                                dbg, lockedBy.dbg, waiting.size()));
                    }
                    waiting.add(waiter);
                }
            }

            // Have not locked, are waiting
            // Making sure to do it outside synchronization for safety.
            return waiter.notifier.asMono()
                    .publishOn(SchedulerUtil.scheduler)

                    .timeout(timeout)
                    .publishOn(SchedulerUtil.scheduler) // timeout happens on parallel scheduler
                    .onErrorResume(err -> {
                        if (err instanceof TimeoutException) {
                            // timeout is only set if we do not immediately hold the lock after .lock() ends
                            String msg = String.format("Attempt expired while [%s] waiting for lock on timeout of %sms, lock currently held by [%s], %d other waiters",
                                    dbg, timeout.toMillis(), lockedBy == null ? "none" : lockedBy.dbg, waiting.size());
                            if (ctx != null) {
                                ctx.logger().info(ctx.attemptId(), msg);
                                return Mono.error(AccessorUtil.operationFailed(ctx, createError()
                                        .raiseException(TransactionOperationFailedException.FinalErrorToRaise.TRANSACTION_EXPIRED)
                                        .doNotRollbackAttempt()
                                        .cause(new AttemptExpiredException(msg, err)).build()));
                            } else {
                                // Just used for tests
                                return Mono.error(new AttemptExpiredException("Expired " + dbg, err));
                            }
                        } else {
                            return Mono.error(err);
                        }
                    })
                    .doFinally(v -> {
                        // Thread safety 7.6 and 7.8
                        // CANCEL signal should not arrive anymore following MonoBridge.  This is probably indicative of an internal bug.
                        // Thread safety 10.2: it is
                        if (v == SignalType.CANCEL) {
                            ctx.logger().info(ctx.attemptId(), "cancel signal while waiter %s is waiting for lock", dbg);

                            // Thread safety 7.6: removeFromWaiters on CANCEL because this operation is dying and does not want to have or be given the lock
                            unlock(waiter, "onCancel", true).block();
                        }
                    });
        });
    }

    /**
     * The thing currently waiting on this lock is now unlocking it.
     * Let one (and only one) thing waiting on this lock, continue.
     * <p>
     * It's reactive style as always need to do a .timeout() after it
     */
    public Mono<Void> unlock(Waiter waiter) {
        return unlock(waiter, null);
    }

    public Mono<Void> unlock(Waiter waiter, @Nullable String extraDbg) {
        return unlock(waiter, extraDbg, false);
    }

    public Mono<Void> unlock(Waiter waiter, @Nullable String extraDbg, boolean removeFromWaiters) {
        return Mono.defer(() -> {
            Waiter next = null;

            synchronized (this) {
                if (waiter == null) {
                    ctx.logger().info(ctx.attemptId(), "LOCK: internal bug, waiter is null %s", extraDbg);
                }
                if (lockedBy != waiter) {
                    // Allowing double-unlocks, to permit a paranoid style of coding that both unlocks where it's expected to, and in a doFinally as a safety precaution
                    // Can also get here if a concurrent op is being cancelled

                    if (removeFromWaiters) {
                        waiting.remove(waiter);
                        if (debugMode) {
                            String msg = String.format("LOCK: [%s: %s] is unlocking, but does not have the lock - removing from waiters, leaving %d others", waiter == null ? "-" : waiter.dbg, extraDbg == null ? "-" : extraDbg, waiting.size());
                            ctx.logger().info(ctx.attemptId(), msg);
                        }
                    } else {
                        if (debugMode) {
                            String msg = String.format("LOCK: [%s: %s] is unlocking, but does not have the lock", waiter == null ? "-" : waiter.dbg, extraDbg == null ? "-" : extraDbg);
                            ctx.logger().info(ctx.attemptId(), msg);
                        }
                    }
                    return Mono.empty();
                } else {
                    if (!waiting.isEmpty()) {
                        next = waiting.remove(0);
                        if (debugMode) {
                            ctx.logger().info(ctx.attemptId(), String.format("LOCK: [%s: %s] is unlocking, [%s] now has lock, %d left waiting",
                                    waiter == null ? "-" : waiter.dbg, extraDbg == null ? "-" : extraDbg, next.dbg, waiting.size()));
                        }
                        lockedBy = next;
                    } else {
                        lockedBy = null;
                        if (debugMode) {
                            ctx.logger().info(ctx.attemptId(), String.format("LOCK: [%s: %s] is unlocking, nothing waiting",
                                    waiter == null ? "-" : waiter.dbg, extraDbg == null ? "-" : extraDbg));
                        }
                    }
                }
            }

            if (next != null) {
                // Essential to emit a value here, since lockers are waiting for onNext - as they need to use the lockToken
                // Making sure to do it outside synchronization for safety.
                next.notifier.tryEmitValue(next).orThrow();
            }

            return Mono.empty();
        });
    }

    public boolean debugAsSingleThreaded() {
        return debugAsSingleThreaded;
    }

    public synchronized boolean isLocked() {
        return lockedBy != null;
    }
}
