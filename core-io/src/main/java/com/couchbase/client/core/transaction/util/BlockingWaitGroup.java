/*
 * Copyright 2026 Couchbase, Inc.
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
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides locking functionality in line with Go's WaitGroups.
 */
@Stability.Internal
public class BlockingWaitGroup {

    @FunctionalInterface
    public interface TimeoutExceptionFactory {
        RuntimeException create(String message, TimeoutException cause);
    }

    @FunctionalInterface
    public interface WaitGroupLogger {
        void log(String format, Object... args);
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition noWaiters = lock.newCondition();
    private final AtomicInteger waiters = new AtomicInteger();
    private boolean closed;
    private final TimeoutExceptionFactory timeoutExceptionFactory;
    private final @Nullable WaitGroupLogger logger;
    private final boolean debugMode;

    public BlockingWaitGroup(TimeoutExceptionFactory timeoutExceptionFactory, @Nullable WaitGroupLogger logger, boolean debugMode) {
        this.timeoutExceptionFactory = Objects.requireNonNull(timeoutExceptionFactory);
        this.logger = logger;
        this.debugMode = debugMode;
    }

    public int waitingCount() {
        return waiters.get();
    }

    public boolean add(String dbg) {
        lock.lock();
        try {
            if (closed) {
                log("WG: rejecting add for [{}], wait group is closed", dbg);
                return false;
            }

            int newCount = waiters.incrementAndGet();
            log("WG: adding [{}], {} now in waiting", dbg, newCount);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void done() {
        lock.lock();
        try {
            int newCount = waiters.decrementAndGet();
            if (newCount < 0) {
                log("WG: internal error, probably a thread has called done() too many twice");
                waiters.set(0);
                throw new NoSuchElementException();
            }

            log("WG: an operation is done, {} now in waiting", newCount);

            if (newCount <= 0) {
                noWaiters.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void await(Duration timeout) {
        awaitInternal(timeout, false);
    }

    public void closeAndAwait(Duration timeout) {
        awaitInternal(timeout, true);
    }

    // Exposed for testing.
    public boolean isClosed() {
        // Lock to force a memory barrier as closed is not volatile.
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    private void awaitInternal(Duration timeout, boolean close) {
        Objects.requireNonNull(timeout, "timeout");

        long remainingNanos = timeout.toNanos();
        lock.lock();
        try {
            if (close) {
                closed = true;
            }

            while (waiters.get() > 0) {
                if (remainingNanos <= 0) {
                    throw timeoutException(waiters.get());
                }

                try {
                    // awaitNanos atomically releases the held lock so other threads can call done(), and
                    // acquires it again on returning.
                    remainingNanos = noWaiters.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for operations to finish", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private RuntimeException timeoutException(int remainingWaiters) {
        String msg = String.format("Timed out while waiting for %d waiters", remainingWaiters);
        log(msg);
        return timeoutExceptionFactory.create(msg, new TimeoutException(msg));
    }

    private void log(String format, Object... args) {
        if (debugMode && logger != null) {
            logger.log(format, args);
        }
    }
}
