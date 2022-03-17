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

package com.couchbase.client.core.transaction.log;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A logger optimized for logging transactions-specific info.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
@Stability.Internal
public class CoreTransactionLogger {
    @Nullable private final EventBus eventBus;
    // We use a thread-safe list to allow safe logging during concurrent ops.  This list is 'practically lock-free' and
    // the performance should not be a factor.
    private final ConcurrentLinkedQueue<TransactionLogEvent> logs = new ConcurrentLinkedQueue<>();
    private final String transactionId;

    public CoreTransactionLogger(@Nullable EventBus eventBus,
                                 String primaryId) {
        this.eventBus = eventBus;
        this.transactionId = Objects.requireNonNull(primaryId);
    }

    public void debug(Throwable err) {
        log(err, Event.Severity.DEBUG);
    }

    public void debug(String secondaryId, Throwable err) {
        log(secondaryId, err, Event.Severity.DEBUG);
    }

    public void info(String secondaryId, Throwable err) {
        log(secondaryId, err, Event.Severity.INFO);
    }

    public void log(String secondaryId, Throwable err, Event.Severity level) {
        String st = SimpleEventBusLogger.stackTraceToString(err);
        log(secondaryId, st, level);
    }

    public void log(Throwable err, Event.Severity level) {
        String st = SimpleEventBusLogger.stackTraceToString(err);
        log(st, level);
    }

    public void logDefer(String secondaryId, String fmt, Event.Severity level, Object... values) {
        TransactionLogEvent defer = new TransactionLogEvent(System.currentTimeMillis(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName(),
                transactionId,
                secondaryId,
                fmt,
                values,
                level);
        logs.add(defer);
    }

    public void logDefer(String secondaryId, String fmt, Event.Severity level) {
        TransactionLogEvent defer = new TransactionLogEvent(System.currentTimeMillis(),
                Thread.currentThread().getId(),
                Thread.currentThread().getName(),
                transactionId,
                secondaryId,
                fmt,
                null,
                level);
        logs.add(defer);
    }

    public void log(String secondaryId, String value, Event.Severity level) {
        logDefer(secondaryId, value, level);
    }

    public void log(String value, Event.Severity level) {
        logDefer(null, value, level);
    }

    public void error(String secondaryId, String value) {
        log(secondaryId, value, Event.Severity.ERROR);
    }

    public void warn(String secondaryId, String value) {
        log(secondaryId, value, Event.Severity.WARN);
    }

    public void warn(String secondaryId, String fmt, Object... values) {
        logDefer(secondaryId, fmt, Event.Severity.WARN, values);
    }

    public void info(String secondaryId, String value) {
        log(secondaryId, value, Event.Severity.INFO);
    }

    public void debug(String secondaryId, String value) {
        log(secondaryId, value, Event.Severity.DEBUG);
    }

    public void debug(String secondaryId, String fmt, Object... values) {
        logDefer(secondaryId, fmt, Event.Severity.DEBUG, values);
    }

    public void trace(String secondaryId, String value) {
        log(secondaryId, value, Event.Severity.VERBOSE);
    }

    public void error(String value) {
        log(value, Event.Severity.ERROR);
    }

    public void warn(String value) {
        log(value, Event.Severity.WARN);
    }

    public void info(String value) {
        log(value, Event.Severity.INFO);
    }

    public void debug(String value) {
        log(value, Event.Severity.DEBUG);
    }

    public void trace(String value) {
        log(value, Event.Severity.VERBOSE);
    }

    public void trace(String secondaryId, String fmt, Object... values) {
        logDefer(secondaryId, fmt, Event.Severity.VERBOSE, values);
    }

    public void info(String secondaryId, String fmt, Object... values) {
        logDefer(secondaryId, fmt, Event.Severity.INFO, values);
    }

    public void info(String fmt, Object... values) {
        logDefer(null, fmt, Event.Severity.INFO, values);
    }

    public List<TransactionLogEvent> logs() {
        return new ArrayList<>(logs);
    }

    public @Nullable EventBus eventBus() {
        return eventBus;
    }
}

