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
package com.couchbase.client.core.cnc.events.transaction;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import reactor.util.annotation.Nullable;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

/**
 * A transaction log message.
 * <p>
 * Includes a timestamp as the usual flow is that all of the buffered logs for a transaction are being written by the
 * application at the same time.
 */
public class TransactionLogEvent extends TransactionEvent {
    // TXNJ-52 - don't care about the date really, the time is important
    final static DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    final private long timestamp;
    final private long threadId;
    final private String threadName;
    final private String transactionId;
    final private String secondaryId;
    final private String fmt;
    final private Object[] values;

    // TXNJ-56: Use fewer characters for UUIDs in logging
    final public static int CHARS_TO_LOG = 5;

    @Stability.Internal
    public TransactionLogEvent(long timestamp,
                               long threadId,
                               String threadName,
                               String transactionId,
                               @Nullable String secondaryId,
                               String fmt,
                               @Nullable Object[] values,
                               Event.Severity level) {
        super(level, DEFAULT_CATEGORY);
        this.timestamp = timestamp;
        this.threadId = threadId;
        this.threadName = Objects.requireNonNull(threadName);
        this.transactionId = Objects.requireNonNull(transactionId);
        this.secondaryId = secondaryId;
        this.fmt = Objects.requireNonNull(fmt);
        this.values = values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // TXNJ-52
        LocalDateTime localTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
        );
        sb.append(FORMATTER.format(localTime));
        sb.append(' ');
        sb.append(threadId);
        sb.append('/');
        sb.append(threadName);
        sb.append('/');
        if (transactionId.length() > CHARS_TO_LOG) {
            sb.append(transactionId.substring(0, CHARS_TO_LOG));
        }
        else {
            sb.append(transactionId);
        }
        if (secondaryId != null) {
            sb.append('/');
            if (secondaryId.length() > CHARS_TO_LOG) {
                sb.append(secondaryId.substring(0, CHARS_TO_LOG));
            }
            else {
                sb.append(secondaryId);
            }
        }
        sb.append(' ');
        if (values != null) {
            String value = String.format(fmt, values);
            sb.append(value);
        } else {
            sb.append(fmt);
        }
        return sb.toString();
    }

    @Override
    public String description() {
        return toString();
    }
}
