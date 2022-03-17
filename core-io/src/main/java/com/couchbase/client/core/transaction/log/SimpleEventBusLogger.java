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
import com.couchbase.client.core.cnc.events.core.LogEvent;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple wrapper around logging to the Couchbase event bus.
 *
 * @author Graham Pople
 * @since 3.3.0
 */
@Stability.Internal
public class SimpleEventBusLogger {
    private static final Pattern PLACEHOLDER = Pattern.compile(Pattern.quote("{}"));

    private final EventBus eventBus;
    private final String category;

    public SimpleEventBusLogger(EventBus eventBus, String category) {
        this.eventBus = Objects.requireNonNull(eventBus);
        this.category = Objects.requireNonNull(category);
    }

    public void verbose(String message, Object... args) {
        // Not tracing, that's for something different
        internal(message, Event.Severity.VERBOSE, args);
    }

    public void debug(String message, Object... args) {
        internal(message, Event.Severity.DEBUG, args);
    }

    public void info(String message, Object... args) {
        internal(message, Event.Severity.INFO, args);
    }

    public void warn(String message, Object... args) {
        internal(message, Event.Severity.WARN, args);
    }

    public void error(String message, Object... args) {
        internal(message, Event.Severity.ERROR, args);
    }

    private void internal(String message, Event.Severity level, Object... args) {
        String s = format(message, args);
        eventBus.publish(new LogEvent(level, category, s));
    }

    public static String format(String message, Object... args) {
        if (args.length == 0) {
            return message;
        }

        Iterator<?> i = Arrays.asList(args).iterator();
        Matcher m = PLACEHOLDER.matcher(message);
        StringBuffer result = new StringBuffer();
        while (m.find()) {
            String replacement = i.hasNext() ? String.valueOf(i.next()) : "{}";
            m.appendReplacement(result, replacement);
        }
        m.appendTail(result);

        if (i.hasNext()) {
            Object lastExtraArg = args[args.length-1];
            if (lastExtraArg instanceof Throwable) {
                result.append("\n")
                        .append(stackTraceToString((Throwable) lastExtraArg));
            }
        }

        return result.toString();
    }

    public static String stackTraceToString(Throwable t) {
        StringWriter w = new StringWriter();
        t.printStackTrace(new PrintWriter(w));
        return w.toString();
    }
}
