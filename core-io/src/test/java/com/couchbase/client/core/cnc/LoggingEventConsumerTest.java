/*
 * Copyright (c) 2018 Couchbase, Inc.
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
package com.couchbase.client.core.cnc;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Verifies the functionality of the {@link LoggingEventConsumer}.
 */
class LoggingEventConsumerTest {

    @Test
    void formatAndLogWithoutContextOrDuration() {
        LoggingEventConsumer.Logger logger = mock(LoggingEventConsumer.Logger.class);
        LoggingEventConsumer loggingEventConsumer = LoggingEventConsumer.builder().customLogger(logger).build();

        Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ZERO, null);
        loggingEventConsumer.accept(event);

        verify(logger, times(1)).info("[IO][MyEvent]");
    }

    @Test
    void formatAndLogWithDuration() {
        LoggingEventConsumer.Logger logger = mock(LoggingEventConsumer.Logger.class);
        LoggingEventConsumer loggingEventConsumer = LoggingEventConsumer.builder().customLogger(logger).build();

        Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(123), null);
        loggingEventConsumer.accept(event);

        verify(logger, times(1)).info("[IO][MyEvent][123000µs]");
    }

    @Test
    void formatAndLogWithContext() {
        LoggingEventConsumer.Logger logger = mock(LoggingEventConsumer.Logger.class);
        LoggingEventConsumer loggingEventConsumer = LoggingEventConsumer.builder().customLogger(logger).build();

        Map<String, Object> ctxData = new HashMap<>();
        ctxData.put("foo", true);
        Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ZERO, new MyContext(ctxData));
        loggingEventConsumer.accept(event);

        verify(logger, times(1)).info("[IO][MyEvent]: {\"foo\":true}");
    }

    @Test
    void formatAndLogWithContextAndDuration() {
        LoggingEventConsumer.Logger logger = mock(LoggingEventConsumer.Logger.class);
        LoggingEventConsumer loggingEventConsumer = LoggingEventConsumer.builder().customLogger(logger).build();

        Map<String, Object> ctxData = new HashMap<>();
        ctxData.put("foo", true);
        Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(123), new MyContext(ctxData));
        loggingEventConsumer.accept(event);

        verify(logger, times(1)).info("[IO][MyEvent][123000µs]: {\"foo\":true}");
    }

    static class MyEvent extends AbstractEvent {
        MyEvent(Severity severity, Category category, Duration duration, Context context) {
            super(severity, category, duration, context);
        }
    }

    static class MyContext extends AbstractContext {

        private final Map<String, Object> data;

        MyContext(Map<String, Object> data) {
            this.data = data;
        }

        @Override
        protected Map<String, Object> injectExportableParams() {
            return data;
        }
    }

}
