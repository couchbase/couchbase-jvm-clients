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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Verifies the functionality of the {@link LoggingEventConsumer}.
 */
class LoggingEventConsumerTest {

  private LoggingEventConsumer.Logger logger;
  private LoggingEventConsumer loggingEventConsumer;

  @BeforeEach
  void setup() {
    logger = mock(LoggingEventConsumer.Logger.class);
    loggingEventConsumer = LoggingEventConsumer
      .builder()
      .customLogger(logger)
      .build();
  }

  @Test
  void formatAndLogWithoutContextOrDuration() {
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ZERO, null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[IO][MyEvent]");
  }

  @Test
  void formatAndLogWithDuration() {
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(123), null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[IO][MyEvent][123000µs]");
  }

  @Test
  void formatAndLogWithContext() {
    Map<String, Object> ctxData = new HashMap<>();
    ctxData.put("foo", true);
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ZERO,
      new MyContext(ctxData));
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[IO][MyEvent] {\"foo\":true}");
  }

  @Test
  void formatAndLogWithContextAndDuration() {
    Map<String, Object> ctxData = new HashMap<>();
    ctxData.put("foo", true);
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(123),
      new MyContext(ctxData));
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[IO][MyEvent][123000µs] {\"foo\":true}");
  }

  @Test
  void formatAndLogWithDescription() {
    Event event = new EventWithDescription("some text");
    loggingEventConsumer.accept(event);
    verify(logger, times(1))
      .debug("[IO][EventWithDescription][3600000000µs] some text");
  }

  static class MyEvent extends AbstractEvent {
    MyEvent(Severity severity, Category category, Duration duration, Context context) {
      super(severity, category, duration, context);
    }
  }

  static class EventWithDescription extends AbstractEvent {

    final String desc;

    EventWithDescription(String desc) {
      super(Severity.DEBUG, Category.IO, Duration.ofHours(1), null);
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }
  }

  static class MyContext extends AbstractContext {

    private final Map<String, Object> data;

    MyContext(Map<String, Object> data) {
      this.data = data;
    }

    @Override
    protected void injectExportableParams(Map<String, Object> data) {
      data.putAll(this.data);
    }
  }

}
