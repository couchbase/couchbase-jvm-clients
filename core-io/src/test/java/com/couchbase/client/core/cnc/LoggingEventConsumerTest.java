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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.couchbase.client.core.cnc.events.request.RequestRetryScheduledEvent;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.retry.RetryReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * Verifies the functionality of the {@link LoggingEventConsumer}.
 */
class LoggingEventConsumerTest {

  private LoggingEventConsumer.Logger logger;
  private LoggingEventConsumer loggingEventConsumer;

  @BeforeEach
  void setup() {
    logger = mock(LoggingEventConsumer.Logger.class);

    // Enable all log levels by default
    when(logger.isTraceEnabled()).thenReturn(true);
    when(logger.isDebugEnabled()).thenReturn(true);
    when(logger.isInfoEnabled()).thenReturn(true);
    when(logger.isWarnEnabled()).thenReturn(true);
    when(logger.isErrorEnabled()).thenReturn(true);

    loggingEventConsumer = LoggingEventConsumer.create(LoggerConfig.customLogger(logger).build());
  }

  @Test
  void formatAndLogWithoutContextOrDuration() {
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ZERO, null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent]");
  }

  @Test
  void formatAndLogWithDuration() {
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(123), null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent][123ms]");
  }

  @Test
  void formatAndLogWithContext() {
    Map<String, Object> ctxData = new HashMap<>();
    ctxData.put("foo", true);
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ZERO,
      new MyContext(ctxData));
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent] {\"foo\":true}");
  }

  @Test
  void formatAndLogWithContextAndDuration() {
    Map<String, Object> ctxData = new HashMap<>();
    ctxData.put("foo", true);
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(123),
      new MyContext(ctxData));
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent][123ms] {\"foo\":true}");
  }

  @Test
  void formatAndLogWithDescription() {
    Event event = new EventWithDescription("some text");
    loggingEventConsumer.accept(event);
    verify(logger, times(1))
      .debug("[com.couchbase.io][EventWithDescription][3600s] some text");
  }

  @Test
  void attachesClientContextIfEnabled() {
    LoggingEventConsumer loggingEventConsumer = LoggingEventConsumer.create(
      LoggerConfig.enableDiagnosticContext(true).customLogger(logger).build()
    );

    RequestContext context = mock(RequestContext.class);
    Map<String, Object> userContext = new HashMap<>();
    userContext.put("hello", "world");
    when(context.clientContext()).thenReturn(userContext);

    RequestRetryScheduledEvent retryEvent = new RequestRetryScheduledEvent(Duration.ofSeconds(1), context, GetRequest.class, RetryReason.UNKNOWN);
    loggingEventConsumer.accept(retryEvent);

    verify(logger, times(1)).attachContext(userContext);
  }

  @Test
  void doesNotAttachClientContextByDefault() {
    RequestContext context = mock(RequestContext.class);
    Map<String, Object> userContext = new HashMap<>();
    userContext.put("hello", "world");
    when(context.clientContext()).thenReturn(userContext);

    RequestRetryScheduledEvent retryEvent = new RequestRetryScheduledEvent(Duration.ofSeconds(1), context, GetRequest.class, RetryReason.UNKNOWN);
    loggingEventConsumer.accept(retryEvent);

    verify(logger, never()).attachContext(userContext);
  }

  @Test
  void convertsDurationsAtExpectedBoundaries() {
    Event event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(1), null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent][1000us]");

    event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofMillis(11), null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent][11ms]");

    event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofSeconds(1), null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent][1000ms]");

    event = new MyEvent(Event.Severity.INFO, Event.Category.IO, Duration.ofSeconds(11), null);
    loggingEventConsumer.accept(event);
    verify(logger, times(1)).info("[com.couchbase.io][MyEvent][11s]");
  }

  @Test
  void verifyConsoleLoggerLogLevelEnablement() {
    LoggingEventConsumer.ConsoleLogger logger = new LoggingEventConsumer.ConsoleLogger("logger", Level.INFO);

    assertFalse(logger.isTraceEnabled());
    assertFalse(logger.isDebugEnabled());
    assertTrue(logger.isInfoEnabled());
    assertTrue(logger.isWarnEnabled());
    assertTrue(logger.isErrorEnabled());
  }

  static class MyEvent extends AbstractEvent {
    MyEvent(Severity severity, Category category, Duration duration, Context context) {
      super(severity, category, duration, context);
    }

    @Override
    public String description() {
      return "";
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
    public void injectExportableParams(Map<String, Object> data) {
      data.putAll(this.data);
    }
  }

}
