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

import com.couchbase.client.core.cnc.events.request.RequestRetryScheduledEvent;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.retry.RetryReason;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.CharArrayWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.cnc.LoggingEventConsumer.convertEventDuration;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LoggingEventConsumerTest {

  private static final String TEST_MDC_KEY = "testMdcKey";
  private static final String TEST_MDC_VALUE = "testMdcValue";

  private static final String PATTERN = "%-5level %X{" + TEST_MDC_KEY + "} %msg";
  private static final Event.Category category = Event.Category.IO;

  private Logger logger;
  private Appender appender;
  private final CharArrayWriter logOutput = new CharArrayWriter();

  private LoggingEventConsumer loggingEventConsumer;

  @BeforeEach
  void setup() {
    logger = (Logger) LogManager.getLogger(category.path());

    StringLayout layout = PatternLayout.newBuilder().withPattern(PATTERN).build();
    appender = WriterAppender.newBuilder()
      .setTarget(logOutput)
      .setLayout(layout)
      .setName("test-appender")
      .build();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.ALL);

    loggingEventConsumer = LoggingEventConsumer.create();
  }

  @AfterEach
  void cleanup() {
    logger.removeAppender(appender);
    appender.stop();
  }

  private void assertLogOutput(String expectedLine) {
    assertEquals(expectedLine, logOutput.toString());
  }

  @Test
  void formatAndLogWithoutContextOrDuration() {
    Event event = new MyEvent(Event.Severity.INFO, category, Duration.ZERO, null);
    loggingEventConsumer.accept(event);
    assertLogOutput("INFO   [com.couchbase.io][MyEvent]");
  }

  @Test
  void formatAndLogWithDuration() {
    Event event = new MyEvent(Event.Severity.INFO, category, Duration.ofMillis(123), null);
    loggingEventConsumer.accept(event);
    assertLogOutput("INFO   [com.couchbase.io][MyEvent][123ms]");
  }

  @Test
  void formatAndLogWithContext() {
    Map<String, Object> ctxData = new HashMap<>();
    ctxData.put("foo", true);
    Event event = new MyEvent(Event.Severity.INFO, category, Duration.ZERO,
      new MyContext(ctxData));
    loggingEventConsumer.accept(event);
    assertLogOutput("INFO   [com.couchbase.io][MyEvent] {\"foo\":true}");
  }

  @Test
  void formatAndLogWithContextAndDuration() {
    Map<String, Object> ctxData = new HashMap<>();
    ctxData.put("foo", true);
    Event event = new MyEvent(Event.Severity.INFO, category, Duration.ofMillis(123),
      new MyContext(ctxData));
    loggingEventConsumer.accept(event);
    assertLogOutput("INFO   [com.couchbase.io][MyEvent][123ms] {\"foo\":true}");
  }

  @Test
  void formatAndLogWithDescription() {
    Event event = new EventWithDescription("some text");
    loggingEventConsumer.accept(event);
    assertLogOutput("DEBUG  [com.couchbase.io][EventWithDescription][3600s] some text");
  }

  @Test
  void attachesClientContextIfEnabled() {
    LoggingEventConsumer loggingEventConsumer = LoggingEventConsumer.create(
      LoggerConfig.builder()
        .enableDiagnosticContext(true)
        .build()
    );

    loggingEventConsumer.accept(mockRetryEvent());
    assertLogOutput("DEBUG " + TEST_MDC_VALUE + " [com.couchbase.io][][1000ms] Request GetRequest retry scheduled per RetryStrategy (Reason: UNKNOWN) {\"foo\":true}");
  }

  @Test
  void doesNotAttachClientContextByDefault() {
    loggingEventConsumer.accept(mockRetryEvent());
    assertLogOutput("DEBUG  [com.couchbase.io][][1000ms] Request GetRequest retry scheduled per RetryStrategy (Reason: UNKNOWN) {\"foo\":true}");
  }

  private static Event mockRetryEvent() {
    RequestContext context = mock(RequestContext.class);
    when(context.clientContext()).thenReturn(mapOf(TEST_MDC_KEY, TEST_MDC_VALUE));
    when(context.exportAsString(Context.ExportFormat.JSON)).thenReturn("{\"foo\":true}");

    return new RequestRetryScheduledEvent(Duration.ofSeconds(1), context, GetRequest.class, RetryReason.UNKNOWN) {
      public String category() {
        return category.path();
      }
    };
  }

  @Test
  void convertsDurationsAtExpectedBoundaries() {
    assertEquals("1000us", convertEventDuration(Duration.ofMillis(1)));
    assertEquals("11ms", convertEventDuration(Duration.ofMillis(11)));
    assertEquals("1000ms", convertEventDuration(Duration.ofSeconds(1)));
    assertEquals("11s", convertEventDuration(Duration.ofSeconds(11)));
  }

  private static class MyEvent extends AbstractEvent {
    MyEvent(Severity severity, Category category, Duration duration, Context context) {
      super(severity, category, duration, context);
    }

    @Override
    public String description() {
      return "";
    }
  }

  private static class EventWithDescription extends AbstractEvent {

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

  private static class MyContext extends AbstractContext {

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
