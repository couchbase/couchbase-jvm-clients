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

import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.msg.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;

/**
 * Consumes {@link Event Events} and logs them per configuration.
 * <p>
 * This consumer is intended to be attached by default and performs convenient logging
 * throughout the system.
 */
public class LoggingEventConsumer implements Consumer<Event> {

  /**
   * Contains the selected loggers that should be used for logging.
   */
  private final Map<String, Logger> loggers = new HashMap<>(Event.Category.values().length);

  private final LoggerConfig loggerConfig;

  /**
   * Creates a new {@link LoggingEventConsumer} with all defaults.
   *
   * @return a {@link LoggingEventConsumer}.
   */
  public static LoggingEventConsumer create() {
    return new LoggingEventConsumer(LoggerConfig.create());
  }

  public static LoggingEventConsumer create(final LoggerConfig loggerConfig) {
    return new LoggingEventConsumer(loggerConfig);
  }

  private LoggingEventConsumer(final LoggerConfig loggerConfig) {
    this.loggerConfig = loggerConfig;
  }

  @Override
  public void accept(final Event event) {
    Event.Severity severity = event.severity();

    if (severity == Event.Severity.TRACING) {
      return;
    }

    Logger logger = loggers.computeIfAbsent(event.category(), LoggerFactory::getLogger);

    if (!mustLogEvent(severity, logger)) {
      return;
    }

    StringBuilder logLineBuilder = new StringBuilder();

    logLineBuilder.append("[").append(event.category()).append("]");
    logLineBuilder.append("[").append(event.getClass().getSimpleName()).append("]");

    if (!event.duration().isZero()) {
      logLineBuilder
        .append("[")
        .append(convertEventDuration(event.duration()))
        .append("]");
    }

    String description = event.description();
    if (description != null && !description.isEmpty()) {
      logLineBuilder.append(" ").append(description);
    }

    if (event.context() != null) {
      logLineBuilder.append(" ").append(event.context().exportAsString(Context.ExportFormat.JSON));
    }

    String logLine = logLineBuilder.toString();

    boolean diagnosticContext = loggerConfig.diagnosticContextEnabled() && event.context() instanceof RequestContext;

    if (diagnosticContext) {
      attachContext(((RequestContext) event.context()).clientContext());
    }

    switch (severity) {
      case VERBOSE:
        if (event.cause() != null) {
          logger.trace("{}", logLine, event.cause());
        } else {
          logger.trace("{}", logLine);
        }
        break;
      case DEBUG:
        if (event.cause() != null) {
          logger.debug("{}", logLine, event.cause());
        } else {
          logger.debug("{}", logLine);
        }
        break;
      case INFO:
        if (event.cause() != null) {
          logger.info("{}", logLine, event.cause());
        } else {
          logger.info("{}", logLine);
        }
        break;
      case WARN:
        if (event.cause() != null) {
          logger.warn("{}", logLine, event.cause());
        } else {
          logger.warn("{}", logLine);
        }
        break;
      case ERROR:
        if (event.cause() != null) {
          logger.error("{}", logLine, event.cause());
        } else {
          logger.error("{}", logLine);
        }
      default:
    }

    if (diagnosticContext) {
      MDC.clear();
    }
  }

  private static void attachContext(final Map<String, Object> context) {
    if (isNullOrEmpty(context)) {
      return;
    }

    MDC.setContextMap(
      context.entrySet().stream().collect(Collectors.toMap(
        e -> redactUser(e.getKey()).toString(),
        e -> {
          Object v = e.getValue();
          return v == null ? "" : redactUser(v.toString()).toString();
        }))
    );
  }

  /**
   * Helper method to check if an event must be logged based on the severity.
   *
   * @param severity the severity of the event.
   * @param logger the logger to check if specific log levels are enabled.
   * @return true if the event must be logged, false otherwise.
   */
  private static boolean mustLogEvent(final Event.Severity severity, final Logger logger) {
    switch(severity) {
      case VERBOSE:
        return logger.isTraceEnabled();
      case DEBUG:
        return logger.isDebugEnabled();
      case INFO:
        return logger.isInfoEnabled();
      case WARN:
        return logger.isWarnEnabled();
      case ERROR:
        return logger.isErrorEnabled();
      default:
        return true;
    }
  }

  /**
   * Converts the event duration into a reasonable string format.
   * <p>
   * Note that the units are not cut directly at the "kilo" boundary but rather at kilo * 10 so that the precision
   * at the lower ends is not lost. So 1ms will be shown as 10000 micros and 1s as 10000 millis.
   *
   * @param duration the duration to convert.
   * @return the converted duration.
   */
   static String convertEventDuration(final Duration duration) {
    long nanos = duration.toNanos();
    if (nanos < 1000L) { // everything below a microsecond is ns
      return nanos + "ns";
    } else if (nanos < 10_000_000L) {
      return TimeUnit.NANOSECONDS.toMicros(nanos) + "us"; // everything below 10ms is micros
    } else if (nanos < 10_000_000_000L) { // everything below 10s is millis
      return TimeUnit.NANOSECONDS.toMillis(nanos) + "ms";
    } else { // everything higher than 10s is seconds
      return TimeUnit.NANOSECONDS.toSeconds(nanos) + "s";
    }
  }

}
