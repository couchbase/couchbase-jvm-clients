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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.LoggerConfig;
import com.couchbase.client.core.msg.RequestContext;
import org.slf4j.MDC;

import java.io.PrintStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;

/**
 * Consumes {@link Event Events} and logs them per configuration.
 *
 * <p>This consumer is intended to be attached per default and performs convenient logging
 * throughout the system. It tries to detect settings and loggers in a best-effort
 * way but can always be swapped out or changed to implement custom functionality.</p>
 *
 * <p>If SLF4J is detected on the classpath it will be used, otherwise it will fall back to
 * java.com.couchbase.client.test.util.logging or the console depending on the configuration.</p>
 */
public class LoggingEventConsumer implements Consumer<Event> {

  /**
   * Contains true if SLF4J is on the classpath, false otherwise.
   */
  private static final boolean SLF4J_AVAILABLE = slf4JOnClasspath();

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

  private Logger createLogger(final String name) {
    Logger logger;

    if (loggerConfig.customLogger() != null) {
      logger = loggerConfig.customLogger();
    } else if (SLF4J_AVAILABLE && !loggerConfig.disableSlf4J()) {
      logger = new Slf4JLogger(name);
    } else if (loggerConfig.fallbackToConsole()) {
      logger = new ConsoleLogger(name);
    } else {
      logger = new JdkLogger(name);
    }

    return logger;
  }

  @Override
  public void accept(final Event event) {
    if (event.severity() == Event.Severity.TRACING) {
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

    Logger logger = loggers.get(event.category());
    if (logger == null) {
      logger = createLogger(event.category());
      loggers.put(event.category(), logger);
    }

    boolean diagnosticContext = loggerConfig.diagnosticContextEnabled() && event.context() instanceof RequestContext;

    if (diagnosticContext) {
      logger.attachContext(((RequestContext) event.context()).clientContext());
    }

    switch (event.severity()) {
      case VERBOSE:
        if (event.cause() != null) {
          logger.trace(logLine, event.cause());
        } else {
          logger.trace(logLine);
        }
        break;
      case DEBUG:
        if (event.cause() != null) {
          logger.debug(logLine, event.cause());
        } else {
          logger.debug(logLine);
        }
        break;
      case INFO:
        if (event.cause() != null) {
          logger.info(logLine, event.cause());
        } else {
          logger.info(logLine);
        }
        break;
      case WARN:
        if (event.cause() != null) {
          logger.warn(logLine, event.cause());
        } else {
          logger.warn(logLine);
        }
        break;
      case ERROR:
        if (event.cause() != null) {
          logger.error(logLine, event.cause());
        } else {
          logger.error(logLine);
        }
      default:
    }

    if (diagnosticContext) {
      logger.clearContext();
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
  private static String convertEventDuration(final Duration duration) {
    long nanos = duration.toNanos();
    if (nanos < 1000L) { // everything below a microsecond is ns
      return nanos + "ns";
    } else if (nanos < 1000_000_0L) {
      return TimeUnit.NANOSECONDS.toMicros(nanos) + "µs"; // everything below 10ms is micros
    } else if (nanos < 1000_000_000_0L) { // everything below 10s is millis
      return TimeUnit.NANOSECONDS.toMillis(nanos) + "ms";
    } else { // everything higher than 10s is seconds
      return TimeUnit.NANOSECONDS.toSeconds(nanos) + "s";
    }
  }

  /**
   * Helper method to check if SLF4J is on the classpath.
   */
  private static boolean slf4JOnClasspath() {
    try {
      Class.forName("org.slf4j.Logger");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Generic logger interface.
   */
  @Stability.Internal
  public interface Logger {

    /**
     * Return the name of this <code>Logger</code> instance.
     *
     * @return name of this logger instance
     */
    String getName();

    /**
     * Is the logger instance enabled for the TRACE level.
     *
     * @return True if this Logger is enabled for the TRACE level,
     *         false otherwise.
     */
    boolean isTraceEnabled();

    /**
     * Log a message at the TRACE level.
     *
     * @param msg the message string to be logged
     */
    void trace(String msg);

    /**
     * Log a message at the TRACE level according to the specified format
     * and arguments.
     *
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the TRACE level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for TRACE.</p>
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    void trace(String format, Object... arguments);

    /**
     * Log an exception (throwable) at the TRACE level with an
     * accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    void trace(String msg, Throwable t);

    /**
     * Is the logger instance enabled for the DEBUG level.
     *
     * @return True if this Logger is enabled for the DEBUG level,
     *         false otherwise.
     */
    boolean isDebugEnabled();

    /**
     * Log a message at the DEBUG level.
     *
     * @param msg the message string to be logged
     */
    void debug(String msg);

    /**
     * Log a message at the DEBUG level according to the specified format
     * and arguments.
     *
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the DEBUG level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for DEBUG. </p>
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    void debug(String format, Object... arguments);

    /**
     * Log an exception (throwable) at the DEBUG level with an
     * accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    void debug(String msg, Throwable t);

    /**
     * Is the logger instance enabled for the INFO level.
     *
     * @return True if this Logger is enabled for the INFO level,
     *         false otherwise.
     */
    boolean isInfoEnabled();

    /**
     * Log a message at the INFO level.
     *
     * @param msg the message string to be logged
     */
    void info(String msg);

    /**
     * Log a message at the INFO level according to the specified format
     * and arguments.
     *
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the INFO level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for INFO. </p>
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    void info(String format, Object... arguments);

    /**
     * Log an exception (throwable) at the INFO level with an
     * accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    void info(String msg, Throwable t);

    /**
     * Is the logger instance enabled for the WARN level.
     *
     * @return True if this Logger is enabled for the WARN level,
     *         false otherwise.
     */
    boolean isWarnEnabled();

    /**
     * Log a message at the WARN level.
     *
     * @param msg the message string to be logged
     */
    void warn(String msg);

    /**
     * Log a message at the WARN level according to the specified format
     * and arguments.
     *
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the WARN level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for WARN. </p>
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    void warn(String format, Object... arguments);

    /**
     * Log an exception (throwable) at the WARN level with an
     * accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    void warn(String msg, Throwable t);

    /**
     * Is the logger instance enabled for the ERROR level.
     *
     * @return True if this Logger is enabled for the ERROR level,
     *         false otherwise.
     */
    boolean isErrorEnabled();

    /**
     * Log a message at the ERROR level.
     *
     * @param msg the message string to be logged
     */
    void error(String msg);

    /**
     * Log a message at the ERROR level according to the specified format
     * and arguments.
     *
     * <p>This form avoids superfluous string concatenation when the logger
     * is disabled for the ERROR level. However, this variant incurs the hidden
     * (and relatively small) cost of creating an <code>Object[]</code> before invoking the method,
     * even if this logger is disabled for ERROR. </p>
     *
     * @param format    the format string
     * @param arguments a list of 3 or more arguments
     */
    void error(String format, Object... arguments);

    /**
     * Log an exception (throwable) at the ERROR level with an
     * accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    void error(String msg, Throwable t);

    /**
     * Writes a diagnostics key/value pair.
     *
     * <p>note that this feature might not be supported by all implementations.</p>
     *
     * @param context the context to attach
     */
    default void attachContext(Map<String, Object> context) {}

    /**
     * Clears the diagnostics context for this thread.
     *
     * <p>note that this feature might not be supported by all implementations.</p>
     */
    default void clearContext() {}

  }

  static class Slf4JLogger implements Logger {

    private final org.slf4j.Logger logger;

    Slf4JLogger(final String name) {
      logger = org.slf4j.LoggerFactory.getLogger(name);
    }

    @Override
    public String getName() {
      return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
      return logger.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
      logger.trace(msg);
    }

    @Override
    public void trace(String format, Object... arguments) {
      logger.trace(format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
      logger.trace(msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
      return logger.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
      logger.debug(msg);
    }

    @Override
    public void debug(String format, Object... arguments) {
      logger.debug(format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
      logger.debug(msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
      return logger.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
      logger.info(msg);
    }

    @Override
    public void info(String format, Object... arguments) {
      logger.info(format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
      logger.info(msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
      return logger.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
      logger.warn(msg);
    }

    @Override
    public void warn(String format, Object... arguments) {
      logger.warn(format, arguments);
    }

    @Override
    public void warn(String msg, Throwable t) {
      logger.warn(msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
      return logger.isErrorEnabled();
    }

    @Override
    public void error(String msg) {
      logger.error(msg);
    }

    @Override
    public void error(String format, Object... arguments) {
      logger.error(format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
      logger.error(msg, t);
    }

    @Override
    public void attachContext(final Map<String, Object> context) {
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

    @Override
    public void clearContext() {
      MDC.clear();
    }

  }

  static class JdkLogger implements Logger {

    private final java.util.logging.Logger logger;

    JdkLogger(String name) {
      this.logger = java.util.logging.Logger.getLogger(name);
    }

    @Override
    public String getName() {
      return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
      return logger.isLoggable(Level.FINEST);
    }

    @Override
    public void trace(String msg) {
      logger.log(Level.FINEST, msg);
    }

    @Override
    public void trace(String format, Object... arguments) {
      logger.log(Level.FINEST, formatHelper(format, arguments));
    }

    @Override
    public void trace(String msg, Throwable t) {
      logger.log(Level.FINEST, msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
      return logger.isLoggable(Level.FINE);
    }

    @Override
    public void debug(String msg) {
      logger.log(Level.FINE, msg);
    }

    @Override
    public void debug(String format, Object... arguments) {
      logger.log(Level.FINE, formatHelper(format, arguments));
    }

    @Override
    public void debug(String msg, Throwable t) {
      logger.log(Level.FINE, msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
      return logger.isLoggable(Level.INFO);
    }

    @Override
    public void info(String msg) {
      logger.log(Level.INFO, msg);
    }

    @Override
    public void info(String format, Object... arguments) {
      logger.log(Level.INFO, formatHelper(format, arguments));
    }

    @Override
    public void info(String msg, Throwable t) {
      logger.log(Level.INFO, msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
      return logger.isLoggable(Level.WARNING);
    }

    @Override
    public void warn(String msg) {
      logger.log(Level.WARNING, msg);
    }

    @Override
    public void warn(String format, Object... arguments) {
      logger.log(Level.WARNING, formatHelper(format, arguments));
    }

    @Override
    public void warn(String msg, Throwable t) {
      logger.log(Level.WARNING, msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
      return logger.isLoggable(Level.SEVERE);
    }

    @Override
    public void error(String msg) {
      logger.log(Level.SEVERE, msg);
    }

    @Override
    public void error(String format, Object... arguments) {
      logger.log(Level.SEVERE, formatHelper(format, arguments));
    }

    @Override
    public void error(String msg, Throwable t) {
      logger.log(Level.SEVERE, msg, t);
    }

  }

  static class ConsoleLogger implements Logger {

    private final String name;
    private final PrintStream err;
    private final PrintStream log;

    ConsoleLogger(String name) {
      this.name = name;
      this.log = System.out;
      this.err = System.err;
    }

    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public boolean isTraceEnabled() {
      return true;
    }

    @Override
    public synchronized void trace(String msg) {
      this.log.format("[TRACE] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void trace(String format, Object... arguments) {
      this.log.format("[TRACE] (%s) %s\n", Thread.currentThread().getName(),
        formatHelper(format, arguments));
    }

    @Override
    public synchronized void trace(String msg, Throwable t) {
      this.log.format("[TRACE] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.log);
    }

    @Override
    public boolean isDebugEnabled() {
      return true;
    }

    @Override
    public synchronized void debug(String msg) {
      this.log.format("[DEBUG] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void debug(String format, Object... arguments) {
      this.log.format("[DEBUG] (%s) %s\n", Thread.currentThread().getName(),
        formatHelper(format, arguments));
    }

    @Override
    public synchronized void debug(String msg, Throwable t) {
      this.log.format("[DEBUG] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.log);
    }

    @Override
    public boolean isInfoEnabled() {
      return true;
    }

    @Override
    public synchronized void info(String msg) {
      this.log.format("[ INFO] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void info(String format, Object... arguments) {
      this.log.format("[ INFO] (%s) %s\n", Thread.currentThread().getName(),
        formatHelper(format, arguments));
    }

    @Override
    public synchronized void info(String msg, Throwable t) {
      this.log.format("[ INFO] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.log);
    }

    @Override
    public boolean isWarnEnabled() {
      return true;
    }

    @Override
    public synchronized void warn(String msg) {
      this.err.format("[ WARN] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void warn(String format, Object... arguments) {
      this.err.format("[ WARN] (%s) %s\n", Thread.currentThread().getName(),
        formatHelper(format, arguments));
    }

    @Override
    public synchronized void warn(String msg, Throwable t) {
      this.err.format("[ WARN] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.err);
    }

    @Override
    public boolean isErrorEnabled() {
      return true;
    }

    @Override
    public synchronized void error(String msg) {
      this.err.format("[ERROR] (%s) %s\n", Thread.currentThread().getName(), msg);
    }

    @Override
    public synchronized void error(String format, Object... arguments) {
      this.err.format("[ERROR] (%s) %s\n", Thread.currentThread().getName(),
        formatHelper(format, arguments));
    }

    @Override
    public synchronized void error(String msg, Throwable t) {
      this.err.format("[ERROR] (%s) %s - %s\n", Thread.currentThread().getName(), msg, t);
      t.printStackTrace(this.err);
    }
  }

  /**
   * Helper method to compute the formatted arguments.
   *
   * @param from      the original string
   * @param arguments the arguments to replace
   * @return the formatted string.
   */
  private static String formatHelper(final String from, final Object... arguments) {
    if (from != null) {
      String computed = from;
      if (arguments != null && arguments.length != 0) {
        for (Object argument : arguments) {
          computed = computed.replaceFirst(
            "\\{\\}", Matcher.quoteReplacement(argument.toString())
          );
        }
      }
      return computed;
    }
    return null;
  }

}
