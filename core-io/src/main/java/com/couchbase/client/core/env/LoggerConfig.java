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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.LoggingEventConsumer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * The {@link LoggerConfig} allows to customize various aspects of the SDKs logging behavior.
 */
public class LoggerConfig {

  @Stability.Internal
  public static class Defaults {
    public static final boolean DEFAULT_FALLBACK_TO_CONSOLE = false;
    public static final boolean DEFAULT_DISABLE_SLF4J = false;
    public static final String DEFAULT_LOGGER_NAME = "CouchbaseLogger";
    public static final boolean DEFAULT_DIAGNOSTIC_CONTEXT_ENABLED = false;
    public static final Level DEFAULT_CONSOLE_LOG_LEVEL = Level.INFO;
  }

  private final LoggingEventConsumer.Logger customLogger;
  private final boolean fallbackToConsole;
  private final boolean disableSlf4J;
  private final String loggerName;
  private final boolean diagnosticContextEnabled;
  private final Level consoleLogLevel;

  private LoggerConfig(final Builder builder) {
    customLogger = builder.customLogger;
    disableSlf4J = builder.disableSlf4J;
    loggerName = builder.loggerName;
    fallbackToConsole = builder.fallbackToConsole;
    diagnosticContextEnabled = builder.diagnosticContextEnabled;
    consoleLogLevel = builder.consoleLogLevel;
  }

  /**
   * Returns a {@link Builder} which can be used to customize the different logging properties.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a {@link LoggerConfig} with all the defaults (can be found in {@link Defaults}).
   *
   * @return the created, immutable logger config with defaults.
   */
  public static LoggerConfig create() {
    return builder().build();
  }

  /**
   * Use the console logger instead of the java.util.logging fallback in case SLF4J is not found or disabled.
   *
   * Please note that in addition setting this to true, either SLF4J must not be on the classpath or manually disabled
   * via {@link #disableSlf4J(boolean)} to make it work. By default, it will log at INFO level to stdout/stderr, but
   * the loglevel can be configured via {@link #consoleLogLevel(Level)}.
   *
   * @param fallbackToConsole true if the console logger should be used as a fallback.
   * @return a {@link Builder} for chaining purposes.
   */
  public static Builder fallbackToConsole(boolean fallbackToConsole) {
    return builder().fallbackToConsole(fallbackToConsole);
  }

  /**
   * Disable SLF4J logging, which is by default the first option tried.
   *
   * If SLF4J is disabled, java.util.logging will be tried next, unless {@link #fallbackToConsole(boolean)} is set
   * to true.
   *
   * @param disableSlf4J set to true to disable SLF4J logging.
   * @return a {@link Builder} for chaining purposes.
   */
  public static Builder disableSlf4J(boolean disableSlf4J) {
    return builder().disableSlf4J(disableSlf4J);
  }

  /**
   * Allowed to set a custom logger name - does not have an effect and is deprecated.
   *
   * @param loggerName the custom logger name.
   * @return a {@link Builder} for chaining purposes.
   * @deprecated the logging infrastructure picks the logger name automatically now based on the event type
   * so it is easier to enable/disable logging or change the verbosity level for certain groups rather than having a
   * single universal logger name.
   */
  @Deprecated
  public static Builder loggerName(String loggerName) {
    return builder().loggerName(loggerName);
  }

  /**
   * Enables the diagnostic context (if supported by the used logger) - disabled by default.
   *
   * Please note that this will only work for the SLF4J logger. Neither the java util logger, nor the console
   * logger support the diagnostic context at this point. In SLF4J parlance, it is called the MDC.
   *
   * @param diagnosticContextEnabled if the diagnostic context should be enabled.
   * @return a {@link Builder} for chaining purposes.
   */
  public static Builder enableDiagnosticContext(boolean diagnosticContextEnabled) {
    return builder().enableDiagnosticContext(diagnosticContextEnabled);
  }

  /**
   * Allows to specify a custom logger. This is used for testing only.
   *
   * @param customLogger the custom logger to use in testing.
   * @return a {@link Builder} for chaining purposes.
   */
  @Stability.Internal
  public static Builder customLogger(final LoggingEventConsumer.Logger customLogger) {
    return builder().customLogger(customLogger);
  }

  /**
   * Allows to customize the log level for the Console Logger.
   *
   * Please note that this DOES NOT AFFECT any other logging infrastructure (so neither the java.util.logging, nor
   * the SLF4J setup which is the default!). It will only affect the log level if {@link #fallbackToConsole(boolean)}
   * is set to true at the same time.
   *
   * @param consoleLogLevel the log level for the console logger.
   * @return a {@link Builder} for chaining purposes.
   */
  public static Builder consoleLogLevel(final Level consoleLogLevel) {
    return builder().consoleLogLevel(consoleLogLevel);
  }

  /**
   * Returns a custom logger if configured for testing.
   */
  @Stability.Internal
  public LoggingEventConsumer.Logger customLogger() {
    return customLogger;
  }

  /**
   * Returns true if the console fallback is activated.
   */
  public boolean fallbackToConsole() {
    return fallbackToConsole;
  }

  /**
   * Returns true if SLF4J should not be used, even if found on the classpath.
   */
  public boolean disableSlf4J() {
    return disableSlf4J;
  }

  /**
   * Returns the custom logger name to use, deprecated.
   */
  @Deprecated
  public String loggerName() {
    return loggerName;
  }

  /**
   * Returns true if the diagnostic context is enabled (disabled by default).
   */
  public boolean diagnosticContextEnabled() {
    return diagnosticContextEnabled;
  }

  /**
   * Returns the log level that should be used if the ConsoleLogger is enabled/used.
   *
   * @return the log level for the console logger.
   */
  public Level consoleLogLevel() {
    return consoleLogLevel;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("customLogger", customLogger == null ? null : customLogger.getClass().getSimpleName());
    export.put("fallbackToConsole", fallbackToConsole);
    export.put("consoleLogLevel", consoleLogLevel);
    export.put("disableSlf4j", disableSlf4J);
    export.put("loggerName", loggerName);
    export.put("diagnosticContextEnabled", diagnosticContextEnabled);
    return export;
  }

  public static class Builder {
    private LoggingEventConsumer.Logger customLogger = null;
    private boolean fallbackToConsole = Defaults.DEFAULT_FALLBACK_TO_CONSOLE;
    private boolean disableSlf4J = Defaults.DEFAULT_DISABLE_SLF4J;
    private String loggerName = Defaults.DEFAULT_LOGGER_NAME;
    private boolean diagnosticContextEnabled = Defaults.DEFAULT_DIAGNOSTIC_CONTEXT_ENABLED;
    private Level consoleLogLevel = Defaults.DEFAULT_CONSOLE_LOG_LEVEL;

    /**
     * Allows to specify a custom logger. This is used for testing only.
     *
     * @param customLogger the custom logger
     * @return this {@link Builder} for chaining purposes.
     */
    @Stability.Internal
    public Builder customLogger(final LoggingEventConsumer.Logger customLogger) {
      this.customLogger = customLogger;
      return this;
    }

    /**
     * Use the console logger instead of the java.util.logging fallback in case SLF4J is not found or disabled.
     *
     * Please note that in addition setting this to true, either SLF4J must not be on the classpath or manually disabled
     * via {@link #disableSlf4J(boolean)} to make it work. By default, it will log at INFO level to stdout/stderr, but
     * the loglevel can be configured via {@link #consoleLogLevel(Level)}.
     *
     * @param fallbackToConsole true if the console logger should be used as a fallback.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder fallbackToConsole(final boolean fallbackToConsole) {
      this.fallbackToConsole = fallbackToConsole;
      return this;
    }

    /**
     * Disable SLF4J logging, which is by default the first option tried.
     *
     * If SLF4J is disabled, java.util.logging will be tried next, unless {@link #fallbackToConsole(boolean)} is set
     * to true.
     *
     * @param disableSlf4J set to true to disable SLF4J logging.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder disableSlf4J(final boolean disableSlf4J) {
      this.disableSlf4J = disableSlf4J;
      return this;
    }

    /**
     * Allowed to set a custom logger name - does not have an effect and is deprecated.
     *
     * @param loggerName the custom logger name.
     * @return this {@link Builder} for chaining purposes.
     * @deprecated the logging infrastructure picks the logger name automatically now based on the event type
     * so it is easier to enable/disable logging or change the verbosity level for certain groups rather than having a
     * single universal logger name.
     */
    @Deprecated
    public Builder loggerName(final String loggerName) {
      this.loggerName = loggerName;
      return this;
    }

    /**
     * Enables the diagnostic context (if supported by the used logger) - disabled by default.
     *
     * Please note that this will only work for the SLF4J logger. Neither the java util logger, nor the console
     * logger support the diagnostic context at this point. In SLF4J parlance, it is called the MDC.
     *
     * @param diagnosticContextEnabled if the diagnostic context should be enabled.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enableDiagnosticContext(boolean diagnosticContextEnabled) {
      this.diagnosticContextEnabled = diagnosticContextEnabled;
      return this;
    }

    /**
     * Allows to customize the log level for the Console Logger.
     *
     * Please note that this DOES NOT AFFECT any other logging infrastructure (so neither the java.util.logging, nor
     * the SLF4J setup which is the default!). It will only affect the log level if {@link #fallbackToConsole(boolean)}
     * is set to true at the same time.
     *
     * @param consoleLogLevel the log level for the console logger.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder consoleLogLevel(final Level consoleLogLevel) {
      this.consoleLogLevel = consoleLogLevel;
      return this;
    }

    /**
     * Builds the {@link LoggerConfig} and makes it immutable.
     *
     * @return the built, immutable logger config.
     */
    public LoggerConfig build() {
      return new LoggerConfig(this);
    }

  }


}
