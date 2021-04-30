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

public class LoggerConfig {

  @Stability.Internal
  public static class Defaults {
    public static final boolean DEFAULT_FALLBACK_TO_CONSOLE = true;
    public static final boolean DEFAULT_DISABLE_SLF4J = false;
    public static final String DEFAULT_LOGGER_NAME = "CouchbaseLogger";
    public static final boolean DEFAULT_DIAGNOSTIC_CONTEXT_ENABLED = false;
  }

  private final LoggingEventConsumer.Logger customLogger;
  private final boolean fallbackToConsole;
  private final boolean disableSlf4J;
  private final String loggerName;
  private final boolean diagnosticContextEnabled;

  private LoggerConfig(final Builder builder) {
    customLogger = builder.customLogger;
    disableSlf4J = builder.disableSlf4J;
    loggerName = builder.loggerName;
    fallbackToConsole = builder.fallbackToConsole;
    diagnosticContextEnabled = builder.diagnosticContextEnabled;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static LoggerConfig create() {
    return builder().build();
  }

  public static Builder fallbackToConsole(boolean fallbackToConsole) {
    return builder().fallbackToConsole(fallbackToConsole);
  }

  public static Builder disableSlf4J(boolean disableSlf4J) {
    return builder().disableSlf4J(disableSlf4J);
  }

  public static Builder loggerName(String loggerName) {
    return builder().loggerName(loggerName);
  }

  public static Builder enableDiagnosticContext(boolean diagnosticContextEnabled) {
    return builder().enableDiagnosticContext(diagnosticContextEnabled);
  }

  public static Builder customLogger(LoggingEventConsumer.Logger customLogger) {
    return builder().customLogger(customLogger);
  }

  public LoggingEventConsumer.Logger customLogger() {
    return customLogger;
  }

  public boolean fallbackToConsole() {
    return fallbackToConsole;
  }

  public boolean disableSlf4J() {
    return disableSlf4J;
  }

  public String loggerName() {
    return loggerName;
  }

  public boolean diagnosticContextEnabled() {
    return diagnosticContextEnabled;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("customLogger", customLogger == null ? null : customLogger.getClass().getSimpleName());
    export.put("fallbackToConsole", fallbackToConsole);
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

    /**
     * Allows to specify a custom logger. This is used for testing only.
     *
     * @param customLogger the custom logger
     * @return the Builder for chaining purposes
     */
    @Stability.Internal
    public Builder customLogger(LoggingEventConsumer.Logger customLogger) {
      this.customLogger = customLogger;
      return this;
    }

    public Builder fallbackToConsole(boolean fallbackToConsole) {
      this.fallbackToConsole = fallbackToConsole;
      return this;
    }

    public Builder disableSlf4J(boolean disableSlf4J) {
      this.disableSlf4J = disableSlf4J;
      return this;
    }

    public Builder loggerName(String loggerName) {
      this.loggerName = loggerName;
      return this;
    }

    public Builder enableDiagnosticContext(boolean diagnosticContextEnabled) {
      this.diagnosticContextEnabled = diagnosticContextEnabled;
      return this;
    }

    public LoggerConfig build() {
      return new LoggerConfig(this);
    }

  }


}
