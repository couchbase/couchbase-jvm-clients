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
import com.couchbase.client.core.cnc.DefaultLoggerFormatter;
import com.couchbase.client.core.cnc.LoggerFormatter;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * The {@link LoggerConfig} allows to customize various aspects of the SDKs logging behavior.
 */
public class LoggerConfig {
  static {
    String loggerFactoryClassName = LoggerFactory.getILoggerFactory().getClass().getName();
    boolean missingSlf4jBinding = loggerFactoryClassName.equals("org.slf4j.helpers.NOPLoggerFactory");

    if (missingSlf4jBinding) {
      String slf4jVersion = "2.0.9";
      System.err.printf(
        "WARN: The Couchbase SDK uses SLF4J for logging, but there does not appear to be an SLF4J binding on the class path.%n" +
          "To see log messages from the Couchbase SDK, please add an SLF4J binding as a dependency of your project.%n" +
          "If you're using Maven, a simple way to enable basic logging is to add these dependencies to your POM:%n" +
          "    <dependency>%n" +
          "        <groupId>org.slf4j</groupId>%n" +
          "        <artifactId>slf4j-api</artifactId>%n" +
          "        <version>" + slf4jVersion + "</version>%n" +
          "    </dependency>%n" +
          "    <dependency>%n" +
          "        <groupId>org.slf4j</groupId>%n" +
          "        <artifactId>slf4j-simple</artifactId>%n" +
          "        <version>" + slf4jVersion + "</version>%n" +
          "    </dependency>%n" +
          "Or if you're using Gradle, add these to your dependencies section:%n" +
          "    implementation(\"org.slf4j:slf4j-api:" + slf4jVersion + "\")%n" +
          "    implementation(\"org.slf4j:slf4j-simple:" + slf4jVersion + "\")%n" +
          "If you see this warning even though there's an SLF4J binding on the class path,%n" +
          "it may be due to an `slf4j-api` version mismatch; try adding an explicit dependency%n" +
          "on the version of `slf4j-api` required by your selected binding.%n" +
          "To learn more about SLF4J bindings, see https://www.slf4j.org/manual.html#swapping%n" +
          "%n"
      );
    }
  }

  @Stability.Internal
  public static class Defaults {
    public static final boolean DEFAULT_DIAGNOSTIC_CONTEXT_ENABLED = false;
  }

  private final boolean diagnosticContextEnabled;

  private LoggerConfig(final Builder builder) {
    diagnosticContextEnabled = builder.diagnosticContextEnabled;
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
   * Returns a new LoggerConfig builder with default settings.
   * <p>
   * SLF4J is used for all logging. If you wish to log to the console,
   * please configure your SLF4J binding accordingly.
   *
   * @param fallbackToConsole ignored
   * @return a {@link Builder} for chaining purposes.
   * @deprecated Please use {@link LoggerConfig#builder()} instead,
   * and configure your SLF4J binding to log to the console if desired.
   */
  @Deprecated
  public static Builder fallbackToConsole(boolean fallbackToConsole) {
    return builder().fallbackToConsole(fallbackToConsole);
  }

  /**
   * Returns a new LoggerConfig builder with default settings.
   * <p>
   * Deprecated because SLF4J is used for all logging, and cannot be disabled.
   *
   * @param disableSlf4J ignored
   * @return a {@link Builder} for chaining purposes.
   * @deprecated Please use {@link LoggerConfig#builder()} instead.
   */
  @Deprecated
  public static Builder disableSlf4J(boolean disableSlf4J) {
    return builder().disableSlf4J(disableSlf4J);
  }

  /**
   * Returns a new LoggerConfig builder with default settings.
   *
   * @param loggerName ignored
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
   * Creates a new LoggerConfig builder with the {@code enableDiagnosticContext} feature
   * set to the specified value.
   *
   * @param diagnosticContextEnabled if the diagnostic context should be enabled.
   * @return a {@link Builder} for chaining purposes.
   * @deprecated Instead, please create a new builder with {@link LoggerConfig#builder()},
   * and then call the non-static {@link LoggerConfig.Builder#enableDiagnosticContext(boolean)} method.
   */
  @Deprecated
  public static Builder enableDiagnosticContext(boolean diagnosticContextEnabled) {
    return builder().enableDiagnosticContext(diagnosticContextEnabled);
  }

  /**
   * Returns a new LoggerConfig builder with default settings.
   * <p>
   * Deprecated because SLF4J is used for all logging. Please configure logging
   * settings like this via your SLF4J binding.
   *
   * @param consoleLogLevel ignored
   * @return a {@link Builder} for chaining purposes.
   * @deprecated Please use {@link LoggerConfig#builder()} instead.
   */
  @Deprecated
  public static Builder consoleLogLevel(final Level consoleLogLevel) {
    return builder().consoleLogLevel(consoleLogLevel);
  }

  /**
   * Returns a new LoggerConfig builder with default settings.
   * <p>
   * Deprecated because SLF4J is used for all logging. Please configure
   * this kind of logging setting via your SLF4J binding.
   *
   * @param loggerFormatter ignored
   * @return a {@link Builder} for chaining purposes.
   * @deprecated Please use {@link LoggerConfig#builder()} instead.
   */
  @Deprecated
  public Builder consoleLoggerFormatter(final LoggerFormatter loggerFormatter) {
    return builder().consoleLoggerFormatter(loggerFormatter);
  }

  /**
   * Always returns false.
   *
   * @deprecated SLF4J is used for all logging.
   */
  @Deprecated
  public boolean fallbackToConsole() {
    return false;
  }

  /**
   * Always returns false.
   *
   * @deprecated SLF4J is used for all logging.
   */
  @Deprecated
  public boolean disableSlf4J() {
    return false;
  }

  /**
   * Always returns empty string.
   *
   * @deprecated Setting a custom logger name has no effect.
   */
  @Deprecated
  public String loggerName() {
    return "";
  }

  /**
   * Returns true if the diagnostic context is enabled (disabled by default).
   */
  public boolean diagnosticContextEnabled() {
    return diagnosticContextEnabled;
  }

  /**
   * Always returns {@link Level#INFO}.
   *
   * @deprecated SLF4J is used for all logging. Setting a console log level has no effect.
   */
  @Deprecated
  public Level consoleLogLevel() {
    return Level.INFO;
  }

  /**
   * Always returns an instance of DefaultLoggerFormatter.
   *
   * @return the logger formatter.
   * @deprecated SLF4J is used for all logging. Setting a custom console logger formatter has no effect.
   */
  @Deprecated
  public LoggerFormatter consoleLoggerFormatter() {
    return DefaultLoggerFormatter.INSTANCE;
  }

  /**
   * Returns this config as a map so it can be exported into i.e. JSON for display.
   */
  @Stability.Volatile
  Map<String, Object> exportAsMap() {
    Map<String, Object> export = new LinkedHashMap<>();
    export.put("diagnosticContextEnabled", diagnosticContextEnabled);
    return export;
  }

  public static class Builder {
    private boolean diagnosticContextEnabled = Defaults.DEFAULT_DIAGNOSTIC_CONTEXT_ENABLED;

    private Builder deprecatedInFavorOfSlf4J(String clientSettingName) {
      // A user who sets these properties probably isn't using SLF4J,
      // so it doesn't make sense to log this warning via SLF4J.
      // This is a rare case where writing directly to stderr is appropriate.
      System.err.println(
        "WARN: The Couchbase SDK `" + clientSettingName + "` client setting is deprecated, and has no effect." +
          " The Couchbase SDK now uses SLF4J for all logging." +
          " Instead of customizing log output via SDK client settings," +
          " please include an appropriate SLF4J binding as a dependency of your project," +
          " and configure your chosen logging framework to generate log messages in the desired format." +
          " To learn more about SLF4J bindings, see https://www.slf4j.org/manual.html#swapping"
      );

      return this;
    }

    /**
     * This method has no effect.
     * <p>
     * SLF4J is used for all logging. If you wish to log to the console,
     * please configure your SLF4J binding accordingly.
     *
     * @param fallbackToConsole ignored
     * @return this {@link Builder} for chaining purposes.
     * @deprecated SLF4J is used for all logging.
     */
    @Deprecated
    public Builder fallbackToConsole(final boolean fallbackToConsole) {
      return deprecatedInFavorOfSlf4J("logger.fallbackToConsole");
    }

    /**
     * This method does nothing.
     *
     * @param disableSlf4J ignored
     * @return this {@link Builder} for chaining purposes.
     * @deprecated SFL4J is always used for logging.
     */
    @Deprecated
    public Builder disableSlf4J(final boolean disableSlf4J) {
      return deprecatedInFavorOfSlf4J("logger.disableSlf4J");
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
      return deprecatedInFavorOfSlf4J("logger.loggerName");
    }

    /**
     * If enabled, and the user specifies a {@code clientContext} for a request,
     * the client context is copied into the Mapped Diagnostic Context (MDC)
     * when events related to that request are logged.
     * <p>
     * Has no effect if the SLF4J binding does not support MDC.
     * <p>
     * Disabled by default.
     *
     * @param diagnosticContextEnabled if the diagnostic context should be enabled.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder enableDiagnosticContext(boolean diagnosticContextEnabled) {
      this.diagnosticContextEnabled = diagnosticContextEnabled;
      return this;
    }

    /**
     * This method is deprecated, and has no effect.
     *
     * @param consoleLogLevel ignored
     * @return this {@link Builder} for chaining purposes.
     * @deprecated This method has no effect. SLF4J is used for all logging.
     */
    @Deprecated
    public Builder consoleLogLevel(final Level consoleLogLevel) {
      return deprecatedInFavorOfSlf4J("logger.consoleLogLevel");
    }

    /**
     * This method has no effect.
     * <p>
     * If you wish to customize the log output, please configure your SLF4J binding.
     *
     * @param loggerFormatter ignored
     * @return this {@link Builder} for chaining purposes.
     * @deprecated Please configure logger output via SLF4J binding instead.
     */
    @Deprecated
    public Builder consoleLoggerFormatter(final LoggerFormatter loggerFormatter) {
      return deprecatedInFavorOfSlf4J("logger.consoleLoggerFormatter");
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
