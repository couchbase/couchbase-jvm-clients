/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.core.error.CouchbaseException;
import reactor.util.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Formatter;
import java.util.Locale;
import java.util.logging.Level;

/**
 * The default implementation for the {@link LoggerFormatter}.
 */
public class DefaultLoggerFormatter implements LoggerFormatter {

  private static final String CHARSET_NAME = StandardCharsets.UTF_8.displayName();

  public static final DefaultLoggerFormatter INSTANCE = new DefaultLoggerFormatter();

  @Override
  public String format(final Level logLevel, final String message, final @Nullable Throwable throwable) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);

    try (Formatter formatter = new Formatter(ps, Locale.ROOT)) {
      String currentThread = Thread.currentThread().getName();
      String convertedLevel = logLevel(logLevel);
      String timestamp = timestamp();
      if (timestamp != null && !timestamp.isEmpty()) {
        timestamp = timestamp + " ";
      }

      if (throwable == null) {
        formatter.format("%s[%s] (%s) %s%n", timestamp, convertedLevel, currentThread, message);
      } else {
        formatter.format("%s[%s] (%s) %s - %s%n", timestamp, convertedLevel, currentThread, message, throwable);
        throwable.printStackTrace(ps);
      }

      try {
        return os.toString(CHARSET_NAME);
      } catch (UnsupportedEncodingException e) {
        throw new CouchbaseException("Unsupported charset while formatting log: " + CHARSET_NAME, e);
      }
    }
  }

  /**
   * Returns a timestamp to be included in the default output log format.
   *
   * @return a timestamp or date format, might be null or empty.
   */
  protected String timestamp() {
    return "";
  }

  /**
   * Converts the log level enum to its string representation.
   * <p>
   * Note that in the default implementation, the aim is to preserve the length of the log levels
   * so some levels are prefixed with whitespace - this is not a typo.
   *
   * @param level the level to convert.
   * @return the converted level.
   */
  protected String logLevel(final Level level) {
    switch (level.getName()) {
      case "SEVERE":
        return "ERROR";
      case "WARNING":
          return " WARN";
      case "INFO":
        return " INFO";
      case "CONFIG":
      case "FINE":
      case "FINER":
        return "DEBUG";
      case "FINEST":
        return "TRACE";
      default:
        throw new IllegalStateException("Unsupported Log Level: " + level);
    }
  }

}
