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

package com.couchbase.client.test;

import org.awaitility.core.ThrowingRunnable;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.MissingResourceException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.with;

/**
 * Provides a bunch of utility APIs that help with testing.
 */
public class Util {

  /**
   * Waits and sleeps for a little bit of time until the given condition is met.
   *
   * <p>Sleeps 1ms between "false" invocations. It will wait at most one minute to prevent hanging forever in case
   * the condition never becomes true.</p>
   *
   * @param supplier return true once it should stop waiting.
   */
  public static void waitUntilCondition(final BooleanSupplier supplier) {
    waitUntilCondition(supplier, Duration.ofMinutes(1));
  }

  public static void waitUntilCondition(final ThrowingRunnable runnable) {
    waitUntilCondition(runnable, Duration.ofMinutes(1));
  }

  public static void waitUntilCondition(final ThrowingRunnable runnable, Duration atMost) {
    with().pollInterval(Duration.ofMillis(1)).await().atMost(atMost).untilAsserted(runnable);
  }

  public static void waitUntilCondition(final BooleanSupplier supplier, Duration atMost) {
    with().pollInterval(Duration.ofMillis(1)).await().atMost(atMost).until(supplier::getAsBoolean);
  }

  public static void waitUntilCondition(final BooleanSupplier supplier, Duration atMost, Duration delay) {
    with().pollInterval(delay).await().atMost(atMost).until(supplier::getAsBoolean);
  }

  public static void waitUntilThrows(final Class<? extends Exception> clazz, final Supplier<Object> supplier) {
    with()
      .pollInterval(Duration.ofMillis(1))
      .await()
      .atMost(Duration.ofMinutes(1))
      .until(() -> {
        try {
          supplier.get();
        } catch (final Exception ex) {
          return ex.getClass().isAssignableFrom(clazz);
        }
        return false;
      });
  }

  /**
   * Returns true if a thread with the given name is currently running.
   *
   * @param name the name of the thread.
   * @return true if running, false otherwise.
   */
  public static boolean threadRunning(final String name) {
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().equalsIgnoreCase(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reads a class path resource from a location relative to the given class's package.
   *
   * @param resourceName the name of the resource
   * @param context the class whose {@link Class#getResourceAsStream(String)} method is used to load the resource.
   * @return the resource as a string, decoded using UTF-8.
   * @throws MissingResourceException if resource is not found.
   * @see Resources
   */
  public static String readResource(final String resourceName, final Class<?> context) {
    return Resources.from(context).getString(resourceName);
  }

  static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name())
        .replace("+", "%20"); // Make sure spaces are encoded as "%20"
      // so the result can be used in path components and with "application/x-www-form-urlencoded"
    } catch (UnsupportedEncodingException inconceivable) {
      throw new AssertionError("UTF-8 not supported", inconceivable);
    }
  }
}
