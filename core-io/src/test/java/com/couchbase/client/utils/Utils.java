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

package com.couchbase.client.utils;

import java.io.InputStream;
import java.util.function.BooleanSupplier;

/**
 * Provides a bunch of utility APIs that help with testing.
 */
public class Utils {

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
   * Waits and sleeps for a little bit of time until the given condition is met.
   *
   * <p>Sleeps 10ms between "false" invocations.</p>
   *
   * @param supplier return true once it should stop waiting.
   */
  public static void waitUntilCondition(final BooleanSupplier supplier) {
    while (!supplier.getAsBoolean()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Reads a file from the resources folder (in the same path as the requesting test class).
   *
   * <p>The class will be automatically loaded relative to the namespace and converted
   * to a string.</p>
   *
   * @param filename the filename of the resource.
   * @param clazz    the reference class.
   * @return the loaded string.
   */
  public static String readResource(final String filename, final Class<?> clazz) {
    String path = "/" + clazz.getPackage().getName().replace(".", "/") + "/" + filename;
    InputStream stream = clazz.getResourceAsStream(path);
    java.util.Scanner s = new java.util.Scanner(stream).useDelimiter("\\A");
    return s.hasNext() ? s.next() : "";
  }
}
