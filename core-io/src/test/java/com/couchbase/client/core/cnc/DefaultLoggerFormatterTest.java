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

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

class DefaultLoggerFormatterTest {

  @Test
  void formatWithoutException() {
    String currentThread = Thread.currentThread().getName();

    DefaultLoggerFormatter formatter = DefaultLoggerFormatter.INSTANCE;
    String result = formatter.format(Level.INFO, "my message", null);
    assertEquals("[ INFO] (" + currentThread + ") my message\n", result);
  }

  @Test
  void formatWithException() {
    String currentThread = Thread.currentThread().getName();

    DefaultLoggerFormatter formatter = DefaultLoggerFormatter.INSTANCE;
    String result = formatter.format(Level.INFO, "my message", new Exception());
    assertTrue(result.contains("[ INFO] (" + currentThread + ") my message - java.lang.Exception"));
    assertTrue(result.contains("com.couchbase.client.core.cnc.DefaultLoggerFormatterTest"));
  }

  @Test
  void appliesCustomTimestamp() {
    String now = LocalDateTime.now().toString();

    DefaultLoggerFormatter formatter = new DefaultLoggerFormatter() {
      @Override
      protected String timestamp() {
        return now;
      }
    };

    String result = formatter.format(Level.FINE, "some other msg", null);
    assertEquals(now + " [DEBUG] (main) some other msg\n", result);
  }

}