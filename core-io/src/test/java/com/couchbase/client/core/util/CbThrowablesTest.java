/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.util;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.couchbase.client.core.util.CbThrowables.filterStackTrace;
import static com.couchbase.client.core.util.CbThrowables.getStackTraceAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbThrowablesTest {

  /**
   * If multiple exceptions in the causal chain meet the criteria,
   * expect the result to be the one nearest to the given throwable (farthest from the root cause).
   */
  @Test
  void findsNearestCause() {
    Optional<IllegalArgumentException> result = CbThrowables.findCause(
        new RuntimeException(
            new IllegalArgumentException("foo",
                new IllegalArgumentException("bar"))),
        IllegalArgumentException.class);

    assertTrue(result.isPresent());
    assertEquals("foo", result.get().getMessage());
  }

  /**
   * If the given exception matches the criteria, return it (even though it's not technically a "cause")
   * expect the result to be the one closest to the top of the chain.
   */
  @Test
  void findRoot() {
    Optional<IllegalArgumentException> result = CbThrowables.findCause(
        new IllegalArgumentException("foo",
            new IllegalArgumentException("bar")),
        IllegalArgumentException.class);

    assertTrue(result.isPresent());
    assertEquals("foo", result.get().getMessage());
  }

  @Test
  void findBySuperclass() {
    Optional<RuntimeException> result = CbThrowables.findCause(
        new Exception("foo",
            new IllegalArgumentException("foo")),
        RuntimeException.class);

    assertTrue(result.isPresent());
    assertEquals(IllegalArgumentException.class, result.get().getClass());
    assertEquals("foo", result.get().getMessage());
  }

  @Test
  void throwIfUncheckedWorks() {
    // should not throw
    CbThrowables.throwIfUnchecked(new Exception("oops, should not have throw this"));

    assertThrows(IndexOutOfBoundsException.class, () ->
        CbThrowables.throwIfUnchecked(new IndexOutOfBoundsException()));
  }

  @Test
  void canFilterStackTraceWithCircularCause() {
    Throwable a = new RuntimeException();
    Throwable b = new RuntimeException(a);
    a.initCause(b);
    filterStackTrace(a, frame -> true);
  }

  @Test
  void canFilterStackTraceWithCircularSuppression() {
    Throwable a = new RuntimeException();
    Throwable b = new RuntimeException();
    a.addSuppressed(b);
    b.addSuppressed(a);
    filterStackTrace(a, frame -> true);
  }

  @Test
  void canFilterStackTrace() {
    String className = getClass().getName();

    Throwable t = new RuntimeException(new RuntimeException(new RuntimeException()));
    t.addSuppressed(new RuntimeException(new RuntimeException()));

    assertTrue(getStackTraceAsString(t).contains(className)); // sanity check

    filterStackTrace(t, frame -> !frame.getClassName().equals(className));

    String s = getStackTraceAsString(t);
    assertFalse(
      s.contains(className),
      "Stack trace should not contain " + className + ", but got: " + s);
  }
}
