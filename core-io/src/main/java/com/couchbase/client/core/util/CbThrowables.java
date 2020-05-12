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

import com.couchbase.client.core.annotation.Stability;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CbThrowables {
  private CbThrowables() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Walks the causal chain of the given throwable (starting with the given throwable itself)
   * and returns the first throwable that is an instance of the specified type.
   */
  public static <T extends Throwable> Optional<T> findCause(Throwable t, final Class<T> type) {
    for (; t != null; t = t.getCause()) {
      if (type.isAssignableFrom(t.getClass())) {
        return Optional.of(type.cast(t));
      }
    }
    return Optional.empty();
  }

  /**
   * Returns true if the given throwable or any throwable in its causal chain is an instance of the given type.
   */
  public static boolean hasCause(Throwable t, final Class<? extends Throwable> type) {
    return findCause(t, type).isPresent();
  }

  /**
   * If the given Throwable is an instance of RuntimeException or Error, throw it.
   * Otherwise do nothing.
   */
  public static void throwIfUnchecked(Throwable t) {
    requireNonNull(t);
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
  }

  /**
   * If the given Throwable is an instance of the given class, throw it.
   * Otherwise do nothing.
   */
  public static <T extends Throwable> void throwIfInstanceOf(Throwable t, Class<T> clazz) throws T {
    requireNonNull(t);
    if (clazz.isInstance(t)) {
      throw clazz.cast(t);
    }
  }
}
