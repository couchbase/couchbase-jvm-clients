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

package com.couchbase.client.core.logging;

import static java.util.Objects.requireNonNull;

/**
 * Static utility methods for global log redaction settings.
 */
public class LogRedaction {
  private static volatile RedactionLevel redactionLevel = RedactionLevel.NONE;

  private LogRedaction() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Returns the current redaction level.
   */
  public static RedactionLevel getRedactionLevel() {
    return redactionLevel;
  }

  /**
   * Changes the redaction level.
   */
  public static void setRedactionLevel(RedactionLevel redactionLevel) {
    LogRedaction.redactionLevel = requireNonNull(redactionLevel, "redactionLevel");
  }
}
