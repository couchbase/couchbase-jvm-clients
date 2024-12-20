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

@Stability.Internal
public class CbStrings {
  private CbStrings() {
    throw new AssertionError("not instantiable");
  }

  public static final String MIN_CODE_POINT_AS_STRING = codePointToString(Character.MIN_CODE_POINT);
  public static final String MAX_CODE_POINT_AS_STRING = codePointToString(Character.MAX_CODE_POINT);

  /**
   * Backport of Java 11's {@code Character.toString(int codePoint)}.
   */
  public static String codePointToString(int codePoint) {
    return new String(Character.toChars(codePoint));
  }

  public static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }

  public static String emptyToNull(String s) {
    return isNullOrEmpty(s) ? null : s;
  }

  public static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }

  public static String removeStart(String s, String removeMe) {
    return s.startsWith(removeMe) ? s.substring(removeMe.length()) : s;
  }

  public static String removeEnd(String s, String removeMe) {
    return s.endsWith(removeMe) ? s.substring(0, s.length() - removeMe.length()) : s;
  }
}
