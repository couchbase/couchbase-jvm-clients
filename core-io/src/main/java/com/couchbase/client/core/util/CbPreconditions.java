/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Stability.Internal
public class CbPreconditions {
  private CbPreconditions() {}

  public static void check(boolean condition) {
    if (!condition) throw new IllegalArgumentException();
  }

  public static void check(boolean condition, String message) {
    if (!condition) throw new IllegalArgumentException(message);
  }
  
  public static void check(boolean condition, String message, Object... args) {
    if (!condition) throw new IllegalArgumentException(format(message, args));
  }

  private static final Pattern PLACEHOLDER = Pattern.compile(Pattern.quote("{}"));

  private static String format(String message, Object... args) {
    if (args.length == 0) {
      return message;
    }

    Iterator<?> i = Arrays.asList(args).iterator();
    Matcher m = PLACEHOLDER.matcher(message);
    StringBuffer result = new StringBuffer();
    while (m.find()) {
      String replacement = i.hasNext() ? String.valueOf(i.next()) : "{}";
      m.appendReplacement(result, replacement);
    }
    m.appendTail(result);

    if (i.hasNext()) {
      Object lastExtraArg = args[args.length-1];
      if (lastExtraArg instanceof Throwable) {
        result.append("\n")
          .append(CbThrowables.getStackTraceAsString((Throwable) lastExtraArg));
      }
    }

    return result.toString();
  }
}
