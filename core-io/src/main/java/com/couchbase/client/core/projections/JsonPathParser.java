/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.core.projections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses a JSON projections string into a Seq of `PathElement`s.
 * <p>
 * E.g. "foo.bar" or "foo[2].bar"
 * <p>
 *
 * @since 1.0.0
 */
public class JsonPathParser {
  private JsonPathParser() {
  }

  static String debugPos(int idx) {
    return "position " + idx;
  }

  public static List<PathElement> parse(String path) {
    int elemIdx = 0;
    int idx = 0;
    int len = path.length();
    ArrayList ret = new ArrayList<PathElement>();

    while (idx < len) {
      char ch = path.charAt(idx);
      idx += 1;

      if (ch == '.') {
        String first = path.substring(elemIdx, idx - 1);
        elemIdx = idx;
        ret.add(new PathObjectOrField(first));
      } else if (ch == '[') {
        int arrayIdxStart = idx;
        Integer out = null;

        while (idx < len && out == null) {
          int arrayIdxCh = path.charAt(idx);

          if (arrayIdxCh == ']') {
            String arrayIdxStr = path.substring(arrayIdxStart, idx);
            out = Integer.parseInt(arrayIdxStr);
          } else if (!(arrayIdxCh >= '0' && arrayIdxCh <= '9')) {
            throw new IllegalArgumentException("Found unexpected non-digit in middle of array index at " + debugPos(idx));
          }

          idx += 1;
        }

        if (out == null) {
          throw new IllegalArgumentException("Could not find ']' to complete array index");
        }

        String first = path.substring(elemIdx, arrayIdxStart - 1);
        elemIdx = idx;
        ret.add(new PathArray(first, out));

        // In "foo[2].bar", skip over the .
        if (idx < len && path.charAt(idx) == '.') {
          idx += 1;
          elemIdx = idx;
        }
      }
    }

    if (idx != elemIdx) {
      String first = path.substring(elemIdx, idx);
      ret.add(new PathObjectOrField(first));
    }

    return Collections.unmodifiableList(ret);
  }
}
