/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.topology;

import com.couchbase.client.core.annotation.Stability;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class TopologyHelper {
  private TopologyHelper() {
  }

  /**
   * Returns new map derived by merging contiguous keys that have the same value.
   * Key ranges are expressed as: {@code START_INCLUSIVE..END_INCLUSIVE}
   * <p>
   * For example:
   * <pre>
   * Given:   {0=A, 1=A, 3=A, 4=B, 5=A}
   * Returns: {0..1=A, 3=A, 4=B, 5=A}
   * </pre>
   *
   * @throws NullPointerException if map is null or contains a null key
   */
  public static <V> LinkedHashMap<String, V> compressKeyRuns(Map<Integer, V> map) {
    SortedMap<Integer, V> sorted = map instanceof SortedMap ? ((SortedMap<Integer, V>) map) : new TreeMap<>(map);

    LinkedHashMap<String, V> result = new LinkedHashMap<>();

    Integer runStart = null;
    V runValue = null;
    Integer prevKey = null;

    for (Map.Entry<Integer, V> entry : sorted.entrySet()) {
      Integer key = requireNonNull(entry.getKey(), "map had null key");
      V value = entry.getValue();

      if (prevKey == null) {
        // first entry; start a new run
        runStart = key;
        runValue = value;

        prevKey = key;
        continue;
      }

      if (Objects.equals(value, runValue) && key.equals(prevKey + 1)) {
        // extend the current run
        prevKey = key;
        continue;
      }

      // end previous run and start a new run
      result.put(closedIntRangeToString(runStart, prevKey), runValue);
      runStart = key;
      runValue = value;

      prevKey = key;
    }

    // finish the final range
    if (prevKey != null) {
      result.put(closedIntRangeToString(runStart, prevKey), runValue);
    }
    return result;
  }

  private static String closedIntRangeToString(int start, int end) {
    return start == end
      ? Integer.toString(start)
      : start + ".." + end;
  }
}
