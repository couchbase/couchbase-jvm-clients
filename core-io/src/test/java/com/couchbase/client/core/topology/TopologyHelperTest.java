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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.topology.TopologyHelper.compressKeyRuns;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TopologyHelperTest {

  @Test
  void canCompressEmptyMap() {
    check(
      "{}",
      emptyMap()
    );
  }

  @Test
  void canCompressSingletonMap() {
    check(
      "{1=A}",
      mapOf(1, "A")
    );
  }

  @Test
  void canCloseRange() {
    check(
      "{1..2=A}",
      mapOf(
        1, "A",
        2, "A"
      )
    );
  }

  @Test
  void gapsPreventRangeMerging() {
    check(
      "{1=A, 3=A}",
      mapOf(
        1, "A",
        3, "A"
      )
    );
  }

  @Test
  void twoRanges() {
    check(
      "{1..3=A, 4..5=B}",
      mapOf(
        1, "A",
        2, "A",
        3, "A",
        4, "B",
        5, "B"
      )
    );
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  void throwsNpeForNullMap() {
    assertThrows(NullPointerException.class, () -> compressKeyRuns(null));
  }

  @Test
  void throwsNpeForNullKey() {
    Map<Integer, String> map = new HashMap<>();
    map.put(null, "A");
    assertThrows(NullPointerException.class, () -> compressKeyRuns(map));
  }

  @Test
  void acceptsNullValue() {
    Map<Integer, String> map = new HashMap<>();

    map.put(1, null);
    check(
      "{1=null}",
      map
    );

    map.put(2,null);
    check(
      "{1..2=null}",
      map
    );
  }

  private static <V> void check(String expected, Map<Integer, V> map) {
    assertEquals(
      expected,
      compressKeyRuns(map).toString()
    );
  }

}
