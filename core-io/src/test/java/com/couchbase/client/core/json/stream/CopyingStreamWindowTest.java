/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.json.stream;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CopyingStreamWindowTest {
  @Test
  void smokeTest() {
    CopyingStreamWindow window = new CopyingStreamWindow();
    assertArrayEquals(new byte[0], window.getBytes(0, 0));

    window.add("hello".getBytes(UTF_8));
    assertEquals("ello", new String(window.getBytes(1, 5), UTF_8));

    assertEquals("ello\0", new String(window.getBytes(1, 6), UTF_8));

    window.add(new byte[1000]);
    window.add("world".getBytes(UTF_8));
    assertEquals("world", new String(window.getBytes(1005, 1010), UTF_8));

    window.releaseBefore(1005);
    assertEquals("world", new String(window.getBytes(1005, 1010), UTF_8));

    assertThrows(IndexOutOfBoundsException.class, () -> window.getBytes(1, 5));
    assertThrows(IndexOutOfBoundsException.class, () -> window.getBytes(0, 0));
  }
}
