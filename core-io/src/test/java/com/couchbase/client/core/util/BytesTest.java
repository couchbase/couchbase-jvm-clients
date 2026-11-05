/*
 * Copyright 2026 Couchbase, Inc.
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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BytesTest {

  @Test
  void toHexUsesTwoLowercaseDigitsPerByte() {
    assertEquals("", Bytes.toHex(Bytes.EMPTY_BYTE_ARRAY));
    assertEquals("00ff10", Bytes.toHex(new byte[]{0x00, (byte) 0xff, 0x10}));
  }

  @Test
  void toHexHandlesEveryByteValue() {
    byte[] everyByte = new byte[256];
    StringBuilder expected = new StringBuilder();
    for (int i = 0; i < everyByte.length; i++) {
      everyByte[i] = (byte) i;
      expected.append(String.format("%02x", i));
    }
    assertEquals(expected.toString(), Bytes.toHex(everyByte));
  }

  @Test
  void hexDumpOfNothingIsEmpty() {
    assertEquals("", Bytes.prettyHexDump(Bytes.EMPTY_BYTE_ARRAY));
  }

  @Test
  void hexDumpOfOneFullRow() {
    byte[] bytes = "Hello, Couchbase".getBytes(StandardCharsets.UTF_8);
    assertEquals(
      lines(
        "         +-------------------------------------------------+",
        "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |",
        "+--------+-------------------------------------------------+----------------+",
        "|00000000| 48 65 6c 6c 6f 2c 20 43 6f 75 63 68 62 61 73 65 |Hello, Couchbase|",
        "+--------+-------------------------------------------------+----------------+"
      ),
      Bytes.prettyHexDump(bytes)
    );
  }

  @Test
  void hexDumpPadsTheLastRowAndShowsUnprintableBytesAsDots() {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    byte[] text = "Hello, Couchbase!".getBytes(StandardCharsets.UTF_8);
    bytes.write(text, 0, text.length);
    bytes.write(0x00);
    bytes.write(0x7f);
    bytes.write(0xff);

    assertEquals(
      lines(
        "         +-------------------------------------------------+",
        "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |",
        "+--------+-------------------------------------------------+----------------+",
        "|00000000| 48 65 6c 6c 6f 2c 20 43 6f 75 63 68 62 61 73 65 |Hello, Couchbase|",
        "|00000010| 21 00 7f ff                                     |!...            |",
        "+--------+-------------------------------------------------+----------------+"
      ),
      Bytes.prettyHexDump(bytes.toByteArray())
    );
  }

  private static String lines(String... lines) {
    return String.join(System.lineSeparator(), lines);
  }
}
