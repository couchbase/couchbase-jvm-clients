/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Defines useful constants and methods regarding bytes.
 */
public class Bytes {

  /**
   * Holds an empty byte array, so we do not need to create one every time.
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[]{};

  private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

  public static byte[] readAllBytes(InputStream is) {
    notNull(is, "input stream");
    try {
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[4 * 1024];
      int len;
      while ((len = is.read(buffer)) != -1) {
        result.write(buffer, 0, len);
      }
      return result.toByteArray();
    } catch (IOException e) {
      throw new CouchbaseException("Failed to read bytes from stream.", e);
    }
  }

  /**
   * Returns the bytes as hex digits, two lowercase digits per byte, with no separators
   * (for example, "00ff10" for the bytes 0x00, 0xff and 0x10).
   */
  @Stability.Internal
  public static String toHex(byte[] bytes) {
    char[] result = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      result[i * 2] = HEX_DIGITS[(bytes[i] >> 4) & 0xf];
      result[i * 2 + 1] = HEX_DIGITS[bytes[i] & 0xf];
    }
    return new String(result);
  }

  /**
   * Returns a hex dump of the bytes, in the same format as Netty's {@code ByteBufUtil.prettyHexDump}.
   */
  @Stability.Internal
  public static String prettyHexDump(byte[] bytes) {
    return prettyHexDump(bytes, 0);
  }

  /**
   * Like {@link #prettyHexDump(byte[])}, except that each row is labelled with its offset counting from the given offset
   * (for example, for a chunk of a response body after the first).
   */
  @Stability.Internal
  public static String prettyHexDump(byte[] bytes, long startOffset) {
    if (bytes.length == 0) {
      return "";
    }
    String newline = System.lineSeparator(); // like Netty's
    StringBuilder sb = new StringBuilder()
      .append("         +-------------------------------------------------+").append(newline)
      .append("         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |").append(newline)
      .append("+--------+-------------------------------------------------+----------------+");
    for (int row = 0; row < bytes.length; row += 16) {
      sb.append(newline).append('|');
      appendHex(sb, startOffset + row, 8);
      sb.append('|');
      StringBuilder ascii = new StringBuilder();
      for (int i = row; i < row + 16; i++) {
        if (i < bytes.length) {
          int b = bytes[i] & 0xff;
          sb.append(' ');
          appendHex(sb, b, 2);
          ascii.append(b >= 0x20 && b < 0x7f ? (char) b : '.');
        } else {
          sb.append("   ");
          ascii.append(' ');
        }
      }
      sb.append(" |").append(ascii).append('|');
    }
    return sb.append(newline).append("+--------+-------------------------------------------------+----------------+").toString();
  }

  /**
   * Appends the value's lowest {@code digits} hex digits, with leading zeros: like String.format("%0<digits>x")
   * for values that fit, without its cost (hex dumps format every byte).
   */
  private static void appendHex(StringBuilder sb, long value, int digits) {
    for (int shift = (digits - 1) * 4; shift >= 0; shift -= 4) {
      sb.append(HEX_DIGITS[(int) (value >>> shift) & 0xf]);
    }
  }
}

