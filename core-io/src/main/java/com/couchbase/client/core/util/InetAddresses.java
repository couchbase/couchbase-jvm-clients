/*
 * Copyright (c) 2008 The Guava Authors
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

import org.jspecify.annotations.NullUnmarked;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Static utility methods pertaining to InetAddress instances.
 *
 * @author Erik Kline
 * @since 5.0
 */
@NullUnmarked
final class InetAddresses {

  private static final int IPV4_PART_COUNT = 4;
  private static final int IPV6_PART_COUNT = 8;

  private InetAddresses() {}

  /**
   * Returns {@code true} if the supplied string is a valid IP string literal, {@code false}
   * otherwise.
   *
   * @param ipString {@code String} to evaluated as an IP string literal
   * @return {@code true} if the argument is a valid IP string literal
   */
  static boolean isInetAddress(String ipString) {
    return ipStringToBytes(ipString) != null;
  }

  private static byte[] ipStringToBytes(String ipString) {
    // Make a first pass to categorize the characters in this string.
    boolean hasColon = false;
    boolean hasDot = false;
    for (int i = 0; i < ipString.length(); i++) {
      char c = ipString.charAt(i);
      if (c == '.') {
        hasDot = true;
      } else if (c == ':') {
        if (hasDot) {
          return null; // Colons must not appear after dots.
        }
        hasColon = true;
      } else if (Character.digit(c, 16) == -1) {
        return null; // Everything else must be a decimal or hex digit.
      }
    }

    // Now decide which address family to parse.
    if (hasColon) {
      if (hasDot) {
        ipString = convertDottedQuadToHex(ipString);
        if (ipString == null) {
          return null;
        }
      }
      return textToNumericFormatV6(ipString);
    } else if (hasDot) {
      return textToNumericFormatV4(ipString);
    }
    return null;
  }

  private static byte[] textToNumericFormatV4(String ipString) {
    byte[] bytes = new byte[IPV4_PART_COUNT];
    int i = 0;
    try {

      for (String octet : ipString.split("\\.", IPV4_PART_COUNT + 1)) {
        bytes[i++] = parseOctet(octet);
      }
    } catch (NumberFormatException ex) {
      return null;
    }

    return i == IPV4_PART_COUNT ? bytes : null;
  }

  private static byte[] textToNumericFormatV6(String ipString) {
    // An address can have [2..8] colons, and N colons make N+1 parts.
    List<String> parts = Arrays.asList(ipString.split(":", IPV6_PART_COUNT + 2));
    if (parts.size() < 3 || parts.size() > IPV6_PART_COUNT + 1) {
      return null;
    }

    // Disregarding the endpoints, find "::" with nothing in between.
    // This indicates that a run of zeroes has been skipped.
    int skipIndex = -1;
    for (int i = 1; i < parts.size() - 1; i++) {
      if (parts.get(i).length() == 0) {
        if (skipIndex >= 0) {
          return null; // Can't have more than one ::
        }
        skipIndex = i;
      }
    }

    int partsHi; // Number of parts to copy from above/before the "::"
    int partsLo; // Number of parts to copy from below/after the "::"
    if (skipIndex >= 0) {
      // If we found a "::", then check if it also covers the endpoints.
      partsHi = skipIndex;
      partsLo = parts.size() - skipIndex - 1;
      if (parts.get(0).length() == 0 && --partsHi != 0) {
        return null; // ^: requires ^::
      }

      if (parts.get(parts.size() - 1).length() == 0 && --partsLo != 0) {
        return null; // :$ requires ::$
      }
    } else {
      // Otherwise, allocate the entire address to partsHi. The endpoints
      // could still be empty, but parseHextet() will check for that.
      partsHi = parts.size();
      partsLo = 0;
    }

    // If we found a ::, then we must have skipped at least one part.
    // Otherwise, we must have exactly the right number of parts.
    int partsSkipped = IPV6_PART_COUNT - (partsHi + partsLo);
    if (!(skipIndex >= 0 ? partsSkipped >= 1 : partsSkipped == 0)) {
      return null;
    }

    // Now parse the hextets into a byte array.
    ByteBuffer rawBytes = ByteBuffer.allocate(2 * IPV6_PART_COUNT);
    try {
      for (int i = 0; i < partsHi; i++) {
        rawBytes.putShort(parseHextet(parts.get(i)));
      }
      for (int i = 0; i < partsSkipped; i++) {
        rawBytes.putShort((short) 0);
      }
      for (int i = partsLo; i > 0; i--) {
        rawBytes.putShort(parseHextet(parts.get(parts.size() - i)));
      }
    } catch (NumberFormatException ex) {
      return null;
    }
    return rawBytes.array();
  }

  private static String convertDottedQuadToHex(String ipString) {
    int lastColon = ipString.lastIndexOf(':');
    String initialPart = ipString.substring(0, lastColon + 1);
    String dottedQuad = ipString.substring(lastColon + 1);
    byte[] quad = textToNumericFormatV4(dottedQuad);
    if (quad == null) {
      return null;
    }
    String penultimate = Integer.toHexString(((quad[0] & 0xff) << 8) | (quad[1] & 0xff));
    String ultimate = Integer.toHexString(((quad[2] & 0xff) << 8) | (quad[3] & 0xff));
    return initialPart + penultimate + ":" + ultimate;
  }

  private static byte parseOctet(String ipPart) {
    // Note: we already verified that this string contains only hex digits.
    int octet = Integer.parseInt(ipPart);
    // Disallow leading zeroes, because no clear standard exists on
    // whether these should be interpreted as decimal or octal.
    if (octet > 255 || (ipPart.startsWith("0") && ipPart.length() > 1)) {
      throw new NumberFormatException();
    }
    return (byte) octet;
  }

  private static short parseHextet(String ipPart) {
    // Note: we already verified that this string contains only hex digits.
    int hextet = Integer.parseInt(ipPart, 16);
    if (hextet > 0xffff) {
      throw new NumberFormatException();
    }
    return (short) hextet;
  }

}
