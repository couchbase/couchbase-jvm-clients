/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

public enum CodecFlags {
  ;

  /**
   * 32bit flag is composed of:
   *  - 3 compression bits
   *  - 1 bit reserved for future use
   *  - 4 format flags bits. those 8 upper bits make up the common flags
   *  - 8 bits reserved for future use
   *  - 16 bits for legacy flags
   *
   * This mask allows to compare a 32 bits flags with the 4 common flag format bits
   * ("00001111 00000000 00000000 00000000").
   *
   * @see #extractCommonFlags(int)
   * @see #hasCommonFlags(int)
   * @see #hasCompressionFlags(int)
   */
  public static final int COMMON_FORMAT_MASK = 0x0F000000;

  public static final int PRIVATE_COMMON_FLAGS = createCommonFlags(CommonFlags.PRIVATE.ordinal());
  public static final int JSON_COMMON_FLAGS = createCommonFlags(CommonFlags.JSON.ordinal());
  public static final int BINARY_COMMON_FLAGS = createCommonFlags(CommonFlags.BINARY.ordinal());
  public static final int STRING_COMMON_FLAGS = createCommonFlags(CommonFlags.STRING.ordinal());

  public static final int SERIALIZED_LEGACY_FLAGS = 1;
  public static final int BINARY_LEGACY_FLAGS = (8 << 8);
  public static final int STRING_LEGACY_FLAGS = 0;
  public static final int JSON_LEGACY_FLAGS = STRING_LEGACY_FLAGS;
  public static final int BOOLEAN_LEGACY_FLAGS = STRING_LEGACY_FLAGS;
  public static final int LONG_LEGACY_FLAGS = STRING_LEGACY_FLAGS;
  public static final int DOUBLE_LEGACY_FLAGS = STRING_LEGACY_FLAGS;


  public static final int SERIALIZED_COMPAT_FLAGS = PRIVATE_COMMON_FLAGS  | SERIALIZED_LEGACY_FLAGS;
  public static final int JSON_COMPAT_FLAGS       = JSON_COMMON_FLAGS     | JSON_LEGACY_FLAGS;
  public static final int BINARY_COMPAT_FLAGS     = BINARY_COMMON_FLAGS   | BINARY_LEGACY_FLAGS;
  public static final int BOOLEAN_COMPAT_FLAGS    = JSON_COMMON_FLAGS     | BOOLEAN_LEGACY_FLAGS;
  public static final int LONG_COMPAT_FLAGS       = JSON_COMMON_FLAGS     | LONG_LEGACY_FLAGS;
  public static final int DOUBLE_COMPAT_FLAGS     = JSON_COMMON_FLAGS     | DOUBLE_LEGACY_FLAGS;
  public static final int STRING_COMPAT_FLAGS     = STRING_COMMON_FLAGS   | STRING_LEGACY_FLAGS;

  /**
   * Takes a integer representation of flags and moves them to the common flags MSBs.
   *
   * @param flags the flags to shift.
   * @return an integer having the common flags set.
   */
  public static int createCommonFlags(final int flags) {
    return flags << 24;
  }

  /**
   * Checks whether the upper 8 bits are set, indicating common flags presence.
   *
   * It does this by shifting bits to the right until only the most significant
   * bits are remaining and then checks if one of them is set.
   *
   * @param flags the flags to check.
   * @return true if set, false otherwise.
   */
  public static boolean hasCommonFlags(final int flags) {
    return (flags >> 24) > 0;
  }

  /**
   * Checks that flags has common flags bits set and that they correspond to expected common flags format.
   *
   * @param flags the 32 bits flags to check
   * @param expectedCommonFlag the expected common flags format bits
   * @return true if common flags bits are set and correspond to expectedCommonFlag format
   */
  public static boolean hasCommonFormat(final int flags,
                                        final int expectedCommonFlag) {
    return hasCommonFlags(flags) && (flags & COMMON_FORMAT_MASK) == expectedCommonFlag;
  }

  /**
   * Returns only the common flags from the full flags.
   *
   * @param flags the flags to check.
   * @return only the common flags simple representation (8 bits).
   */
  public static int extractCommonFlags(final int flags) {
    return flags >> 24;
  }

  /**
   * Returns just the 4 bit format field from the common flags.
   *
   * @param userFlags the full user flags flags to check.
   * @return only the format portion of the common flags simple representation (4 bits).
   */
  public static int extractCommonFormatFlags(final int userFlags) {
    return extractCommonFlags(userFlags) & 0x0f;
  }

  /**
   * Checks whether the upper 3 bits are set, indicating compression presence.
   *
   * It does this by shifting bits to the right until only the most significant
   * bits are remaining and then checks if one of them is set.
   *
   * @param flags the flags to check.
   * @return true if compression set, false otherwise.
   */
  public static boolean hasCompressionFlags(final int flags) {
    return (flags >> 29) > 0;
  }

  /**
   * The common flags enum.
   */
  public enum CommonFlags {
    RESERVED,
    PRIVATE,
    JSON,
    BINARY,
    STRING
  }

}
