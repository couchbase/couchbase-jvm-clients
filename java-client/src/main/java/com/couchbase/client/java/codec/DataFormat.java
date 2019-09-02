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

package com.couchbase.client.java.codec;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.CodecFlags;

import static com.couchbase.client.core.msg.kv.CodecFlags.BINARY_COMMON_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.BINARY_LEGACY_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.JSON_COMMON_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.JSON_LEGACY_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.PRIVATE_COMMON_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.SERIALIZED_LEGACY_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.STRING_COMMON_FLAGS;
import static com.couchbase.client.core.msg.kv.CodecFlags.STRING_LEGACY_FLAGS;

/**
 * Specifies the transcoding format for KV operations.
 *
 * <p>Changing the {@link DataFormat} has both implications on the underlying serializer used with the default
 * transcoder, but more importantly it sets certain flags on the document itself. Using the wrong data format can
 * have serious implications w.r.t interoperability with other SDKs.</p>
 */
public enum DataFormat {

  /**
   * The provided entity is going to be passed through encoding and decoding of the JSON serializer.
   *
   * <p>This type is the default ({@link #DEFAULT_DATA_FORMAT}).</p>
   */
  JSON(CodecFlags.JSON_COMPAT_FLAGS),
  /**
   * The provided data is assumed to be already serialized and so it is passed through without any modifications, only
   * the JSON interoperability flags are applied properly.
   */
  ENCODED_JSON(CodecFlags.JSON_COMPAT_FLAGS),
  /**
   * Uses the common flags for the arbitrary "string" format, which is NOT JSON.
   */
  STRING(CodecFlags.STRING_COMPAT_FLAGS),
  /**
   * Allows to set the common flags to "binary", passing through the data as is and IS NOT JSON.
   */
  BINARY(CodecFlags.BINARY_COMPAT_FLAGS),
  /**
   * Custom to java, when this data format is set the object is passed through java serialization and flags set
   * accordingly.
   *
   * <p>If this format is used, it is not going to be readable by other SDKs.</p>
   */
  OBJECT_SERIALIZATION(CodecFlags.SERIALIZED_COMPAT_FLAGS);

  /**
   * The default data format used by encoding and decoding.
   */
  public static final DataFormat DEFAULT_DATA_FORMAT = DataFormat.JSON;

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
   */
  private static final int COMMON_FORMAT_MASK = 0x0F000000;

  private final int commonFlag;

  DataFormat(int commonFlag) {
    this.commonFlag = commonFlag;
  }

  /**
   * Extracts the common flag for encoding and decoding purposes.
   */
  @Stability.Internal
  public int commonFlag() {
    return commonFlag;
  }

  /**
   * Helper method to extract the data format from the raw common flag.
   *
   * @param flags the common flag as input.
   * @return the data format extracted.
   */
  public static DataFormat fromCommonFlag(final int flags) {
    if (hasFlags(flags, JSON_COMMON_FLAGS, JSON_LEGACY_FLAGS)) {
      return DataFormat.JSON;
    } else if (hasFlags(flags, BINARY_COMMON_FLAGS, BINARY_LEGACY_FLAGS)) {
      return DataFormat.BINARY;
    } else if (hasFlags(flags, STRING_COMMON_FLAGS, STRING_LEGACY_FLAGS)) {
      return DataFormat.STRING;
    } else if (hasFlags(flags, PRIVATE_COMMON_FLAGS, SERIALIZED_LEGACY_FLAGS)) {
      return DataFormat.OBJECT_SERIALIZATION;
    }
    throw new CouchbaseException("Unknown common flags: 0x" + Integer.toHexString(flags));
  }

  /**
   * Checks that flags has common flags bits set and that they correspond to expected common flags format.
   *
   * @param flags the 32 bits flags to check
   * @param expectedCommonFlag the expected common flags format bits
   * @return true if common flags bits are set and correspond to expectedCommonFlag format
   */
  private static boolean hasCommonFormat(final int flags, final int expectedCommonFlag) {
    return (flags & COMMON_FORMAT_MASK) == expectedCommonFlag;
  }

  /**
   * Utility method to correctly check a flag has a certain type, by checking
   * that either the corresponding flags are set in the common flags bits or
   * the flag is a legacy flag of the correct type.
   *
   * @param flags the flags to be checked.
   * @param expectedCommonFlags the common flags for the expected type
   * @param expectedLegacyFlag the legacy flags for the expected type
   * @return true if flags conform to the correct common flags or legacy flags
   */
  private static boolean hasFlags(final int flags, final int expectedCommonFlags, final int expectedLegacyFlag) {
    return hasCommonFormat(flags, expectedCommonFlags) || flags == expectedLegacyFlag;
  }

}
