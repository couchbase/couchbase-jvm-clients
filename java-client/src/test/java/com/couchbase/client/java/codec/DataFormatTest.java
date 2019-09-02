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

import com.couchbase.client.core.msg.kv.CodecFlags;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the functionality of the {@link DataFormat}.
 */
class DataFormatTest {

  @Test
  void decodesJsonFlags() {
    assertEquals(DataFormat.JSON, DataFormat.fromCommonFlag(CodecFlags.JSON_COMMON_FLAGS));
    assertEquals(DataFormat.JSON, DataFormat.fromCommonFlag(CodecFlags.JSON_COMPAT_FLAGS));
    assertEquals(DataFormat.JSON, DataFormat.fromCommonFlag(CodecFlags.JSON_LEGACY_FLAGS));
  }

  @Test
  void decodesBinaryFlags() {
    assertEquals(DataFormat.BINARY, DataFormat.fromCommonFlag(CodecFlags.BINARY_COMMON_FLAGS));
    assertEquals(DataFormat.BINARY, DataFormat.fromCommonFlag(CodecFlags.BINARY_LEGACY_FLAGS));
    assertEquals(DataFormat.BINARY, DataFormat.fromCommonFlag(CodecFlags.BINARY_COMPAT_FLAGS));
  }

  @Test
  void decodesStringFlags() {
    assertEquals(DataFormat.STRING, DataFormat.fromCommonFlag(CodecFlags.STRING_COMMON_FLAGS));
    assertEquals(DataFormat.STRING, DataFormat.fromCommonFlag(CodecFlags.STRING_COMPAT_FLAGS));
  }

  @Test
  void decodesSerializedFlags() {
    assertEquals(DataFormat.OBJECT_SERIALIZATION, DataFormat.fromCommonFlag(CodecFlags.SERIALIZED_LEGACY_FLAGS));
    assertEquals(DataFormat.OBJECT_SERIALIZATION, DataFormat.fromCommonFlag(CodecFlags.SERIALIZED_COMPAT_FLAGS));
  }

}