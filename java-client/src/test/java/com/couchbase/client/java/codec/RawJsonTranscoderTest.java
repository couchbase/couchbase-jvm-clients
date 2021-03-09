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

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.kv.CodecFlags;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the functionality of the {@link RawJsonTranscoder}.
 */
class RawJsonTranscoderTest {

  private static final Transcoder RAW_JSON_TRANSCODER = RawJsonTranscoder.INSTANCE;

  @Test
  void encodesJsonPassthroughString() {
    String input = "{\"hello\": true}";
    Transcoder.EncodedValue output = RAW_JSON_TRANSCODER.encode(input);
    assertArrayEquals(input.getBytes(StandardCharsets.UTF_8), output.encoded());
  }

  @Test
  void encodesJsonPassthroughByteArray() {
    byte[] input = "{\"hello\": true}".getBytes(StandardCharsets.UTF_8);
    Transcoder.EncodedValue output = RAW_JSON_TRANSCODER.encode(input);
    assertArrayEquals(input, output.encoded());
  }

  @Test
  void decodesJsonPassthroughString() {
    String input = "{\"hello\": true}";
    String output = RAW_JSON_TRANSCODER.decode(
      String.class,
      input.getBytes(StandardCharsets.UTF_8),
      CodecFlags.JSON_COMPAT_FLAGS
    );
    assertEquals(input, output);
  }

  @Test
  void rejectsDecodingAsObject() {
    assertThrows(DecodingFailureException.class, () ->
        RAW_JSON_TRANSCODER.decode(
            Object.class,
            "{\"hello\": true}".getBytes(StandardCharsets.UTF_8),
            CodecFlags.JSON_COMPAT_FLAGS));
  }

  @Test
  void decodesJsonPassthroughByteArray() {
    byte[] input = "{\"hello\": true}".getBytes(StandardCharsets.UTF_8);
    byte[] output = RAW_JSON_TRANSCODER.decode(
      byte[].class,
      input,
      CodecFlags.JSON_COMPAT_FLAGS
    );
    assertArrayEquals(input, output);
  }

}
