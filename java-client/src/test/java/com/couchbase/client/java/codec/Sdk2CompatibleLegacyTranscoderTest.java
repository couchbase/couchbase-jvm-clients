/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class Sdk2CompatibleLegacyTranscoderTest {

  private Sdk2CompatibleLegacyTranscoder converter;

  @BeforeEach
  void setup() {
    converter = Sdk2CompatibleLegacyTranscoder.INSTANCE;
  }

  @Test
  void shouldEncodeJsonString() {
    Transcoder.EncodedValue encoded = converter.encode("{\"test:\":true}");

    assertEquals("{\"test:\":true}", new String(encoded.encoded(), StandardCharsets.UTF_8));
    assertEquals(0, (long) encoded.flags());
  }


  @Test
  void shouldNotCompressLongJsonString() {
    String input = loadFileIntoString("large.json");
    Transcoder.EncodedValue encoded =  converter.encode(input);

    assertEquals(input, new String(encoded.encoded(), StandardCharsets.UTF_8));
    assertEquals(0, (long) encoded.flags());
  }

  @Test
  void shouldCompressLongNonJsonString() {
    String input = loadFileIntoString("large_nonjson.txt");
    Transcoder.EncodedValue encoded =  converter.encode(input);

    assertNotEquals(input, new String(encoded.encoded(), StandardCharsets.UTF_8));
    assertEquals(Sdk2CompatibleLegacyTranscoder.COMPRESSED, (long) encoded.flags());
  }

  private static String loadFileIntoString(final String path) {
    InputStream stream = Sdk2CompatibleLegacyTranscoderTest.class.getResourceAsStream(path);
    java.util.Scanner s = new java.util.Scanner(stream).useDelimiter("\\A");
    String input = s.next();
    s.close();
    return input;
  }

}