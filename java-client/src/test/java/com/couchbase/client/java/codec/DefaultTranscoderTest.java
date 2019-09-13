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

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the functionality of the {@link DefaultTranscoder}.
 */
class DefaultTranscoderTest {

  private static final Transcoder TRANSCODER = DefaultTranscoder.create();

  @Test
  void encodesJsonObject() {
    JsonObject input = JsonObject.create().put("foo", "bar");
    byte[] output = TRANSCODER.encode(input, DataFormat.JSON);
    assertEquals("{\"foo\":\"bar\"}", new String(output, StandardCharsets.UTF_8));
  }

  @Test
  void decodesJsonObject() {
    byte[] input = "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8);
    JsonObject decoded = TRANSCODER.decode(JsonObject.class, input, DataFormat.JSON);
    assertEquals(JsonObject.fromJson(input), decoded);
  }

  @Test
  void encodesJsonArray() {
    JsonArray input = JsonArray.from("1", true, 2);
    byte[] output = TRANSCODER.encode(input, DataFormat.JSON);
    assertEquals("[\"1\",true,2]", new String(output, StandardCharsets.UTF_8));
  }

  @Test
  void decodesJsonArray() {
    byte[] input = "[\"1\",true,2]".getBytes(StandardCharsets.UTF_8);
    JsonArray decoded = TRANSCODER.decode(JsonArray.class, input, DataFormat.JSON);
    assertEquals(JsonArray.fromJson(input), decoded);
  }

  @Test
  void encodesMap() {
    Map<String, Object> input = Collections.singletonMap("foo", "bar");
    byte[] output = TRANSCODER.encode(input, DataFormat.JSON);
    assertEquals("{\"foo\":\"bar\"}", new String(output, StandardCharsets.UTF_8));
  }

  @Test
  void decodesMap() {
    Map<String, Object> expected = Collections.singletonMap("foo", "bar");
    byte[] input = "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8);
    Map decoded = TRANSCODER.decode(Map.class, input, DataFormat.JSON);
    assertEquals(expected, decoded);
  }

  @Test
  void encodesList() {
    List<Object> input = Arrays.asList("1", true, 2);
    byte[] output = TRANSCODER.encode(input, DataFormat.JSON);
    assertEquals("[\"1\",true,2]", new String(output, StandardCharsets.UTF_8));
  }

  @Test
  void decodesList() {
    List<Object> expected = Arrays.asList("1", true, 2);
    byte[] input = "[\"1\",true,2]".getBytes(StandardCharsets.UTF_8);
    List decoded = TRANSCODER.decode(List.class, input, DataFormat.JSON);
    assertEquals(expected, decoded);
  }

  @Test
  void encodesSet() {
    Set<Object> input = new LinkedHashSet<>();
    input.add("foo");
    input.add("bar");
    byte[] output = TRANSCODER.encode(input, DataFormat.JSON);
    assertEquals("[\"foo\",\"bar\"]", new String(output, StandardCharsets.UTF_8));
  }

  @Test
  void decodesSet() {
    Set<Object> expected = new LinkedHashSet<>();
    expected.add("foo");
    expected.add("bar");
    byte[] input = "[\"foo\",\"bar\"]".getBytes(StandardCharsets.UTF_8);
    Set decoded = TRANSCODER.decode(Set.class, input, DataFormat.JSON);
    assertEquals(expected, decoded);
  }

  @Test
  void encodesJsonPassthroughString() {
    String input = "{\"hello\": true}";
    byte[] output = TRANSCODER.encode(input, DataFormat.ENCODED_JSON);
    assertArrayEquals(input.getBytes(StandardCharsets.UTF_8), output);
  }

  @Test
  void encodesJsonPassthroughByteArray() {
    byte[] input = "{\"hello\": true}".getBytes(StandardCharsets.UTF_8);
    byte[] output = TRANSCODER.encode(input, DataFormat.ENCODED_JSON);
    assertArrayEquals(input, output);
  }

  @Test
  void decodesJsonPassthroughString() {
    String input = "{\"hello\": true}";
    String output = TRANSCODER.decode(
      String.class,
      input.getBytes(StandardCharsets.UTF_8),
      DataFormat.ENCODED_JSON
    );
    assertEquals(input, output);
  }

  @Test
  void decodesJsonPassthroughByteArray() {
    byte[] input = "{\"hello\": true}".getBytes(StandardCharsets.UTF_8);
    byte[] output = TRANSCODER.decode(
      byte[].class,
      input,
      DataFormat.ENCODED_JSON
    );
    assertArrayEquals(input, output);
  }

}