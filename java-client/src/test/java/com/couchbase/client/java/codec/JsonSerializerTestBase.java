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

import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the basic functionality of the default {@link DefaultJsonSerializer}.
 */
abstract class JsonSerializerTestBase {

  protected abstract JsonSerializer serializer();

  @Test
  void encodesJsonObject() {
    JsonObject input = JsonObject.create().put("foo", "bar");
    byte[] output = serializer().serialize(input);
    assertEquals("{\"foo\":\"bar\"}", new String(output, UTF_8));
  }

  @Test
  void decodesJsonObject() {
    byte[] input = "{\"foo\":\"bar\"}".getBytes(UTF_8);
    JsonObject decoded = serializer().deserialize(JsonObject.class, input);
    assertEquals(JsonObject.fromJson(input), decoded);

    JsonObject decodedWithTypeRef = serializer().deserialize(new TypeRef<JsonObject>() {
    }, input);
    assertEquals(decoded, decodedWithTypeRef);
  }

  @Test
  void encodesJsonArray() {
    JsonArray input = JsonArray.from("1", true, 2);
    byte[] output = serializer().serialize(input);
    assertEquals("[\"1\",true,2]", new String(output, UTF_8));
  }

  @Test
  void decodesJsonArray() {
    byte[] input = "[\"1\",true,2]".getBytes(UTF_8);
    JsonArray decoded = serializer().deserialize(JsonArray.class, input);
    assertEquals(JsonArray.fromJson(input), decoded);

    JsonArray decodedWithTypeRef = serializer().deserialize(new TypeRef<JsonArray>() {
    }, input);
    assertEquals(decoded, decodedWithTypeRef);
  }

  @Test
  void encodesMap() {
    Map<String, Object> input = Collections.singletonMap("foo", "bar");
    byte[] output = serializer().serialize(input);
    assertEquals("{\"foo\":\"bar\"}", new String(output, UTF_8));
  }

  @Test
  void decodesMap() {
    Map<String, String> expected = Collections.singletonMap("foo", "bar");
    byte[] input = "{\"foo\":\"bar\"}".getBytes(UTF_8);
    Map decoded = serializer().deserialize(Map.class, input);
    assertEquals(expected, decoded);

    Map<String, String> decodedWithTypeRef = serializer().deserialize(new TypeRef<Map<String, String>>() {
    }, input);
    assertEquals(decoded, decodedWithTypeRef);
  }

  @Test
  void encodesList() {
    List<Object> input = Arrays.asList("1", true, 2);
    byte[] output = serializer().serialize(input);
    assertEquals("[\"1\",true,2]", new String(output, UTF_8));
  }

  @Test
  void decodesList() {
    List<Object> expected = Arrays.asList("1", true, 2);
    byte[] input = "[\"1\",true,2]".getBytes(UTF_8);
    List decoded = serializer().deserialize(List.class, input);
    assertEquals(expected, decoded);

    List<Object> decodedWithTypeRef = serializer().deserialize(new TypeRef<List<Object>>() {
    }, input);
    assertEquals(decoded, decodedWithTypeRef);
  }

  @Test
  void encodesSet() {
    Set<String> input = new LinkedHashSet<>(listOf("foo", "bar"));
    byte[] output = serializer().serialize(input);
    assertEquals("[\"foo\",\"bar\"]", new String(output, UTF_8));
  }

  @Test
  void decodesSet() {
    Set<String> expected = setOf("foo", "bar");
    byte[] input = "[\"foo\",\"bar\"]".getBytes(UTF_8);
    Set decoded = serializer().deserialize(Set.class, input);
    assertEquals(expected, decoded);

    Set<String> decodedWithTypeRef = serializer().deserialize(new TypeRef<Set<String>>() {
    }, input);
    assertEquals(decoded, decodedWithTypeRef);
  }

  @Test
  void handlesBytesOnEncode() {
    byte[] result = serializer().serialize("foobar".getBytes(UTF_8));
    assertEquals("foobar", new String(result, UTF_8));
  }

  @Test
  void handlesBytesOnDecode() {
    byte[] input = "foobar".getBytes(UTF_8);
    byte[] result = serializer().deserialize(byte[].class, input);
    assertArrayEquals(input, result);

    assertThrows(DecodingFailedException.class, () -> {
      serializer().deserialize(new TypeRef<byte[]>() {
      }, input);
    });
  }

}
