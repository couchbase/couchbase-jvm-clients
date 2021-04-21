/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.java.codec;

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonValueSerializerWrapperTest {

  private static class DelegatedException extends RuntimeException {}

  private static final JsonSerializer serializer = new JsonValueSerializerWrapper(
      new JsonSerializer() {
        @Override
        public byte[] serialize(Object input) {
          throw new DelegatedException();
        }

        @Override
        public <T> T deserialize(Class<T> target, byte[] input) {
          throw new DelegatedException();
        }
      }
  );

  @Test
  void canSerializeJsonObject() {
    assertEquals(
        "{\"foo\":\"bar\"}",
        new String(serializer.serialize(JsonObject.create().put("foo", "bar")), UTF_8));
  }

  @Test
  void canSerializeJsonArray() {
    assertEquals(
        "[1]",
        new String(serializer.serialize(JsonArray.create().add(1)), UTF_8));
  }

  @Test
  void canDeserializeJsonObject() {
    assertEquals(
        JsonObject.create().put("foo", "bar"),
        serializer.deserialize(JsonObject.class, "{\"foo\":\"bar\"}".getBytes(UTF_8)));
  }

  @Test
  void canDeserializeJsonArray() {
    assertEquals(
        JsonArray.create().add(1),
        serializer.deserialize(JsonArray.class, "[1]".getBytes(UTF_8)));
  }

  @Test
  void delegatesForOtherTypes() {
    assertThrows(DelegatedException.class, () -> serializer.serialize("foo"));
    assertThrows(DelegatedException.class, () -> serializer.deserialize(String.class, "\"foo\"".getBytes(UTF_8)));
    assertThrows(DecodingFailureException.class, () -> serializer.deserialize(new TypeRef<String>() {
    }, "\"foo\"".getBytes(UTF_8)));
  }
}
