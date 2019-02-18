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

package com.couchbase.client.java.codec;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.EncodedDocument;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the functionality of the {@link DefaultEncoder}.
 *
 * @since 3.0.0
 */
class DefaultEncoderTest {

  private static final Encoder ENCODER = DefaultEncoder.INSTANCE;

  @Test
  void encodesJsonObject() {
    EncodedDocument result = ENCODER.encode(JsonObject.empty());
    assertEquals(Encoder.JSON_FLAGS, result.flags());
    assertEquals("{}", new String(result.content(), CharsetUtil.UTF_8));
  }

  @Test
  void encodesJsonArray() {
    EncodedDocument result = ENCODER.encode(JsonArray.empty());
    assertEquals(Encoder.JSON_FLAGS, result.flags());
    assertEquals("[]", new String(result.content(), CharsetUtil.UTF_8));
  }

  @Test
  void encodesJavaMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("foo", "bar");
    EncodedDocument result = ENCODER.encode(map);
    assertEquals(Encoder.JSON_FLAGS, result.flags());
    assertEquals("{\"foo\":\"bar\"}", new String(result.content(), CharsetUtil.UTF_8));
  }

  @Test
  void encodesJavaList() {
    List<Object> list = new ArrayList<>();
    list.add("hello");
    list.add(true);
    EncodedDocument result = ENCODER.encode(list);
    assertEquals(Encoder.JSON_FLAGS, result.flags());
    assertEquals("[\"hello\",true]", new String(result.content(), CharsetUtil.UTF_8));
  }

  @Test
  void encodesEncodedJson() {
    String encoded = "{\"foo\":\"bar\"}";
    EncodedDocument result = ENCODER.encode(EncodedJsonContent.wrap(encoded));
    assertEquals(Encoder.JSON_FLAGS, result.flags());
    assertEquals("{\"foo\":\"bar\"}", new String(result.content(), CharsetUtil.UTF_8));
  }

}