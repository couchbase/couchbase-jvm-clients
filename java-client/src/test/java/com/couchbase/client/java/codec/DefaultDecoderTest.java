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

import com.couchbase.client.core.error.DecodingFailedException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the functionality of the {@link DefaultDecoder}.
 *
 * @since 3.0.0
 */
class DefaultDecoderTest {

  private static final Decoder DECODER = DefaultDecoder.INSTANCE;

  @Test
  void decodesJsonObject() {
    EncodedDocument encoded = EncodedDocument.of(Encoder.JSON_FLAGS, "{}".getBytes(CharsetUtil.UTF_8));
    JsonObject result = (JsonObject) DECODER.decode(JsonObject.class, encoded);
    assertEquals(JsonObject.empty(), result);
  }

  @Test
  void decodesJsonArray() {
    EncodedDocument encoded = EncodedDocument.of(Encoder.JSON_FLAGS, "[]".getBytes(CharsetUtil.UTF_8));
    JsonArray result = (JsonArray) DECODER.decode(JsonArray.class, encoded);
    assertEquals(JsonArray.empty(), result);
  }

  @Test
  void decodesJavaMap() {
    EncodedDocument encoded = EncodedDocument.of(Encoder.JSON_FLAGS, "{}".getBytes(CharsetUtil.UTF_8));
    Map<String, Object> result = (Map<String, Object>) DECODER.decode(Map.class, encoded);
    assertEquals(new HashMap<>(), result);
  }

  @Test
  void decodesJavaList() {
    EncodedDocument encoded = EncodedDocument.of(Encoder.JSON_FLAGS, "[]".getBytes(CharsetUtil.UTF_8));
    List<Object> result = (List<Object>) DECODER.decode(List.class, encoded);
    assertEquals(new ArrayList<>(), result);
  }

  @Test
  void throwsOnInvalidCombination() {
    EncodedDocument encoded = EncodedDocument.of(Encoder.JSON_FLAGS, "{}".getBytes(CharsetUtil.UTF_8));
    assertThrows(DecodingFailedException.class, () -> DECODER.decode(JsonArray.class, encoded));
  }

  @Test
  void decodesEncodedJson() {
    EncodedDocument encoded = EncodedDocument.of(Encoder.JSON_FLAGS, "{\"foo\": true}".getBytes(CharsetUtil.UTF_8));
    EncodedJsonContent result = (EncodedJsonContent) DECODER.decode(EncodedJsonContent.class, encoded);
    assertEquals("{\"foo\": true}", new String(result.encoded(), CharsetUtil.UTF_8));
  }

}