/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.java.kv;

import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the functionality of the {@link MutateInUtil} helper class.
 */
class MutateInUtilTest {

  private static final JsonSerializer JSON_SERIALIZER = DefaultJsonSerializer.create();

  /**
   * This shouldn't really happen at the higher levels, but make sure the case is at least covered.
   */
  @Test
  void convertsEmptyDocList() {
    byte[] result = MutateInUtil.convertDocsToBytes(Collections.emptyList(), JSON_SERIALIZER);
    assertEquals(0, result.length);
  }

  /**
   * This tests a special case optimization that only converts a single doc.
   */
  @Test
  void convertsSingleDocList() {
    JsonObject input = JsonObject.create().put("hello", "world");
    byte[] result = MutateInUtil.convertDocsToBytes(Collections.singletonList(input), JSON_SERIALIZER);
    assertArrayEquals("{\"hello\":\"world\"}".getBytes(StandardCharsets.UTF_8), result);
  }

  /**
   * Tests more than one doc so they are concatenated with a "," per protocol spec.
   */
  @Test
  void convertsMultiDocList() {
    JsonObject i1 = JsonObject.create().put("hello", "world");
    String i2 = "What?";
    boolean i3 = false;
    byte[] result = MutateInUtil.convertDocsToBytes(Arrays.asList(i1, i2, i3), JSON_SERIALIZER);
    assertArrayEquals("{\"hello\":\"world\"},\"What?\",false".getBytes(StandardCharsets.UTF_8), result);
  }

}