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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the basic functionality of the passthrough serializer.
 */
class PassthroughSerializerTest {

  @Test
  void handlesStringOnEncode() {
    byte[] result = PassthroughSerializer.INSTANCE.serialize("foobar");
    assertEquals("foobar", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void handlesBytesOnEncode() {
    byte[] result = PassthroughSerializer.INSTANCE.serialize("foobar".getBytes(StandardCharsets.UTF_8));
    assertEquals("foobar", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void handlesStringOnDecode() {
    String result = PassthroughSerializer.INSTANCE.deserialize(String.class, "foobar".getBytes(StandardCharsets.UTF_8));
    assertEquals("foobar", result);
  }

  @Test
  void handlesBytesOnDecode() {
    byte[] input = "foobar".getBytes(StandardCharsets.UTF_8);
    byte[] result = PassthroughSerializer.INSTANCE.deserialize(byte[].class, input);
    assertArrayEquals(input, result);
  }

}
