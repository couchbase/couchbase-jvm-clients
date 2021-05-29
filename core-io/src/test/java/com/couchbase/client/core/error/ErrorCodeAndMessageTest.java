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

package com.couchbase.client.core.error;

import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorCodeAndMessageTest {
  @Test
  void parsesPlaintext() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" 123:Something bad happened!\n".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
  }

  @Test
  void parsesMalformedPlaintext() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("oops".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Failed to decode error: oops", e.message());
  }

  @Test
  void parseJsonObject() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"code\":123,\"msg\":\"Something bad happened!\"}".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
  }

  @Test
  void parseJsonObjectWithMissingCode() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"msg\":\"Something bad happened!\"}".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Something bad happened!", e.message());
  }

  @Test
  void parseJsonObjectWithWeirdProperties() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("{\"foo\":\"bar\"}".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals(mapOf("foo", "bar"), e.context());
  }

  @Test
  void parseMalformedJsonObject() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("{".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Failed to decode error: {", e.message());
  }

  @Test
  void parseJsonArray() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" [{\"code\":123,\"msg\":\"Something bad happened!\"}]".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
  }

  @Test
  void parseMalformedJsonArray() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("[".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Failed to decode error: [", e.message());
  }
}
