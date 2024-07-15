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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorCodeAndMessageTest {

  /**
   * This is how endpoints other than "/analytics/service" reported errors
   * before Couchbase Server 7.1.0.
   */
  @Test
  void parsesPlaintextHttpResponseBody() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" 123:Something bad happened!\n".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parsesMalformedPlaintext() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("oops".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Failed to decode error: oops", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  /**
   * This is what a top-level error report looks like in Couchbase Server 7.1.0 and later.
   * NOTE: This is consistent with how errors are reported in the /analytics/service
   * endpoint -- but when we parse that we use a streaming response parser that strips
   * off the "errors" wrapper. This test ensures we can parse errors from the full
   * body of a non-streaming response.
   */
  @Test
  void parsesJsonHttpResponseBody() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("{\"errors\":[{\"code\":123,\"msg\":\"Something bad happened!\"}],\"status\":\"errors\"}".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parseJsonObject() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"code\":123,\"msg\":\"Something bad happened!\"}".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parseJsonObjectWithMissingCode() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"msg\":\"Something bad happened!\"}".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Something bad happened!", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parseJsonObjectWithWeirdProperties() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("{\"foo\":\"bar\"}".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals(mapOf("foo", "bar"), e.context());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parseMalformedJsonObject() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("{".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Failed to decode error: {", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  /**
   * This is how we see errors when reading them from the "/analytics/service" response stream.
   * It's the same format as {@link #parsesJsonHttpResponseBody} but without the "errors" wrapper.
   * (The response stream parser consumes the "errors" wrapper.)
   */
  @Test
  void parseJsonArray() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" [{\"code\":123,\"msg\":\"Something bad happened!\"}]".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parseMalformedJsonArray() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from("[".getBytes(UTF_8)).get(0);
    assertEquals(0, e.code());
    assertEquals("Failed to decode error: [", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parsesJsonWithRetry() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"code\":123,\"msg\":\"Something bad happened!\",\"retry\":true}".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertTrue(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parsesJsonWithRetryFalse() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"code\":123,\"msg\":\"Something bad happened!\",\"retry\":false}".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parsesJsonWithAnalyticsRetriable() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"code\":123,\"msg\":\"Something bad happened!\",\"retriable\":true}".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertTrue(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parsesJsonWithAnalyticsRetriableFalse() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"code\":123,\"msg\":\"Something bad happened!\",\"retriable\":false}".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertFalse(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parseJsonArrayWithRetry() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" [{\"code\":123,\"msg\":\"Something bad happened!\",\"retry\":true}]".getBytes(UTF_8)).get(0);
    assertEquals(123, e.code());
    assertEquals("Something bad happened!", e.message());
    assertTrue(e.retry());
    assertNull(e.reason());
  }

  @Test
  void parsesReason() {
    ErrorCodeAndMessage e = ErrorCodeAndMessage.from(" {\"code\":123,\"msg\":\"Something bad happened!\",\"retry\":false,\"reason\":{\"foo\":\"bar\"}}}".getBytes(UTF_8)).get(0);
    assertEquals("bar", e.reason().get("foo"));
  }

}
