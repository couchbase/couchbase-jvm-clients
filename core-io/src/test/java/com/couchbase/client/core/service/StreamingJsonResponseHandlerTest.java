/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamingJsonResponseHandlerTest {

  private static HeadInterceptInputStream body(String s) {
    return new HeadInterceptInputStream(new ByteArrayInputStream(s.getBytes(UTF_8)), StreamingJsonResponseHandler.BODY_HEAD_LIMIT);
  }

  @Test
  void objectsAndArraysLookLikeJson() {
    assertTrue(StreamingJsonResponseHandler.looksLikeJson(body("{\"a\":1}")));
    assertTrue(StreamingJsonResponseHandler.looksLikeJson(body("[1,2")));
    assertTrue(StreamingJsonResponseHandler.looksLikeJson(body(" \t\r\n {garbage")));
  }

  @Test
  void otherBodiesDoNotLookLikeJson() {
    assertFalse(StreamingJsonResponseHandler.looksLikeJson(body("")));
    assertFalse(StreamingJsonResponseHandler.looksLikeJson(body("   \n")));
    assertFalse(StreamingJsonResponseHandler.looksLikeJson(body("<html>{}</html>")));
    assertFalse(StreamingJsonResponseHandler.looksLikeJson(body("Service Unavailable")));
  }

  @Test
  void checksWhatTheParserHasNotReadYet() throws IOException {
    HeadInterceptInputStream body = body("   {\"a\":1}");
    assertEquals(' ', body.read()); // a parser gave up after one byte
    assertTrue(StreamingJsonResponseHandler.looksLikeJson(body));
  }
}
