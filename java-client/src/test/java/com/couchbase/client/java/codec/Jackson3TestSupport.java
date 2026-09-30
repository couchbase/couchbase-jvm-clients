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

package com.couchbase.client.java.codec;

import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Holds the Jackson 3 code for the tests in this package.
 * Jackson 3 needs Java 17, so this code must stay out of the test classes.
 * Otherwise JUnit fails to discover them on Java 8, even when they are disabled.
 */
class Jackson3TestSupport {
  private Jackson3TestSupport() {
  }

  static JsonSerializer serializerWithSharedMapper() {
    return Jackson3JsonSerializer.create(JsonMapper.shared());
  }

  static void assertJsonEquals(String expectedJson, byte[] actualJson) {
    assertEquals(JsonMapper.shared().readTree(expectedJson), JsonMapper.shared().readTree(actualJson));
  }
}
