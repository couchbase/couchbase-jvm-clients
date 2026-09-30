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

package com.couchbase.client.java.encryption.databind.jackson3;

import com.couchbase.client.core.encryption.CryptoManager;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Holds the Jackson 3 code for the tests in this package.
 * Jackson 3 needs Java 17, so this code must stay out of the test classes.
 * Otherwise JUnit fails to discover them on Java 8, even when they are disabled.
 */
class Jackson3TestSupport {
  private final JsonMapper cryptoMapper;

  Jackson3TestSupport(CryptoManager cryptoManager) {
    // The shared tests expect these Jackson 2 defaults. Jackson 3 disables them by default.
    this.cryptoMapper = JsonMapper.builder()
      .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
      .addModule(new EncryptionModule(cryptoManager))
      .build();
  }

  <T> T readValue(String json, Class<T> type) {
    return cryptoMapper.readValue(json, type);
  }

  void assertJsonEquals(String expectedJson, Object actual) {
    assertEquals(cryptoMapper.readTree(expectedJson), cryptoMapper.convertValue(actual, JsonNode.class));
  }
}
