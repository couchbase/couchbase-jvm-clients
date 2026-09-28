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

import com.couchbase.client.java.encryption.databind.jackson.AbstractEncryptionModuleTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisabledForJreRange(
  min = JRE.JAVA_8,
  max = JRE.JAVA_16,
  disabledReason = "Jackson 3 requires Java 17 or later."
)
public class Jackson3EncryptionModuleTest extends AbstractEncryptionModuleTest {
  private static JsonMapper cryptoMapper;

  @BeforeAll
  static void init() {
    // The shared tests expect these Jackson 2 defaults. Jackson 3 disables them by default.
    cryptoMapper = JsonMapper.builder()
      .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
      .addModule(new EncryptionModule(getCryptoManager()))
      .build();
  }

  @Override
  protected <T extends MaximHolder> void doCheck(Class<T> pojoClass, String inputJson, String expectedOutputJson, Consumer<T> pojoValidator) throws Exception {
    T pojo = cryptoMapper.readValue(inputJson, pojoClass);
    pojoValidator.accept(pojo);
    assertEquals(cryptoMapper.readTree(expectedOutputJson), cryptoMapper.convertValue(pojo, JsonNode.class));
  }
}
