/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.java.encryption.databind.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EncryptionModuleTest extends AbstractEncryptionModuleTest {
  private static final ObjectMapper cryptoMapper = new ObjectMapper();

  @BeforeAll
  static void init() {
    cryptoMapper.registerModule(new EncryptionModule(getCryptoManager()));
  }

  @Override
  protected <T extends MaximHolder> void doCheck(Class<T> pojoClass, String inputJson, String expectedOutputJson, Consumer<T> pojoValidator) throws Exception {
    T pojo = cryptoMapper.readValue(inputJson, pojoClass);
    pojoValidator.accept(pojo);
    assertEquals(cryptoMapper.readTree(expectedOutputJson), cryptoMapper.convertValue(pojo, JsonNode.class));
  }
}
