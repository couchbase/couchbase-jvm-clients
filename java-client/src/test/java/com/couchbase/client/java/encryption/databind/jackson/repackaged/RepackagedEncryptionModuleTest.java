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

package com.couchbase.client.java.encryption.databind.jackson.repackaged;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.encryption.databind.jackson.AbstractEncryptionModuleTest;
import org.junit.jupiter.api.BeforeAll;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RepackagedEncryptionModuleTest extends AbstractEncryptionModuleTest {
  private static final ObjectMapper cryptoMapper = new ObjectMapper();

  @BeforeAll
  static void init() {
    cryptoMapper.registerModule(new RepackagedEncryptionModule(getCryptoManager()));
  }

  @Override
  protected <T extends MaximHolder> void doCheck(Class<T> pojoClass, String json, Consumer<T> pojoValidator) throws Exception {
    T pojo = cryptoMapper.readValue(json, pojoClass);
    pojoValidator.accept(pojo);
    assertEquals(cryptoMapper.readTree(json), cryptoMapper.convertValue(pojo, JsonNode.class));
  }
}
